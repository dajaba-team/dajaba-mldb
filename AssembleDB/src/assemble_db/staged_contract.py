"""고정된 정형화 입력의 재사용 계약과 단계 진행 기록을 제공합니다."""

import hashlib
import json
import logging
import os
import sys
import threading
import time
import tomllib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from dbcheck.distribution import sha256

from .config import Config
from .exporting import decimal_wire_hash, load_payload, logical_hash

LOG = logging.getLogger("assemble_db.staged")
STAGED_VERSION = "canonical-text-then-ml-parquet-v1"
SOURCE_IDS = frozenset({"ibm_hi_small", "ibm_li_small", "saml_d"})


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def require(condition, message: str) -> None:
    if not condition:
        raise ValueError(message)


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + "." + uuid4().hex + ".partial")
    data = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")
    with partial.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(4):
        try:
            partial.replace(path)
            return
        except PermissionError as error:
            if os.name != "nt" or getattr(error, "winerror", None) != 5 or attempt == 3:
                raise
            delay = (0.05, 0.1, 0.2)[attempt]
            LOG.warning("단계 기록 교체 재시도 %s/3: %s", attempt + 1, path.name)
            time.sleep(delay)
    raise AssertionError("Unreachable atomic publication state")


def read_metadata(path: Path, limit: int = 2 * 1024**2) -> dict:
    require(path.stat().st_size <= limit, "Metadata exceeds its explicit limit")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, "Metadata grew beyond its explicit limit")
    value = json.loads(data.decode("utf-8-sig"))
    require(type(value) is dict, "Metadata must be an object")
    return value


def child(folder: Path, relative: str) -> Path:
    path = (folder / relative).resolve()
    require(path.parent == folder.resolve(), "Dataset data file escapes its folder")
    return path


def same_toml(left, right) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return set(left) == set(right) and all(same_toml(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_toml(a, b) for a, b in zip(left, right))
    return left == right


def archived_config(
    config: Config, snapshot: Path | None, root: Path, *, commit: bool = True
) -> tuple[Config, dict]:
    require(
        sha256(config.check.path) == config.check.config_sha256,
        "Loaded DBcheck configuration changed before archival binding",
    )
    if snapshot is None:
        return config, {
            "mode": "CURRENT_CONFIG",
            "path": str(config.check.path),
            "sha256": config.check.config_sha256,
        }
    snapshot = snapshot.resolve()
    actual = snapshot.read_bytes()
    current = config.check.path.read_bytes()
    require(
        hashlib.sha256(current).hexdigest() == config.check.config_sha256,
        "Loaded DBcheck configuration changed before archival binding",
    )
    require(
        same_toml(
            tomllib.loads(actual.decode("utf-8-sig")), tomllib.loads(current.decode("utf-8-sig"))
        ),
        "Archived DBcheck configuration differs semantically from current settings",
    )
    digest = hashlib.sha256(actual).hexdigest()
    target = root / "provenance" / "validated_dbcheck_snapshot.toml"
    if commit:
        target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        require(target.read_bytes() == actual, "Existing archived snapshot copy changed")
    elif commit:
        with target.open("xb") as stream:
            stream.write(actual)
    if commit:
        require(sha256(target) == digest, "Archived snapshot copy differs")
    # 상대 경로는 현재 설정의 원래 위치에서 이미 해석됐습니다. 실제 과거 바이트만 복원합니다.
    restored = replace(config.check, path=target, config_sha256=digest)
    require(
        snapshot.read_bytes() == actual and config.check.path.read_bytes() == current,
        "Configuration changed while restoring archived bytes",
    )
    proof = {
        "mode": "GENUINE_ARCHIVED_BYTES_WITH_ORIGINAL_PATH_ANCHORS",
        "original_anchor": str(config.check.path),
        "current_sha256": hashlib.sha256(current).hexdigest(),
        "snapshot_origin": str(snapshot),
        "snapshot_copy": str(target),
        "snapshot_sha256": digest,
        "semantic_and_type_equality_checked": True,
    }
    if commit:
        atomic_json(root / "provenance" / "configuration_restoration.json", proof)
    return replace(config, check=restored), proof


class RunLock:
    def __init__(self, root: Path):
        self.path = root / ".staged.lock"
        self.stream = None

    def __enter__(self):
        self.stream = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.stream.close()
            self.stream = None
            raise ValueError("Another staged invocation owns this run") from error
        return self

    def __exit__(self, exc_type, exc, traceback):
        assert self.stream is not None
        try:
            self.stream.seek(0)
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
        finally:
            self.stream.close()
        return False


class Progress:
    def __init__(self, folder: Path, *, interval: float = 300):
        require(interval > 0, "Heartbeat interval must be positive")
        self.folder = folder
        self.interval = interval
        self.started = time.monotonic()
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.failure: Exception | None = None
        self.state: dict = {
            "status": "RUNNING",
            "stage": "starting",
            "processed": 0,
            "total": None,
            "unit": None,
            "last_work_at": utc(),
        }
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)

    def _record(self, heartbeat: bool) -> None:
        value = dict(
            self.state,
            observed_at=utc(),
            heartbeat_only=heartbeat,
            elapsed_seconds=round(time.monotonic() - self.started, 3),
            eta_seconds=None,
        )
        atomic_json(self.folder / "progress.json", value)
        with (self.folder / "progress.log").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
        LOG.info("단계 진행 %s", json.dumps(value, ensure_ascii=False, separators=(",", ":")))

    def emit(
        self,
        stage: str,
        processed: int = 0,
        total: int | None = None,
        unit: str | None = None,
        **details,
    ) -> None:
        with self.lock:
            if self.failure is not None:
                raise RuntimeError("Heartbeat publication failed") from self.failure
            require(type(processed) is int and processed >= 0, "Invalid progress counter")
            require(
                total is None or type(total) is int and total >= processed,
                "Invalid progress denominator",
            )
            self.state.update(
                stage=stage,
                processed=processed,
                total=total,
                unit=unit,
                last_work_at=utc(),
                **details,
            )
            self._record(False)

    def _heartbeat(self) -> None:
        while not self.stop.wait(self.interval):
            try:
                with self.lock:
                    self._record(True)
            except Exception as error:
                self.failure = error
                LOG.exception("Heartbeat publication failed")
                return

    def __enter__(self):
        self.emit("starting")
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop.set()
        self.thread.join(timeout=5)
        with self.lock:
            failed_heartbeat = exc_type is None and self.failure is not None
            self.state["status"] = (
                "FAILED"
                if failed_heartbeat
                else "BUILD_COMPLETED"
                if exc_type is None
                else "CANCELLED"
                if issubclass(exc_type, KeyboardInterrupt)
                else "FAILED"
            )
            self.state["error_type"] = (
                "HeartbeatPublicationError"
                if failed_heartbeat
                else None
                if exc_type is None
                else exc_type.__name__
            )
            try:
                self._record(False)
            except Exception:
                if exc_type is None and not failed_heartbeat:
                    raise
                LOG.exception("Final progress record failed; preserving original exception")
            if failed_heartbeat:
                raise RuntimeError("Heartbeat publication failed") from self.failure
        return False


@dataclass
class TextInput:
    folder: Path
    schema: pa.Schema
    manifest: dict
    format: str
    manifest_sha256: str
    schema_sha256: str

    def path(self, index: int) -> Path:
        matches = [
            f
            for f in self.manifest["parts"][index]["files"]
            if Path(f["path"]).suffix == "." + self.format
        ]
        require(len(matches) == 1, "Canonical text format is not unique")
        return child(self.folder, matches[0]["path"])

    def read(self, index: int) -> pa.Table:
        part = self.manifest["parts"][index]
        item = next(f for f in part["files"] if Path(f["path"]).suffix == "." + self.format)
        path = self.path(index)
        limit = 512 * 1024**2
        require(
            type(item["bytes"]) is int and 0 <= item["bytes"] <= limit,
            "Canonical input byte declaration exceeds its explicit limit",
        )
        with path.open("rb") as stream:
            data = stream.read(item["bytes"] + 1)
        require(
            len(data) == item["bytes"] and hashlib.sha256(data).hexdigest() == item["sha256"],
            "Canonical input bytes changed before bounded decoding",
        )
        fields = self.manifest.get("lexical_decimal_fields", [])
        table, payload = load_payload(path, self.schema, data=data, include_wire=bool(fields))
        if fields:
            require(
                payload is not None
                and decimal_wire_hash(payload, fields) == part.get("lexical_decimal_sha256"),
                "Canonical original decimal spelling differs",
            )
        require(
            len(table) == part["rows"] and logical_hash(table) == part["logical_sha256"],
            "Canonical text typed logical values differ",
        )
        require(sha256(path) == item["sha256"], "Canonical input changed during derivation")
        return table

    def verify_metadata(self) -> None:
        require(
            sha256(self.folder / "manifest.json") == self.manifest_sha256
            and sha256(self.folder / "schema.arrow") == self.schema_sha256,
            "Canonical input manifest/schema changed",
        )

    def reference(self) -> dict:
        return {
            "folder": str(self.folder),
            "format": self.format,
            "manifest_sha256": self.manifest_sha256,
            "schema_sha256": self.schema_sha256,
            "formats": self.manifest.get("formats", ["csv", "json", "parquet"]),
            "rows": self.manifest["rows"],
            "parts": len(self.manifest["parts"]),
        }


def capture_text(
    folder: Path, schema: pa.Schema, expected: dict, fmt: str, progress: Progress, stage: str
) -> TextInput:
    require(fmt in {"csv", "json"}, "Canonical format must be CSV or JSON")
    folder = folder.resolve()
    manifest_path = folder / "manifest.json"
    manifest_digest = sha256(manifest_path)
    manifest = read_metadata(manifest_path)
    require(sha256(manifest_path) == manifest_digest, "Canonical manifest changed during capture")
    require(manifest.get("status") == "VERIFIED", "Canonical source is incomplete")
    for key, value in expected.items():
        require(
            type(manifest.get(key)) is type(value) and manifest[key] == value,
            "Canonical source binding mismatch: " + key,
        )
    schema_path = folder / "schema.arrow"
    require(schema_path.stat().st_size <= 262144, "Canonical schema exceeds limit")
    schema_bytes = schema_path.read_bytes()
    require(len(schema_bytes) <= 262144, "Canonical schema grew beyond its limit")
    schema_digest = hashlib.sha256(schema_bytes).hexdigest()
    observed = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
    require(
        observed.equals(schema), "Canonical source schema differs from current semantic contract"
    )
    require(
        manifest.get("schema")
        == [dict(name=f.name, type=str(f.type), nullable=f.nullable) for f in schema],
        "Canonical schema declaration differs",
    )
    decimal_fields = [f.name for f in schema if pa.types.is_decimal(f.type)]
    if fmt == "json" and decimal_fields:
        require(
            manifest.get("lexical_decimal_fields") == decimal_fields,
            "Canonical JSON lacks an original-decimal spelling contract",
        )

    require(0 < len(manifest["parts"]) <= 100000, "Canonical part inventory limit")
    require(
        sum(p["rows"] for p in manifest["parts"]) == manifest["rows"],
        "Canonical row reconciliation failed",
    )
    result = TextInput(folder, schema, manifest, fmt, manifest_digest, schema_digest)
    for index, part in enumerate(manifest["parts"]):
        require(
            type(part["rows"]) is int and 0 <= part["rows"] <= 100000,
            "Canonical part rows exceed bounded decode",
        )
        matches = [f for f in part["files"] if Path(f["path"]).suffix == "." + fmt]
        require(len(matches) == 1, "Canonical part lacks the selected text format")
        item = matches[0]
        path = result.path(index)
        require(
            path.stat().st_size == item["bytes"] <= 512 * 1024**2,
            "Canonical part byte budget differs",
        )
        require(
            item["rows"] == part["rows"] and sha256(path) == item["sha256"],
            "Canonical input differs from the prior completed fidelity proof",
        )
        progress.emit(stage, index + 1, len(manifest["parts"]), "files")
    result.verify_metadata()
    return result
