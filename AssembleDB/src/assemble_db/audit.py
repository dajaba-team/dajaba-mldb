"""원천과 산출물을 순서대로 읽기 전용 대조합니다. 보정·반올림·DB 접근은 하지 않습니다."""

import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import NotRequired, TypedDict

import pyarrow as pa
import pyarrow.parquet as pq
from dbcheck.distribution import IBM, SAML, sha256

from .config import VERSION
from .contracts import bind, read_json
from .exporting import load_payload, primary_part_path, verify_dataset
from .file_commit import replace_closed_json
from .normalize import (
    ACCOUNT_HEADER,
    ACCOUNT_SCHEMA,
    SERVICE_MAPPING_SCHEMA,
    label_schema,
    service_schema,
    transaction_schema,
)
from .service_contract import INT64_MAX, fits_service_numeric

AUDIT_VERSION = "raw-assembly-audit-v1"
BATCH_ROWS = 256
MAX_PART_ROWS = 100000
MAX_FILE_BYTES = 256 * 1024 * 1024
LOG = logging.getLogger(__name__)


AuditScalar = str | int | bool | datetime | Decimal | None


class NumericAudit(TypedDict):
    """검사 중 집계와 완료 후에만 붙는 판정을 구분합니다."""

    checked_values: int
    incompatible_values: int
    status: NotRequired[str]


class SourceAuditState(TypedDict):
    """중단 시점까지 실제로 기록한 항목만 보고서에 남깁니다."""

    stage: str
    fidelity: str
    verified_rows: int
    input_fingerprints: NotRequired[dict[str, str]]
    dataset: NotRequired[str]
    service_version: NotRequired[str | None]
    service_v4: NotRequired[str]
    numeric_20_6: NotRequired[NumericAudit]
    account_values: NotRequired[str]
    artifact_binding: NotRequired[dict]
    direct_copy_ready: NotRequired[bool]


class AuditReport(TypedDict):
    """소스별 상태와 전체 종료 정보를 가진 감사 보고서입니다."""

    audit_version: str
    status: str
    assembly_run: str
    started_at: str
    sources: dict[str, SourceAuditState]
    resume_supported: bool
    report_is_not_checkpoint: bool
    audit_code_sha256: str
    assembly_run_sha256: str
    config_sha256: str
    validation_run_sha256: str
    finished_at: NotRequired[str]
    error_type: NotRequired[str]
    error: NotRequired[str]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def inside(root, relative):
    relative = Path(relative)
    require(
        not relative.is_absolute() and ".." not in relative.parts,
        "Expected relative artifact path without parent traversal",
    )
    path = (root / relative).resolve()
    require(
        path != root.resolve() and path.is_relative_to(root.resolve()),
        f"Path escapes artifact root: {relative}",
    )
    return path


def child(root, name):
    """입출력 전에 산출물의 직계 하위 경로와 심볼릭 링크의 대상까지 검증합니다."""
    relative = Path(name)
    require(
        not relative.is_absolute()
        and len(relative.parts) == 1
        and relative.name not in {"", ".", ".."},
        "Expected direct child path",
    )
    path = inside(root, name)
    require(path.parent == root.resolve(), "Expected direct child path")
    return path


def integer(value, lower, upper, name):
    require(type(value) is int and lower <= value <= upper, f"Invalid int64 {name}")


def amount(raw, precision, scale) -> Decimal:
    # normalize()/exact_amount() 및 Arrow 형 변환과 의도적으로 독립된 구현입니다.
    require(
        bool(re.fullmatch(r"[+]?[0-9]+(?:\.[0-9]+)?", raw)),
        "Original amount is not unsigned plain decimal",
    )
    whole, _, fraction = raw.lstrip("+").partition(".")
    require(
        len(whole.lstrip("0")) <= precision - scale and len(fraction) <= scale,
        "Original amount exceeds source precision/scale",
    )
    value = Decimal(raw)
    require(value.is_finite(), "Non-finite original amount")
    return value


def bank(raw):
    require(bool(re.fullmatch(r"[0-9]+", raw)), "Non-numeric original bank")
    return raw.lstrip("0") or "0"


def unit(source_id: str, raw: str, aliases: dict[str, str]) -> tuple[str | None, str]:
    require(raw != "", "Empty currency")
    code = aliases.get(raw)
    return code, code if code else json.dumps([source_id, raw], separators=(",", ":"))


def expected(
    source, fingerprint, ordinal, raw, aliases
) -> tuple[dict[str, AuditScalar], dict[str, str | int]]:
    """운영 정규화 함수를 호출하지 않고 독립적으로 CSV 필드를 매핑합니다."""
    integer(ordinal, 0, INT64_MAX - 1, "ordinal")
    ibm = source.kind == "ibm"
    require(len(raw) == len(IBM if ibm else SAML), "CSV column count mismatch")
    text = raw[0] if ibm else raw[1] + " " + raw[0]
    fmt = "%Y/%m/%d %H:%M" if ibm else "%Y-%m-%d %H:%M:%S"
    ts = datetime.strptime(text, fmt)
    require(ts.strftime(fmt) == text, "Timestamp round trip changed original")
    release = f"{source.id}:{fingerprint}"
    uid = f"{release}:{ordinal}"
    sender, receiver = (raw[2], raw[4]) if ibm else (raw[2], raw[3])
    left = [release, bank(raw[1]), sender] if ibm else [release, sender]
    right = [release, bank(raw[3]), receiver] if ibm else [release, receiver]

    def key(parts):
        return json.dumps(parts, separators=(",", ":"))

    tx: dict[str, AuditScalar] = dict(
        transaction_uid=uid,
        source_row_id=ordinal,
        ts=ts,
        from_account_key=key(left),
        to_account_key=key(right),
        from_account_raw=sender,
        to_account_raw=receiver,
        payment_format=raw[9],
        is_self_transfer=left == right,
    )
    require(raw[10] in {"0", "1"}, "Original label must be 0 or 1")
    labels: dict[str, str | int] = dict(
        transaction_uid=uid,
        source_row_id=ordinal,
        source_positive=int(raw[10]),
        source_label_raw=raw[10],
    )
    if ibm:
        tx.update(from_bank_raw=raw[1], to_bank_raw=raw[3])
        for side, money_i, currency_i in [("paid", 7, 8), ("received", 5, 6)]:
            iso, bucket = unit(source.id, raw[currency_i], aliases)
            tx.update(
                {
                    f"amount_{side}": amount(
                        raw[money_i], source.amount_precision, source.amount_scale
                    ),
                    f"amount_{side}_raw": raw[money_i],
                    f"currency_{side}_raw": raw[currency_i],
                    f"currency_{side}_iso": iso,
                    f"currency_{side}_bucket": bucket,
                }
            )
    else:
        paid = unit(source.id, raw[5], aliases)
        received = unit(source.id, raw[6], aliases)
        allowed = paid[1] == received[1]
        tx.update(
            amount=amount(raw[4], source.amount_precision, source.amount_scale),
            amount_raw=raw[4],
            currency_paid_raw=raw[5],
            currency_received_raw=raw[6],
            amount_currency_bucket=paid[1] if allowed else None,
            amount_currency_iso=paid[0] if allowed else None,
            money_operations_allowed=allowed,
            sender_bank_location=raw[7],
            receiver_bank_location=raw[8],
        )
        labels["source_typology"] = raw[11]
    return tx, labels


def expected_service(tx, fingerprint, version):
    require(
        version in {"dajaba-service-candidate-v3", "dajaba-service-candidate-v4"},
        "Unsupported service contract",
    )
    v4 = version.endswith("-v4")

    def account(side):
        raw_bank = tx[f"{side}_bank_raw"]
        return f"{bank(raw_bank) if v4 else raw_bank}_{tx[f'{side}_account_raw']}"

    ordinal = tx["source_row_id"]
    row = dict(
        candidate_transaction_id=ordinal + 1,
        source_file_hash=fingerprint,
        source_row_id=ordinal,
        ts=tx["ts"],
        from_account_key=account("from"),
        to_account_key=account("to"),
        amount_paid=tx["amount_paid"],
        currency_paid=tx["currency_paid_raw"],
        amount_received=tx["amount_received"],
        currency_received=tx["currency_received_raw"],
        payment_format=tx["payment_format"],
        is_self_transfer=tx["is_self_transfer"],
    )
    mapping = dict(
        candidate_transaction_id=ordinal + 1,
        source_file_hash=fingerprint,
        transaction_uid=tx["transaction_uid"],
        source_row_id=ordinal,
        role="TARGET",
    )
    if v4:
        row.update(from_bank_id_raw=tx["from_bank_raw"], to_bank_id_raw=tx["to_bank_raw"])
        mapping["source_row_number"] = ordinal + 1
    return row, mapping


def match(actual, wanted, context):
    require(actual is not None, f"Missing row: {context}")
    require(set(actual) == set(wanted), f"Column mismatch: {context}")
    for name, value in wanted.items():
        observed = actual[name]
        # Python의 동등 비교만으로는 bool==0과 float==int도 허용됩니다.
        require(
            type(observed) is type(value) and observed == value,
            f"Value/type mismatch: {context}/{name}",
        )


class Rows:
    """크기를 제한한 part의 코덱을 검증한 뒤 Parquet 레코드 배치를 하나씩 읽습니다."""

    def __init__(self, folder, schema, metadata):
        self.folder = folder.resolve()
        manifest_path = child(folder, "manifest.json")
        schema_path = child(folder, "schema.arrow")
        self.manifest = read_json(manifest_path)
        self.manifest_hash = sha256(child(folder, "manifest.json"))
        stored = pa.ipc.read_schema(pa.BufferReader(schema_path.read_bytes()))
        require(stored.equals(schema), f"Canonical role schema mismatch: {folder}")
        require(
            self.manifest.get("schema")
            == [dict(name=f.name, type=str(f.type), nullable=f.nullable) for f in schema],
            "Manifest schema declaration mismatch",
        )
        self.schema_hash = sha256(child(folder, "schema.arrow"))
        for name, value in metadata.items():
            observed = self.manifest.get(name)
            require(
                type(observed) is type(value) and observed == value,
                f"Manifest binding mismatch: {name}",
            )
        # part의 파일 정보나 Parquet 메타데이터를 읽기 전에 나열된 모든 파일을 검증합니다.
        for part in self.manifest["parts"]:
            for item in part["files"]:
                child(folder, item["path"])
        seen = set()
        total = 0
        for part in self.manifest["parts"]:
            integer(part["rows"], 0, MAX_PART_ROWS, "part rows")
            total += part["rows"]
            for item in part["files"]:
                integer(item["bytes"], 0, MAX_FILE_BYTES, "file bytes")
                integer(item["rows"], 0, MAX_PART_ROWS, "file rows")
                path = child(folder, item["path"])
                require(path not in seen, "Repeated manifest file")
                seen.add(path)
                require(
                    path.stat().st_size == item["bytes"] <= MAX_FILE_BYTES,
                    "Part byte size mismatch or exceeds audit limit",
                )
                require(item["rows"] == part["rows"], "File/part row mismatch")
                if path.suffix == ".parquet":
                    require(
                        pq.ParquetFile(path).metadata.num_rows == part["rows"],
                        "Physical Parquet row count mismatch",
                    )
        declared_files = {p.name for p in seen}
        # manifest.json은 위에서 별도로 검증한 메타데이터이며 데이터 part가 아닙니다.
        present_files = {
            p.name
            for p in folder.iterdir()
            if p.name != "manifest.json" and p.suffix in {".csv", ".json", ".parquet"}
        }
        require(present_files == declared_files, "Unlisted or missing dataset data file")
        require(total == self.manifest["rows"], "Manifest row sum mismatch")
        # 기존의 독립 CSV/JSON/Parquet 재읽기와 저장된 물리·논리 해시를 확인합니다.
        verify_dataset(folder)
        self.last_decimal_wire: dict[str, str | None] = {}
        self.iterator = self._rows(schema)

    def _rows(self, schema):
        for number, part in enumerate(self.manifest["parts"]):
            path = primary_part_path(self.folder, part)
            reader = None
            payload = None
            count = 0
            fields = self.manifest.get("lexical_decimal_fields", [])
            try:
                if path.suffix == ".parquet":
                    reader = pq.ParquetFile(path)
                    require(reader.schema_arrow.equals(schema), "Parquet role schema mismatch")
                    batches = reader.iter_batches(batch_size=BATCH_ROWS, use_threads=False)
                else:
                    table, payload = load_payload(path, schema, include_wire=bool(fields))
                    require(table.schema.equals(schema), "Text role schema mismatch")
                    batches = iter(table.to_batches(max_chunksize=BATCH_ROWS))
                for batch in batches:
                    for row in batch.to_pylist():
                        self.last_decimal_wire = (
                            {name: payload[count][name] for name in fields}
                            if payload is not None
                            else {}
                        )
                        count += 1
                        yield row
            finally:
                if reader is not None:
                    reader.close()
            require(count == part["rows"], "Streamed part count mismatch")
            LOG.info(
                "audit artifact part completed: %s part=%s rows=%s", self.folder, number, count
            )

    def next(self):
        return next(self.iterator, None)

    def finish(self):
        require(self.next() is None, f"Extra artifact row: {self.folder}")
        require(
            sha256(child(self.folder, "manifest.json")) == self.manifest_hash
            and sha256(child(self.folder, "schema.arrow")) == self.schema_hash,
            "Artifact metadata changed during audit",
        )
        for part in self.manifest["parts"]:
            for item in part["files"]:
                require(
                    sha256(child(self.folder, item["path"])) == item["sha256"],
                    "Artifact changed during audit",
                )


def artifact_binding(folder: Path, recorded: dict) -> dict:
    """감사한 값과 파일 목록을 외부 보고서에 고정합니다."""
    tables = {}
    for name, item in recorded["datasets"].items():
        dataset = inside(folder, item["path"])
        tables[name] = dict(
            path=item["path"],
            manifest_sha256=sha256(child(dataset, "manifest.json")),
            schema_sha256=sha256(child(dataset, "schema.arrow")),
        )
    return dict(source_manifest_sha256=sha256(child(folder, "source_manifest.json")), tables=tables)


def audit_source(config, source, validation, folder, state: SourceAuditState, save):
    manifest_path = child(folder, "source_manifest.json")
    source_hash = sha256(manifest_path)
    recorded = read_json(manifest_path)
    require(
        recorded.get("status") == "VERIFIED" and recorded.get("source_id") == source.id,
        "Incomplete/mismatched source manifest",
    )
    LOG.info("audit binding fingerprints: %s", source.id)
    child(validation, "run.json")
    validation_source = inside(validation, source.id)
    report_path = child(validation_source, "validation_report.json")
    child(validation_source, "source_expected.json")  # 선택 항목이며 선언된 경우에만 bind가 엽니다.
    report, fingerprints = bind(source, config, validation)
    state["input_fingerprints"] = fingerprints
    save()
    fingerprint = fingerprints[str(source.path)]
    require(recorded["input_fingerprints"] == fingerprints, "Source fingerprint set mismatch")
    require(recorded["input_rows"] == report["input"]["rows"], "DBcheck row count mismatch")
    integer(recorded["input_rows"], 0, INT64_MAX, "source rows")
    datasets = recorded["datasets"]
    require(
        {"ml_transactions", "ml_labels"} <= set(datasets),
        "Audit requires complete ML transactions and labels",
    )
    metadata = dict(
        source_id=source.id,
        source_kind=source.kind,
        source_sha256=fingerprint,
        raw_input_rows=recorded["input_rows"],
        config_sha256=sha256(config.path),
        dbcheck_config_sha256=config.check.config_sha256,
        aliases_sha256=config.check.aliases_sha256,
        validation_report_sha256=sha256(report_path),
    )
    audited_binding = artifact_binding(folder, recorded)
    streams = {}

    def add(name, schema):
        state.update(stage="CODEC", dataset=name)
        save()
        LOG.info("audit codec verification: %s/%s", source.id, name)
        spec = datasets[name]
        stream = Rows(inside(folder, spec["path"]), schema, metadata)
        require(
            stream.manifest["rows"] == spec["rows"] == recorded["input_rows"],
            "Dataset/source row total mismatch",
        )
        streams[name] = stream

    add("ml_transactions", transaction_schema(source))
    add("ml_labels", label_schema(source))
    version = recorded.get("service_contract_version")
    service = "service_transactions" in datasets or "service_mapping" in datasets
    v4 = False
    if service:
        require(
            source.id == "ibm_hi_small" and source.kind == "ibm",
            "Only HI service candidates are supported",
        )
        require(
            recorded.get("service_selection", {}).get("mode") == "full_source",
            "Audit supports full-source service only; excerpts need an explicit selection auditor",
        )
        if not isinstance(version, str) or version not in {
            "dajaba-service-candidate-v3",
            "dajaba-service-candidate-v4",
        }:
            raise ValueError("Unsupported service version")
        v4 = version.endswith("-v4")
        schema = service_schema(source)
        mapping_schema = SERVICE_MAPPING_SCHEMA
        if not v4:
            schema = pa.schema(
                [f for f in schema if f.name not in {"from_bank_id_raw", "to_bank_id_raw"}]
            )
            mapping_schema = pa.schema([f for f in mapping_schema if f.name != "source_row_number"])
        add("service_transactions", schema)
        add("service_mapping", mapping_schema)
        for name in ("service_transactions", "service_mapping"):
            m = streams[name].manifest
            require(
                m.get("service_contract_version") == version
                and m.get("id_namespace") == "OFFLINE_CANDIDATE_NOT_BACKEND_OPS"
                and m.get("selection", {}).get("mode") == "full_source",
                "Service version/namespace/selection mismatch",
            )
    numeric: NumericAudit = NumericAudit(checked_values=0, incompatible_values=0)
    state.update(
        stage="RAW_ROWS",
        fidelity="RUNNING",
        verified_rows=0,
        service_version=version if service else None,
        service_v4="PRESENT" if v4 else "NOT_PRESENT",
        numeric_20_6=numeric,
        account_values="NOT_AUDITED; auxiliary fingerprints only",
    )
    save()
    with source.path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, strict=True)
        require(
            next(reader, None) == (IBM if source.kind == "ibm" else SAML),
            "Original CSV header mismatch",
        )
        for ordinal, raw in enumerate(reader):
            tx, labels = expected(source, fingerprint, ordinal, raw, config.check.currency_aliases)
            match(streams["ml_transactions"].next(), tx, f"{source.id}/{ordinal}/transaction")
            match(streams["ml_labels"].next(), labels, f"{source.id}/{ordinal}/label")
            if service:
                candidate, mapping = expected_service(tx, fingerprint, version)
                match(
                    streams["service_transactions"].next(),
                    candidate,
                    f"{source.id}/{ordinal}/service",
                )
                match(streams["service_mapping"].next(), mapping, f"{source.id}/{ordinal}/mapping")
                # typed 값 동등성과 원문 자릿수 동등성은 별도로 확인합니다.
                for name, value in streams["service_transactions"].last_decimal_wire.items():
                    require(
                        value == tx[name + "_raw"],
                        "Service original decimal spelling differs from source",
                    )

            # 금액 저장 가능 여부는 원본 충실성이나 원천 정책의 충족 여부와 다릅니다.
            values = (
                [tx["amount_paid"], tx["amount_received"]]
                if source.kind == "ibm"
                else [tx["amount"]]
            )
            for value in values:
                if not isinstance(value, Decimal):
                    raise ValueError("Expected source amount must be Decimal")
                numeric["checked_values"] += 1
                numeric["incompatible_values"] += not fits_service_numeric(value)
            state["verified_rows"] = ordinal + 1
            if (ordinal + 1) % 25000 == 0:
                save()
                LOG.info("audit raw rows: %s %s/%s", source.id, ordinal + 1, recorded["input_rows"])
    require(
        state["verified_rows"] == recorded["input_rows"], "Raw/source manifest row total mismatch"
    )
    for stream in streams.values():
        stream.finish()
    if source.accounts is not None and "ml_accounts" in datasets:
        state.update(stage="ACCOUNT_MASTER")
        save()
        master = Rows(inside(folder, datasets["ml_accounts"]["path"]), ACCOUNT_SCHEMA, metadata)
        account_rows = 0
        try:
            require(
                recorded["source_release"] == source.id + ":" + fingerprint,
                "Account source release differs",
            )
            with source.accounts.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle, strict=True)
                require(next(reader, None) == ACCOUNT_HEADER, "Original account header differs")
                for account_rows, raw in enumerate(reader, 1):
                    require(len(raw) == 5, "Original account column count differs")
                    wanted = dict(
                        account_key=json.dumps(
                            [source.id + ":" + fingerprint, bank(raw[1]), raw[2]],
                            separators=(",", ":"),
                        ),
                        bank_raw=raw[1],
                        account_raw=raw[2],
                        entity_id_raw=raw[3],
                        entity_name_raw=raw[4],
                        bank_name_raw=raw[0],
                    )
                    match(master.next(), wanted, f"{source.id}/master/{account_rows}")
            master.finish()
            require(
                account_rows == datasets["ml_accounts"]["rows"],
                "Original account master coverage differs",
            )
            state["account_values"] = (
                "PASS; original master values, independent keys, and all declared formats"
            )
        finally:
            master.iterator.close()
    require(
        artifact_binding(folder, recorded) == audited_binding, "Audited artifact metadata changed"
    )
    state["artifact_binding"] = audited_binding
    for path, digest in fingerprints.items():
        require(sha256(Path(path)) == digest, f"Original changed during audit: {path}")
    require(sha256(manifest_path) == source_hash, "Source manifest changed during audit")
    state.update(stage="COMPLETE", fidelity="PASS")
    numeric["status"] = (
        "INCOMPATIBLE_VALUES" if numeric["incompatible_values"] else "EXACTLY_REPRESENTABLE"
    )
    state["direct_copy_ready"] = False
    save()


def audit_run(config, root, output, selected: list[str] | None = None) -> AuditReport:
    """조립 run을 변경하지 않고 별도의 새 감사 디렉터리를 만듭니다."""
    root, output = root.resolve(), output.resolve()
    protected = [root, config.output.resolve(), config.check.output.resolve()]
    protected += [
        p.parent.resolve()
        for s in config.check.sources
        for p in (s.path, s.accounts, s.patterns)
        if p is not None
    ]
    require(
        not any(output == p or output.is_relative_to(p) for p in protected),
        "Audit output must be outside raw, DBcheck and assembly artifact directories",
    )
    require(not output.exists(), "Audit output must be a new directory")
    run_path = child(root, "run.json")
    run = read_json(run_path)
    run_hash = sha256(child(root, "run.json"))
    require(
        run.get("status") == "COMPLETED" and run.get("contract_version") == VERSION,
        "Unsupported/incomplete assembly run",
    )
    validation = Path(run["binding"]["validation_run"]).resolve()
    require(
        validation.is_relative_to(config.check.output.resolve()),
        "DBcheck run escapes configured validation output",
    )
    validation_run_path = child(validation, "run.json")
    require(sha256(config.path) == run["binding"]["config_sha256"], "Assembly config changed")
    require(
        sha256(validation_run_path) == run["binding"]["validation_run_sha256"],
        "DBcheck run metadata changed",
    )
    sources = {s.id: s for s in config.check.sources}
    require(set(run["sources"]) <= set(sources), "Unknown assembly source")
    require(
        set(run["binding"]["selected_sources"]) == set(run["sources"]),
        "Run selected-source set mismatch",
    )
    require(
        len(run["binding"]["selected_sources"]) == len(run["sources"]), "Duplicate selected source"
    )
    selected_ids = set(selected or run["sources"])
    require(
        bool(selected_ids) and selected_ids <= set(run["sources"]),
        "Unknown/empty audit source selection",
    )
    output.mkdir(parents=True, exist_ok=False)
    result: AuditReport = AuditReport(
        audit_version=AUDIT_VERSION,
        status="RUNNING",
        assembly_run=str(root),
        started_at=datetime.now(timezone.utc).isoformat(),
        sources={},
        resume_supported=False,
        report_is_not_checkpoint=True,
        audit_code_sha256=sha256(Path(__file__)),
        assembly_run_sha256=run_hash,
        config_sha256=sha256(config.path),
        validation_run_sha256=sha256(validation_run_path),
    )

    def save():
        temp = output / "audit.json.tmp"
        report = output / "audit.json"
        try:
            temp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            with temp.open("r+b") as stream:
                os.fsync(stream.fileno())
            replace_closed_json(temp, report)
        except BaseException as exc:
            # 교체 실패 시 이전 보고서와 임시 파일을 보존하고 미완료 상태를 알립니다.
            note = (
                f"Audit report update was not committed: {report}. "
                f"The report may be missing or stale; {temp} is uncommitted if present."
            )
            exc.add_note(note)
            LOG.error("%s (%s: %s)", note, type(exc).__name__, exc, exc_info=True)
            raise

    save()
    failure: BaseException | None = None
    try:
        for source_id, item in run["sources"].items():
            if source_id not in selected_ids:
                continue
            state: SourceAuditState = SourceAuditState(
                stage="BINDING", fidelity="NOT_CHECKED", verified_rows=0
            )
            result["sources"][source_id] = state
            save()
            require(item.get("status") == "VERIFIED", "Run source is incomplete")
            folder = inside(root, item["folder"])
            require(
                read_json(child(folder, "source_manifest.json"))["datasets"] == item["datasets"],
                "Run/source dataset index mismatch",
            )
            require(
                read_json(child(folder, "source_manifest.json"))["input_rows"]
                == item["input_rows"],
                "Run/source input row count mismatch",
            )
            audit_source(config, sources[source_id], validation, folder, state, save)
        require(sha256(child(root, "run.json")) == run_hash, "Assembly run metadata changed")
        result["status"] = "PASS"
    except BaseException as exc:
        failure = exc
        result.update(
            status="INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise
    finally:
        # 종료 시각을 포함한 최종 보고서는 성공·실패·취소 모두 한 번만 저장합니다.
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        try:
            save()
        except BaseException as save_error:
            if failure is None:
                raise
            # 저장 오류는 위 로그와 원래 예외의 주석으로 남기고 traceback을 보존합니다.
            failure.add_note(
                "Final audit report was not committed: "
                f"{type(save_error).__name__}: {save_error}. See the report-write error log."
            )
    return result
