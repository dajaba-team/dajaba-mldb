"""원천을 독립적으로 검증하고 해당 원천에 연결된 보고서 계약을 발행합니다."""

import hashlib
import importlib.metadata
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .checks import detailed_checks
from .config import CONTRACT_VERSION, Config
from .distribution import profile, sha256, write_json
from .patterns import PATTERN_CONTRACT, PATTERN_REPORT, PatternMembership
from .reporting import _build_source_report, _expected_counts, _write_source_summary

LOG = logging.getLogger("dbcheck")


def execute(config: Config, selected: list[str] | None = None) -> tuple[Path, bool]:
    selected_ids = set(selected or [s.id for s in config.sources])
    unknown = selected_ids - {s.id for s in config.sources}
    if unknown:
        raise ValueError(f"Unknown source ids: {sorted(unknown)}")
    root = config.output / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    )
    root.mkdir(parents=True, exist_ok=False)
    code = {
        str(p.relative_to(Path(__file__).parent)): sha256(p)
        for p in Path(__file__).parent.rglob("*.py")
    }
    code_digest = hashlib.sha256(str(sorted(code.items())).encode()).hexdigest()
    manifest: dict[str, Any] = dict(
        contract_version=CONTRACT_VERSION,
        status="RUNNING",
        run_id=root.name,
        started_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        config_path=str(config.path),
        packages={name: importlib.metadata.version(name) for name in ("pyarrow", "polars")},
        config_sha256=config.config_sha256,
        aliases_sha256=config.aliases_sha256,
        code_files=code,
        code_sha256=code_digest,
        source_independent=True,
        training_executed=False,
        normalized_data_emitted=False,
        cache_reused=False,
        sources=[],
    )
    write_json(root / "run.json", manifest)
    for source in config.sources:
        if source.id not in selected_ids:
            continue
        folder = root / source.id
        folder.mkdir()
        try:
            LOG.info("%s: raw full scan", source.id)
            membership = PatternMembership(source)
            raw = profile(
                source.id, str(source.path), source.kind, folder, pattern_membership=membership
            )
            details = detailed_checks(source, config, folder)
            if sha256(source.path) != raw["sha256"]:
                raise RuntimeError("Transactions changed during detailed checks")
            if source.kind == "ibm":
                pattern_inputs = [
                    item for item in details["auxiliary_inputs"] if item["role"] == "patterns"
                ]
                observed = [item["sha256"] for item in pattern_inputs]
                expected = [membership.pattern_sha256] if membership.pattern_sha256 else []
                current = (
                    sha256(source.patterns)
                    if source.patterns is not None and source.patterns.is_file()
                    else None
                )
                if observed != expected or current != membership.pattern_sha256:
                    raise RuntimeError("Patterns changed during membership verification")
                if membership.membership_scanned:
                    pattern_inputs[0]["scope"] = (
                        "fingerprint and full original-row membership; see pattern_membership"
                    )
            pattern_report = membership.report(source, raw)
            pattern_path = folder / PATTERN_REPORT
            write_json(pattern_path, pattern_report)
            pattern_ref = dict(
                contract_version=PATTERN_CONTRACT,
                path=PATTERN_REPORT,
                sha256=sha256(pattern_path),
                status=pattern_report["status"],
                reason=pattern_report["reason"],
            )
            result = _build_source_report(
                source, config, raw, details, code_digest, pattern_membership=pattern_ref
            )
            status = result["status"]
            expected = _expected_counts(source, raw, details, status)
            write_json(folder / "source_expected.json", expected)
            result["source_expected"] = dict(
                path="source_expected.json", sha256=sha256(folder / "source_expected.json")
            )
            write_json(folder / "validation_report.json", result)
            manifest["sources"].append(
                dict(
                    id=source.id,
                    status=status,
                    scan_complete=True,
                    rows=raw["rows"],
                    sha256=raw["sha256"],
                    report=str(folder / "validation_report.json"),
                )
            )
            _write_source_summary(folder, source, raw, status, result["checks"])
        except Exception as exc:
            LOG.exception("%s: validation failed; independent sources continue", source.id)
            result = dict(
                contract_version=CONTRACT_VERSION,
                status="FAIL",
                source_id=source.id,
                full_scan_complete=False,
                error_type=type(exc).__name__,
                error=str(exc),
                checks=[
                    dict(
                        rule="SOURCE_READ_OR_VALIDATION_FAILURE",
                        status="FAIL",
                        affected_rows=None,
                        action="Check dataset path, header and log; correct configuration, never overwrite original",
                    )
                ],
            )
            write_json(folder / "validation_report.json", result)
            manifest["sources"].append(
                dict(
                    id=source.id,
                    status="FAIL",
                    scan_complete=False,
                    error=str(exc),
                    report=str(folder / "validation_report.json"),
                )
            )
        write_json(root / "run.json", manifest)
    success = all(s["status"] != "FAIL" for s in manifest["sources"])
    manifest.update(
        status="COMPLETED" if success else "FAILED",
        finished_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    write_json(root / "run.json", manifest)
    (root / "summary.md").write_text(
        "# DBCheck 검증 결과\n\n"
        + "\n".join(
            f"- {s['id']}: {s['status']}; 전체 스캔 {s['scan_complete']}"
            for s in manifest["sources"]
        )
        + "\n",
        encoding="utf-8",
    )
    return root, success
