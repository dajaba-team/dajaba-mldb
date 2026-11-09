"""최신 검증 근거를 실제 변환 대상 파일과 정확히 연결합니다."""

import json
from pathlib import Path

from dbcheck.config import CONTRACT_VERSION, Source
from dbcheck.data_contract import POLICY_VERSION
from dbcheck.distribution import sha256

from .config import Config


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def bind(source: Source, config: Config, run: Path) -> tuple[dict, dict[str, str]]:
    meta = read_json(run / "run.json")
    report = read_json(run / source.id / "validation_report.json")
    if meta.get("contract_version") != CONTRACT_VERSION or meta.get("status") not in {
        "COMPLETED",
        "FAILED",
    }:
        raise ValueError("Validation run is incomplete or has an unsupported contract")
    if report.get("source_id") != source.id or report.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("Validation source/contract mismatch")
    if not report.get("full_scan_complete") or report.get("status") not in {"PASS", "WARN"}:
        raise ValueError("FAIL or incomplete validation blocks this source")
    if report.get("money_identity_policy_version") != POLICY_VERSION:
        raise ValueError("Validation uses an old money/identity policy; rerun DBCheck")
    if not report.get("source_unchanged") or report.get("scope") != "FULL_ORIGINAL":
        raise ValueError("Full immutable-original validation is required")
    if report.get("source_expected"):
        evidence = report["source_expected"]
        path = run / source.id / "source_expected.json"
        if evidence["path"] != "source_expected.json" or sha256(path) != evidence["sha256"]:
            raise ValueError("Source expected-count evidence changed")
        observed = read_json(path)
        if (
            observed["source_id"] != source.id
            or observed["source_sha256"] != report["input"]["sha256"]
            or observed["transaction_rows"] != report["input"]["rows"]
            or observed["scope"] != "FULL_ORIGINAL"
        ):
            raise ValueError("Source expected-count evidence differs from validation")
    if (
        report.get("config_sha256") != config.check.config_sha256
        or report.get("aliases_sha256") != config.check.aliases_sha256
    ):
        raise ValueError("DBCheck config/currency map changed; rerun validation")
    observed_warnings = {r["rule"] for r in report["checks"] if r["status"] == "WARN"}
    if any(r["status"] == "FAIL" for r in report["checks"]):
        raise ValueError("Validation contains FAIL rules")
    unacknowledged = observed_warnings - config.acknowledged_warnings
    if unacknowledged:
        raise ValueError(f"Explicit policy acknowledgement required: {sorted(unacknowledged)}")
    if (
        report.get("exact_amount_contract", {}).get("precision") != source.amount_precision
        or report.get("exact_amount_contract", {}).get("scale") != source.amount_scale
    ):
        raise ValueError("Amount contract differs from validation")
    fingerprints = {str(source.path): sha256(source.path)}
    if (
        fingerprints[str(source.path)] != report["input"]["sha256"]
        or Path(report["input"]["path"]).resolve() != source.path
    ):
        raise ValueError("Original path/fingerprint differs from validation")
    expected = {
        str(p): role
        for p, role in [(source.accounts, "accounts"), (source.patterns, "patterns")]
        if p is not None and p.is_file()
    }
    recorded = {str(Path(item["path"]).resolve()): item for item in report["auxiliary_inputs"]}
    if len(recorded) != len(report["auxiliary_inputs"]) or set(recorded) != set(expected):
        raise ValueError("Auxiliary input set changed or contains duplicate validation records")
    for path, role in expected.items():
        fingerprints[path] = sha256(Path(path))
        if (
            path not in recorded
            or recorded[path]["sha256"] != fingerprints[path]
            or recorded[path]["role"] != role
        ):
            raise ValueError(f"Auxiliary input changed: {role}")
    return report, fingerprints
