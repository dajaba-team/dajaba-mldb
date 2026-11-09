"""Report construction preserves the existing validation contract and severity."""

import pytest

from dbcheck.config import Config, Source
from dbcheck.pipeline import _build_source_report as pipeline_report
from dbcheck.reporting import _build_source_report, _expected_counts, _write_source_summary


@pytest.mark.parametrize(
    "rows,parse_errors,status", [(2, 0, "PASS"), (2, 1, "FAIL"), (0, 0, "FAIL")]
)
def test_report_contract(tmp_path, rows, parse_errors, status):
    source = Source("ibm_hi_small", "ibm", tmp_path / "source.csv", "AMLWORLD_ILLICIT_FLOW")
    config = Config(tmp_path / "config.toml", tmp_path, (source,), {}, "cfg", "alias", 1)
    raw = dict(
        rows=rows,
        sha256="source-hash",
        quality={
            "timestamp_parse_failure": parse_errors,
            "invalid_label": 0,
            "file_order_time_regressions": 0,
            "bank_1_non_digit": 0,
            "bank_3_non_digit": 0,
        },
        no_observation_hours=0,
        unknown_token_cells={},
        na_token_cells={},
        distribution_candidates=[],
        labels={"0": rows},
        daily=[],
        currency_pairs={},
        timestamp_min=None,
        timestamp_max=None,
    )
    details = dict(rules=[], auxiliary_inputs=[], exact_amount_contract={}, bank_identity_bridge={})
    report = _build_source_report(source, config, raw, details, "code-hash")
    assert pipeline_report is _build_source_report
    assert report["status"] == status
    assert report["full_scan_complete"] is True
    assert report["rows_changed"] == report["rows_deleted"] == 0
    assert report["input"] == dict(path=str(source.path), sha256="source-hash", rows=rows)
    assert report["source_unchanged"] is True
    expected = _expected_counts(source, raw, details, status)
    assert expected["transaction_rows"] == rows
    assert expected["validation_status"] == status
    assert expected["backend_copy_verified"] is False
    assert expected["rule_expectations"] is None
    _write_source_summary(tmp_path, source, raw, status, report["checks"])
    assert (tmp_path / "summary.md").read_text(encoding="utf-8").startswith(
        f"# ibm_hi_small: {status}\n"
    )
    assert not source.path.exists()
