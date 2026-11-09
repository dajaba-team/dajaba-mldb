"""작은 파일로 계약 회귀를 확인합니다. 모델이나 실제 데이터 쓰기는 사용하지 않습니다."""

import csv
import json
import subprocess
import sys
from dataclasses import replace

import pytest

from dbcheck.config import CONTRACT_VERSION, Config, Source, load
from dbcheck.distribution import IBM, SAML, sha256
from dbcheck.pipeline import execute


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def setup(tmp_path):
    raw = tmp_path / "input" / "transactions.csv"
    account = tmp_path / "input" / "accounts.csv"
    row = [
        "2022/09/01 00:00",
        "001",
        "0001",
        "2",
        "0002",
        "123.45",
        "US Dollar",
        "123.45",
        "US Dollar",
        "Wire",
        "0",
    ]
    write_csv(raw, IBM, [row, row[:10] + ["1"]])
    write_csv(
        account,
        ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"],
        [["A", "1", "0001", "E1", "One"], ["B", "2", "0002", "E2", "Two"]],
    )
    source = Source("ibm_hi_small", "ibm", raw, "AMLWORLD_ILLICIT_FLOW", account)
    cfg = Config(
        tmp_path / "config.toml",
        tmp_path / "reports",
        (source,),
        {"US Dollar": "USD"},
        "cfg",
        "alias",
        1,
    )
    return cfg, source, row


def result(root, source):
    return json.loads((root / source.id / "validation_report.json").read_text(encoding="utf-8"))


def checks(report):
    return {r["rule"]: r for r in report["checks"]}


def test_full_scan_preserves_raw_identity_and_has_no_ml(setup):
    cfg, s, _ = setup
    before = sha256(s.path)
    root, ok = execute(cfg)
    r = result(root, s)
    assert ok and r["input"]["rows"] == 2 and r["source_unchanged"] and sha256(s.path) == before
    assert r["rows_changed"] == r["rows_deleted"] == 0
    assert checks(r)["IBM_SENDER_ACCOUNT_REFERENCE"]["status"] == "PASS"
    assert checks(r)["IBM_SENDER_RAW_BANK_SPELLING_MISMATCH"]["affected_rows"] == 2
    run = json.loads((root / "run.json").read_text())
    assert not run["training_executed"] and not run["normalized_data_emitted"]
    assert not list(root.rglob("*.parquet"))
    expected = json.loads((root / s.id / "source_expected.json").read_text())
    assert expected["transaction_rows"] == 2
    assert expected["auxiliary_rows"]["accounts"] == 2
    assert expected["source_sha256"] == before
    assert expected["backend_copy_verified"] is False
    assert expected["rule_expectations"] is None


def test_duplicate_rows_warn_and_are_not_removed(setup):
    cfg, s, row = setup
    write_csv(s.path, IBM, [row, row])
    root, ok = execute(cfg)
    r = result(root, s)
    dup = checks(r)["EXACT_RAW_DUPLICATE_CANDIDATE"]
    assert ok and dup["status"] == "WARN" and dup["affected_rows"] == 2 and dup["excess_rows"] == 1
    assert r["input"]["rows"] == 2 and r["rows_deleted"] == 0


@pytest.mark.parametrize(
    "index,value,rule",
    [
        (0, "2022/02/30 00:00", "TIMESTAMP_PARSING"),
        (10, "X", "BINARY_LABEL"),
        (7, "-5", "AMOUNT_7_INVALID"),
        (2, "UNKNOWN", "ACCOUNT_IDENTIFIER_UNRESOLVED"),
        (2, "", "REQUIRED_VALUE_EMPTY"),
        (1, "ABC", "BANK_1_FORMAT"),
    ],
)
def test_invalid_values_fail_without_modifying_source(setup, index, value, rule):
    cfg, s, row = setup
    bad = row.copy()
    bad[index] = value
    write_csv(s.path, IBM, [bad])
    before = sha256(s.path)
    root, ok = execute(cfg)
    r = result(root, s)
    assert not ok and checks(r)[rule]["status"] == "FAIL" and sha256(s.path) == before


def test_decimal_excess_scale_and_unknown_currency_are_not_filled(setup):
    cfg, s, row = setup
    row[7] = "1.1234567890123"
    row[8] = "Dirham"
    write_csv(s.path, IBM, [row])
    root, ok = execute(cfg)
    r = result(root, s)
    assert ok and checks(r)["AMOUNT_7_DECIMAL_CAPACITY"]["affected_rows"] == 1
    assert checks(r)["CURRENCY_ALIAS_UNVERIFIED"]["status"] == "WARN"


def test_missing_account_fails(setup):
    cfg, s, row = setup
    row[2] = "not-present"
    write_csv(s.path, IBM, [row])
    root, ok = execute(cfg)
    assert not ok and checks(result(root, s))["IBM_SENDER_ACCOUNT_REFERENCE"]["status"] == "FAIL"


def test_owner_conflict_blocks_zero_padding_alias(setup):
    cfg, s, _ = setup
    with s.accounts.open("a", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(["A", "01", "0001", "OTHER", "Other"])
    root, ok = execute(cfg)
    assert (
        not ok
        and checks(result(root, s))["IBM_NORMALIZED_ACCOUNT_OWNER_CONFLICT"]["status"] == "FAIL"
    )


def test_bad_source_does_not_block_independent_source(setup):
    cfg, s, _ = setup
    bad = replace(s, id="bad", path=s.path.with_name("bad.csv"))
    bad.path.write_text("wrong,header\n1,2\n", encoding="utf-8")
    root, ok = execute(replace(cfg, sources=(bad, s)))
    assert not ok and not result(root, bad)["full_scan_complete"]
    assert result(root, s)["full_scan_complete"]


def test_saml_single_amount_and_structural_absence(setup):
    cfg, s, _ = setup
    raw = s.path.with_name("saml.csv")
    write_csv(
        raw,
        SAML,
        [
            [
                "12:00:00",
                "2022-01-01",
                "00001",
                "00002",
                "5",
                "US dollar",
                "Euro",
                "UK",
                "USA",
                "ACH",
                "1",
                "Layering",
            ]
        ],
    )
    saml = Source("saml_d", "saml", raw, "SOURCE_DESIGNATED_SUSPICIOUS")
    root, ok = execute(
        replace(cfg, sources=(saml,), currency_aliases={"US dollar": "USD", "Euro": "EUR"})
    )
    r = result(root, saml)
    assert ok and checks(r)["SAML_CURRENCY_BASIS_UNRESOLVED"]["affected_rows"] == 1
    assert checks(r)["BANK_CUSTOMER_REFERENCE"]["status"] == "WARN"
    assert not r["auxiliary_inputs"]


def test_hour_gap_is_counted(setup):
    cfg, s, row = setup
    later = row.copy()
    later[0] = "2022/09/01 02:00"
    write_csv(s.path, IBM, [row, later])
    root, ok = execute(cfg)
    assert ok and checks(result(root, s))["TIME_OBSERVATION_GAPS"]["affected_rows"] == 1


def test_cli_config_paths_and_input_directory_guard(setup):
    cfg, s, _ = setup
    aliases = s.path.parent.parent / "currencies.json"
    aliases.write_text('{"confirmed":{"US Dollar":"USD"}}')
    cfg.path.write_text(f'''schema_version="{CONTRACT_VERSION}"
output_root="reports"
currency_map="currencies.json"
[[sources]]
id="ibm_hi_small"
kind="ibm"
path="input/transactions.csv"
accounts="input/accounts.csv"
label_semantics="AMLWORLD_ILLICIT_FLOW"
''')
    assert load(cfg.path).sources[0].path == s.path
    proc = subprocess.run(
        [sys.executable, "-m", "dbcheck", "check", "--config", str(cfg.path)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0 and "VALIDATION_DIRECTORY=" in proc.stdout
    cfg.path.write_text(
        cfg.path.read_text().replace('output_root="reports"', 'output_root="input/reports"')
    )
    with pytest.raises(ValueError, match="outside"):
        load(cfg.path)


def test_old_model_commands_are_rejected():
    proc = subprocess.run(
        [sys.executable, "-m", "dbcheck", "benchmark"], capture_output=True, text=True
    )
    assert proc.returncode == 2 and "invalid choice" in proc.stderr


def test_detail_worker_failure_does_not_kill_other_source(setup):
    cfg, source, _ = setup
    bad_accounts = source.accounts.with_name("bad_accounts.csv")
    bad_accounts.write_text("bad,header\n1,2\n", encoding="utf-8")
    bad = replace(source, id="bad_accounts", accounts=bad_accounts)
    root, ok = execute(replace(cfg, sources=(bad, source)))
    assert not ok
    assert not result(root, bad)["full_scan_complete"]
    assert "worker exit=" in result(root, bad)["error"]
    assert (root / bad.id / "detail_error.json").is_file()
    assert result(root, source)["full_scan_complete"]


def test_same_raw_input_and_config_produce_same_validation_contract(setup):
    cfg, source, _ = setup
    first, first_ok = execute(cfg)
    second, second_ok = execute(cfg)
    assert first_ok and second_ok
    assert first != second
    assert result(first, source) == result(second, source)


def test_source_specific_precision_removes_the_actual_capacity_mismatch(setup):
    cfg, source, row = setup
    row[7] = "3469.0028040888333"
    write_csv(source.path, IBM, [row])
    source = replace(source, amount_scale=16)
    root, ok = execute(replace(cfg, sources=(source,)))
    report = result(root, source)
    assert ok and checks(report)["AMOUNT_7_DECIMAL_CAPACITY"]["status"] == "PASS"
    assert report["exact_amount_contract"]["scale"] == 16
    assert report["bank_identity_bridge"]["mapping_unique_and_complete"]


@pytest.mark.parametrize("token", ["미확정", " unconfirmed ", " Not Applicable "])
def test_saml_placeholder_account_fails_without_master_reference(setup, token):
    cfg, original, _ = setup
    path = original.path.with_name("saml.csv")
    row = [
        "12:00:00",
        "2022-01-01",
        token,
        "00002",
        "5",
        "US Dollar",
        "US Dollar",
        "UK",
        "USA",
        "ACH",
        "1",
        "Layering",
    ]
    write_csv(path, SAML, [row])
    before = sha256(path)
    source = Source("saml_d", "saml", path, "SOURCE_DESIGNATED_SUSPICIOUS", amount_scale=16)
    root, ok = execute(replace(cfg, sources=(source,)))
    report = result(root, source)
    assert not ok and report["full_scan_complete"]
    assert checks(report)["ACCOUNT_IDENTIFIER_UNRESOLVED"]["status"] == "FAIL"
    assert checks(report)["ACCOUNT_IDENTIFIER_UNRESOLVED"]["affected_rows"] == 1
    assert sha256(path) == before


@pytest.mark.parametrize("kind", ["ibm", "saml"])
def test_header_only_source_has_explicit_empty_failure_and_cli_exit_two(setup, kind):
    cfg, source, _ = setup
    if kind == "saml":
        source = replace(
            source, kind=kind, accounts=None, label_semantics="SOURCE_DESIGNATED_SUSPICIOUS"
        )
    write_csv(source.path, IBM if kind == "ibm" else SAML, [])
    before = sha256(source.path)
    root, ok = execute(replace(cfg, sources=(source,)))
    report = result(root, source)
    assert not ok and report["input"]["rows"] == 0 and report["full_scan_complete"]
    assert checks(report)["EMPTY_SOURCE"]["status"] == "FAIL"
    assert "error_type" not in report and sha256(source.path) == before
    # Python 호출자뿐 아니라 터미널 사용자에게도 같은 실패가 드러나야 합니다.
    aliases = cfg.path.with_name("currencies.json")
    aliases.write_text('{"confirmed":{"US Dollar":"USD"}}', encoding="utf-8")
    accounts_line = 'accounts="input/accounts.csv"' if kind == "ibm" else ""
    cfg.path.write_text(
        f'''schema_version="{CONTRACT_VERSION}"
output_root="reports"
currency_map="currencies.json"
[[sources]]
id="{source.id}"
kind="{kind}"
path="input/transactions.csv"
label_semantics="{source.label_semantics}"
{accounts_line}
''',
        encoding="utf-8",
    )
    proc = subprocess.run(
        [sys.executable, "-m", "dbcheck", "check", "--config", str(cfg.path)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2 and "VALIDATION_DIRECTORY=" in proc.stdout
    assert "KeyError" not in proc.stderr


def test_rule_order_counts_and_expected_handoff_contract(setup):
    cfg, source, _ = setup
    root, ok = execute(cfg)
    report = result(root, source)
    assert ok and report["status"] == "WARN"
    assert [(r["rule"], r["status"], r["affected_rows"]) for r in report["checks"]] == [
        ("EXPECTED_TIME_RANGE", "WARN", None),
        ("AMOUNT_5_INVALID", "PASS", 0),
        ("AMOUNT_5_DECIMAL_CAPACITY", "PASS", 0),
        ("AMOUNT_5_ZERO", "PASS", 0),
        ("AMOUNT_7_INVALID", "PASS", 0),
        ("AMOUNT_7_DECIMAL_CAPACITY", "PASS", 0),
        ("AMOUNT_7_ZERO", "PASS", 0),
        ("REQUIRED_VALUE_EMPTY", "PASS", 0),
        ("ACCOUNT_IDENTIFIER_UNRESOLVED", "PASS", 0),
        ("CURRENCY_ALIAS_UNVERIFIED", "PASS", 0),
        ("PAYMENT_TYPE_UNMAPPED", "PASS", 0),
        ("EXACT_RAW_DUPLICATE_CANDIDATE", "PASS", 0),
        ("IBM_ACCOUNT_REQUIRED_VALUES", "PASS", 0),
        ("IBM_NORMALIZED_ACCOUNT_KEY_DUPLICATE", "PASS", 0),
        ("IBM_NORMALIZED_ACCOUNT_OWNER_CONFLICT", "PASS", 0),
        ("IBM_SENDER_ACCOUNT_REFERENCE", "PASS", 0),
        ("IBM_SENDER_RAW_BANK_SPELLING_MISMATCH", "WARN", 2),
        ("IBM_RECEIVER_ACCOUNT_REFERENCE", "PASS", 0),
        ("IBM_RECEIVER_RAW_BANK_SPELLING_MISMATCH", "PASS", 0),
        ("TIMESTAMP_PARSING", "PASS", 0),
        ("BINARY_LABEL", "PASS", 0),
        ("TIME_FILE_ORDER", "PASS", 0),
        ("TIME_OBSERVATION_GAPS", "PASS", 0),
        ("UNKNOWN_TOKENS", "PASS", 0),
        ("NOT_APPLICABLE_TOKENS", "PASS", 0),
        ("BANK_1_FORMAT", "PASS", 0),
        ("BANK_3_FORMAT", "PASS", 0),
        ("NORMAL_VOLUME_DROP_CANDIDATE", "PASS", 0),
    ]
    expected_path = root / source.id / "source_expected.json"
    assert report["source_expected"] == {
        "path": "source_expected.json",
        "sha256": sha256(expected_path),
    }
    assert json.loads(expected_path.read_text(encoding="utf-8")) == {
        "version": "dajaba-source-counts-v1",
        "source_id": "ibm_hi_small",
        "source_sha256": sha256(source.path),
        "transaction_rows": 2,
        "source_label_counts": {"0": 1, "1": 1},
        "label_counts_scope": "OFFLINE_VALIDATION_ONLY; never inference input",
        "daily_rows": {"2022-09-01": 2},
        "currency_pair_rows": {"US Dollar -> US Dollar": 2},
        "auxiliary_rows": {"accounts": 2},
        "timestamp_min": "2022-09-01 00:00:00",
        "timestamp_max": "2022-09-01 00:00:00",
        "timezone": "UNKNOWN",
        "scope": "FULL_ORIGINAL",
        "validation_status": "WARN",
        "backend_copy_verified": False,
        "rule_expectations": None,
    }
    summary = (root / source.id / "summary.md").read_text(encoding="utf-8")
    assert summary.startswith("# ibm_hi_small: WARN\n\n") and summary.endswith("\n")
    table_rows = [line for line in summary.splitlines() if line.startswith("| ")][1:]
    assert [line.split("|")[1].strip() for line in table_rows] == [
        r["rule"] for r in report["checks"]
    ]
    assert [line.split("|")[2].strip() for line in table_rows] == [
        r["status"] for r in report["checks"]
    ]


def test_failure_report_schema_and_following_source_are_preserved(setup):
    cfg, source, _ = setup
    bad = replace(source, id="bad_report", path=source.path.with_name("bad_report.csv"))
    bad.path.write_text("wrong,header\n1,2\n", encoding="utf-8")
    before = sha256(bad.path)
    root, ok = execute(replace(cfg, sources=(bad, source)))
    message = "bad_report: HEADER_MISMATCH; inspect original contract"
    assert not ok and sha256(bad.path) == before
    assert result(root, bad) == {
        "contract_version": CONTRACT_VERSION,
        "status": "FAIL",
        "source_id": "bad_report",
        "full_scan_complete": False,
        "error_type": "ValueError",
        "error": message,
        "checks": [
            {
                "rule": "SOURCE_READ_OR_VALIDATION_FAILURE",
                "status": "FAIL",
                "affected_rows": None,
                "action": (
                    "Check dataset path, header and log; correct configuration, "
                    "never overwrite original"
                ),
            }
        ],
    }
    run = json.loads((root / "run.json").read_text(encoding="utf-8"))
    assert run["status"] == "FAILED"
    assert [entry["id"] for entry in run["sources"]] == ["bad_report", "ibm_hi_small"]
    assert run["sources"][0] == {
        "id": "bad_report",
        "status": "FAIL",
        "scan_complete": False,
        "error": message,
        "report": str(root / bad.id / "validation_report.json"),
    }
    assert run["sources"][1]["scan_complete"] is True
    assert result(root, source)["full_scan_complete"] is True
    assert not (root / bad.id / "source_expected.json").exists()
