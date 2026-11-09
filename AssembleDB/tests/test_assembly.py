"""작은 실제 파일로 DBCheck → 조립 → 독립 재읽기 경로의 회귀를 확인합니다."""

import csv
import hashlib
import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dbcheck.config import CONTRACT_VERSION
from dbcheck.config import load as load_check
from dbcheck.distribution import IBM, SAML, sha256
from dbcheck.pipeline import execute as check

from assemble_db import pipeline
from assemble_db.config import ServiceSelection, load
from assemble_db.contracts import bind
from assemble_db.exporting import DatasetWriter, verify_dataset
from assemble_db.pipeline import execute, verify_run


def csv_file(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def fixture(tmp_path):
    inputs = tmp_path / "input"
    inputs.mkdir()
    rows = [
        [
            f"2022/09/{day:02d} 00:00",
            "001",
            "000A",
            "2",
            "000B",
            "1.123456789012",
            "US Dollar",
            "1.123456789012",
            "US Dollar",
            "Wire",
            str(day % 2),
        ]
        for day in [1, 2, 3, 4]
    ]
    csv_file(inputs / "hi.csv", IBM, rows)
    csv_file(
        inputs / "accounts.csv",
        ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"],
        [["Bank A", "1", "000A", "E1", ""], ["Bank B", "2", "000B", "E2", "\\N"]],
    )
    csv_file(
        inputs / "saml.csv",
        SAML,
        [
            [
                "12:00:00",
                "2022-01-01",
                "000A",
                "000B",
                "3469.0028040888333",
                "US Dollar",
                "Euro",
                "UK",
                "USA",
                "ACH",
                "1",
                "Layering",
            ],
            [
                "13:00:00",
                "2022-01-01",
                "000A",
                "000B",
                "2.000",
                "Dirham",
                "Dirham",
                "UK",
                "USA",
                "ACH",
                "0",
                "Normal",
            ],
        ],
    )
    (tmp_path / "currencies.json").write_text(
        '{"confirmed":{"US Dollar":"USD","Euro":"EUR"}}', encoding="utf-8"
    )
    cfg = tmp_path / "check.toml"
    cfg.write_text(
        f'''schema_version="{CONTRACT_VERSION}"
output_root="reports"
currency_map="currencies.json"
[[sources]]
id="ibm_hi_small"
kind="ibm"
path="input/hi.csv"
accounts="input/accounts.csv"
label_semantics="AMLWORLD_ILLICIT_FLOW"
[[sources]]
id="saml_d"
kind="saml"
path="input/saml.csv"
label_semantics="SOURCE_DESIGNATED_SUSPICIOUS"
amount_precision=38
amount_scale=16
''',
        encoding="utf-8",
    )
    validation, ok = check(load_check(cfg))
    assert ok
    warnings = sorted(
        {
            r["rule"]
            for p in validation.glob("*/validation_report.json")
            for r in json.loads(p.read_text())["checks"]
            if r["status"] == "WARN"
        }
    )
    config = tmp_path / "assembly.toml"
    config.write_text(
        f"""schema_version="dajaba-assembly-v2"
dbcheck_config="check.toml"
output_root="outputs"
batch_rows=2
acknowledged_warnings={json.dumps(warnings)}
[service]
enabled=true
source="ibm_hi_small"
start="2022-09-02T00:00:00"
end="2022-09-03T00:00:00"
history_start="2022-09-01T00:00:00"
""",
        encoding="utf-8",
    )
    return load(config), validation


def manifest(root, source):
    run = json.loads((root / "run.json").read_text())
    folder = root / run["sources"][source]["folder"]
    return folder, json.loads((folder / "source_manifest.json").read_text())


def table(folder, name):
    return pa.concat_tables([pq.read_table(p) for p in sorted((folder / name).glob("*.parquet"))])


def test_full_ml_and_hi_excerpt_are_independent_lossless_and_label_separate(fixture, caplog):
    config, validation = fixture
    before = {s.path: sha256(s.path) for s in config.check.sources}
    root, ok = execute(config, validation)
    assert ok and verify_run(root)["status"] == "VERIFIED"
    run = json.loads((root / "run.json").read_text())
    assert run["started_at"].endswith("Z") and run["finished_at"].endswith("Z")
    check_run = json.loads((validation / "run.json").read_text())
    assert check_run["started_at"].endswith("Z") and check_run["finished_at"].endswith("Z")
    hi, info = manifest(root, "ibm_hi_small")
    assert info["input_rows"] == info["ml_output_rows"] == 4
    assert (
        info["service_output_rows"] == 2
        and info["service_target_rows"] == 1
        and info["service_history_rows"] == 1
    )
    assert info["service_outside_scope_rows"] == 2
    service = table(hi, "service/transactions")
    assert "transaction_id" not in service.schema.names
    assert service.column("candidate_transaction_id").to_pylist() == [1, 2]
    assert service.column("source_row_id").to_pylist() == [0, 1]
    assert (
        service.column("source_file_hash").to_pylist() == [before[config.check.sources[0].path]] * 2
    )
    assert info["service_money_compatibility"]["target_type"] == "numeric(20,6)"
    assert "cannot fit numeric(20,6) exactly" in caplog.text
    assert "numeric(18,2)" not in caplog.text
    assert info["service_money_compatibility"]["status"] == "BLOCKED_PRECISION_LOSS"
    assert info["service_money_compatibility"]["incompatible_rows"] == 2
    assert info["service_money_compatibility"]["incompatible_values"] == 4
    assert service.column("amount_paid")[0].as_py() == Decimal("1.123456789012")
    assert not info["direct_copy_ready"]
    assert service.column("from_account_key").to_pylist() == ["1_000A"] * 2
    assert service.column("from_bank_id_raw").to_pylist() == ["001"] * 2
    assert service.column("to_bank_id_raw").to_pylist() == ["2"] * 2
    assert table(hi, "service/transaction_mapping").column("source_row_number").to_pylist() == [
        1,
        2,
    ]
    accounts = table(hi, "service/accounts")
    assert accounts.column("account_key").to_pylist() == ["1_000A", "2_000B"]
    assert accounts.column("bank_id").to_pylist() == ["1", "2"]
    assert not {"source_positive", "source_typology", "score", "prediction"} & set(
        service.schema.names
    )
    assert len(table(hi, "service/entities")) == 2
    assert table(hi, "service/accounts").column("master_bank_id_raw").to_pylist() == ["1", "2"]
    saml, saml_info = manifest(root, "saml_d")
    tx = table(saml, "ml/transactions")
    assert tx.column("amount")[0].as_py() == Decimal("3469.0028040888333")
    assert tx.column("money_operations_allowed").to_pylist() == [False, True]
    assert tx.column("amount_currency_iso").to_pylist() == [None, None]
    assert not (saml / "service").exists() and saml_info["ml_output_rows"] == 2
    assert not set(tx.column("transaction_uid").to_pylist()) & set(
        table(hi, "ml/transactions").column("transaction_uid").to_pylist()
    )
    assert all(sha256(path) == value for path, value in before.items())


def test_changed_input_blocks_only_that_source(fixture):
    config, validation = fixture
    with config.check.sources[0].path.open("a", encoding="utf-8") as f:
        f.write("\n")
    root, ok = execute(config, validation)
    assert not ok
    result = json.loads((root / "run.json").read_text())
    assert result["sources"]["ibm_hi_small"]["status"] == "FAILED"
    assert result["sources"]["saml_d"]["status"] == "VERIFIED"


def test_tampering_output_is_detected_and_resume_checks_binding(fixture):
    config, validation = fixture
    root, ok = execute(config, validation)
    assert ok
    again, ok = execute(config, validation, resume=root)
    assert ok and again == root
    folder, _ = manifest(root, "ibm_hi_small")
    next((folder / "ml/transactions").glob("part-*.json")).write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="modified"):
        verify_run(root)
    with config.path.open("a", encoding="utf-8") as f:
        f.write("\n# policy changed\n")
    with pytest.raises(ValueError, match="resume is forbidden"):
        execute(config, validation, resume=root)


def test_missing_excerpt_and_old_policy_are_rejected(fixture):
    config, validation = fixture
    with pytest.raises(ValueError, match="HI excerpt"):
        execute(replace(config, service=None), validation)
    source = config.check.sources[0]
    report = validation / source.id / "validation_report.json"
    data = json.loads(report.read_text())
    data["money_identity_policy_version"] = "source-preserving-money-identity-v1"
    report.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="old money/identity"):
        bind(source, config, validation)


def test_unacknowledged_warning_is_not_silently_accepted(fixture):
    config, validation = fixture
    with pytest.raises(ValueError, match="acknowledgement"):
        bind(
            config.check.sources[0], replace(config, acknowledged_warnings=frozenset()), validation
        )


def test_null_backslash_large_id_exact_amount_and_empty_roundtrip(tmp_path):
    schema = pa.schema(
        [
            ("text", pa.string()),
            ("amount", pa.decimal128(38, 16)),
            ("id", pa.int64()),
            ("ts", pa.timestamp("us")),
            ("flag", pa.bool_()),
        ]
    )
    rows = [
        dict(
            text=value,
            amount=Decimal("3469.0028040888333"),
            id=9007199254740993 + i,
            ts=datetime(2022, 1, 1),
            flag=True,
        )
        for i, value in enumerate(["", None, "\\N", "\\\\ABC"])
    ]
    writer = DatasetWriter(tmp_path / "typed", schema, dict(purpose="test"))
    writer.write(pa.Table.from_pylist(rows, schema=schema))
    assert writer.finish()["rows"] == 4 and verify_dataset(writer.folder)["rows"] == 4
    empty = DatasetWriter(tmp_path / "empty", schema, dict(purpose="test"))
    assert empty.finish()["rows"] == 0 and verify_dataset(empty.folder)["rows"] == 0


def random_config(config, count=2):
    text = config.path.read_text(encoding="utf-8").split("[service]")[0]
    text += '[service]\nenabled=true\nsource="ibm_hi_small"\nmode="random_rows"\n'
    text += f"count={json.dumps(count)}\nseed=42\n"
    config.path.write_text(text, encoding="utf-8")
    return load(config.path)


def test_random_service_selection_is_unique_repeatable_and_batch_independent(fixture):
    config, validation = fixture
    config = random_config(config)
    root, ok = execute(config, validation, purpose="service")
    assert ok
    first, info = manifest(root, "ibm_hi_small")
    ids = table(first, "service/transaction_mapping").column("source_row_id").to_pylist()
    assert len(ids) == len(set(ids)) == 2
    assert info["service_history_rows"] == 0 and info["service_target_rows"] == 2
    assert info["service_selection"]["label_blind"]
    assert not info["service_selection"]["history_complete"]
    assert not (first / "ml").exists()
    second, ok = execute(replace(config, batch_rows=3), validation, purpose="service")
    assert ok
    other, _ = manifest(second, "ibm_hi_small")
    assert table(first, "service/transactions").equals(table(other, "service/transactions"))
    assert ids == table(other, "service/transaction_mapping").column("source_row_id").to_pylist()
    assert verify_run(second)["status"] == "VERIFIED"


@pytest.mark.parametrize("count", [0, True, 100001])
def test_invalid_random_selection_count_is_rejected(fixture, count):
    config, _ = fixture
    with pytest.raises(ValueError, match="integer count"):
        random_config(config, count)


def test_random_service_cannot_export_the_entire_source(fixture):
    config, validation = fixture
    config = random_config(config, 4)
    root, ok = execute(config, validation, purpose="service")
    assert not ok
    run = json.loads((root / "run.json").read_text())
    assert "smaller than" in run["sources"]["ibm_hi_small"]["error"]


def test_service_precision_audit_cannot_be_marked_compatible_by_editing_manifest(fixture):
    config, validation = fixture
    root, ok = execute(config, validation)
    assert ok
    folder, source = manifest(root, "ibm_hi_small")
    source["service_money_compatibility"]["status"] = "EXACTLY_REPRESENTABLE"
    (folder / "source_manifest.json").write_text(json.dumps(source), encoding="utf-8")
    with pytest.raises(ValueError, match="audit mismatch"):
        verify_run(root)


def test_bank_aliases_share_one_service_account_without_losing_raw_spelling(fixture):
    config, _ = fixture
    path = config.check.sources[0].path
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))[1:]
    rows[1][1] = "1"
    csv_file(path, IBM, rows)
    validation, valid = check(config.check)
    assert valid
    root, ok = execute(config, validation)
    assert ok and verify_run(root)["status"] == "VERIFIED"
    folder, source = manifest(root, "ibm_hi_small")
    assert source["service_contract_version"] == "dajaba-service-candidate-v4"
    tx = table(folder, "service/transactions")
    assert tx.column("from_bank_id_raw").to_pylist() == ["001", "1"]
    assert tx.column("from_account_key").to_pylist() == ["1_000A", "1_000A"]
    assert table(folder, "service/accounts").column("account_key").to_pylist() == [
        "1_000A",
        "2_000B",
    ]


def test_legacy_v3_still_checks_precision_audit_after_contract_upgrade(fixture):
    config, validation = fixture
    root, ok = execute(config, validation)
    assert ok
    folder, source = manifest(root, "ibm_hi_small")
    # 실제 출력을 바꾸지 않고 과거 검증 분기를 확인합니다.
    source["service_contract_version"] = "dajaba-service-candidate-v3"
    source.pop("service_expected")
    source["service_money_compatibility"]["status"] = "EXACTLY_REPRESENTABLE"
    (folder / "source_manifest.json").write_text(json.dumps(source), encoding="utf-8")
    with pytest.raises(ValueError, match="audit mismatch"):
        verify_run(root)


def test_explicit_full_source_streams_all_rows_and_handoff_detects_tampering(fixture):
    config, validation = fixture
    text = config.path.read_text().split("[service]")[0]
    config.path.write_text(text + '[service]\nenabled=true\nmode="full_source"\n')
    config = load(config.path)
    root, ok = execute(config, validation, purpose="service")
    assert ok and verify_run(root)["status"] == "VERIFIED"
    folder, info = manifest(root, "ibm_hi_small")
    assert info["input_rows"] == info["service_output_rows"] == 4
    assert info["service_outside_scope_rows"] == info["service_history_rows"] == 0
    assert info["service_selection"]["backend_history_coverage"] == "UNVERIFIED"
    expected = json.loads((folder / "service/expected.json").read_text())
    assert expected["expected_table_rows"]["transactions"] == 4
    assert expected["expected_table_rows"]["mapping"] == 4
    assert expected["backend_copy_verified"] is False
    assert expected["rule_expectations"] is None
    tx = table(folder, "service/transactions")
    assert tx.column("candidate_transaction_id").to_pylist() == [1, 2, 3, 4]
    assert "transaction_id" not in tx.schema.names
    expected["expected_table_rows"]["transactions"] = 3
    (folder / "service/expected.json").write_text(json.dumps(expected))
    with pytest.raises(ValueError, match="expected.json"):
        verify_run(root)


def test_recorded_auxiliary_disappearance_blocks_binding(fixture):
    config, validation = fixture
    source = config.check.sources[0]
    source.accounts.rename(source.accounts.with_suffix(".backup"))
    with pytest.raises(ValueError, match="Auxiliary input set changed"):
        bind(source, config, validation)


def test_source_expected_tampering_blocks_binding(fixture):
    config, validation = fixture
    source = config.check.sources[0]
    p = validation / source.id / "source_expected.json"
    p.write_text("{}")
    with pytest.raises(ValueError, match="expected-count evidence changed"):
        bind(source, config, validation)


def test_full_source_rejects_mixed_sample_settings(fixture):
    config, _ = fixture
    text = config.path.read_text().split("[service]")[0]
    config.path.write_text(text + '[service]\nenabled=true\nmode="full_source"\ncount=2\n')
    with pytest.raises(ValueError, match="must not mix"):
        load(config.path)


@pytest.mark.parametrize("mode", ["random_rows", "full_source", "time_window"])
def test_service_selection_preserves_exact_scope_metadata(mode):
    selection = ServiceSelection(
        datetime(2022, 9, 2),
        datetime(2022, 9, 3),
        datetime(2022, 9, 1),
        mode,
        2 if mode == "random_rows" else None,
        42,
    )

    chosen, metadata = pipeline._service_selection(selection, 4)

    if mode == "random_rows":
        assert chosen == {0, 3}
        assert metadata == {
            "mode": "random_rows",
            "count": 2,
            "seed": 42,
            "replacement": False,
            "label_blind": True,
            "history_complete": False,
            "purpose": "BACKEND_INTEGRATION_SAMPLE",
            "sampled_row_ids_sha256": hashlib.sha256(b"[0,3]").hexdigest(),
        }
    elif mode == "full_source":
        assert chosen is None
        assert metadata == {
            "mode": "full_source",
            "purpose": "FULL_HI_SMALL_SERVICE_CANDIDATE",
            "source_rows_complete": True,
            "backend_history_coverage": "UNVERIFIED",
        }
    else:
        assert chosen is None
        assert metadata == {
            "mode": "time_window",
            "start": "2022-09-02T00:00:00",
            "end": "2022-09-03T00:00:00",
            "history_start": "2022-09-01T00:00:00",
            "interval": "[history_start,end); TARGET >= start",
        }


def _dimension_master():
    return {
        ("2", "000C"): {
            "bank_raw": "002",
            "entity_id_raw": "E1",
            "entity_name_raw": "",
            "account_key": "internal-c",
            "bank_name_raw": "Bank C",
        },
        ("1", "000B"): {
            "bank_raw": "01",
            "entity_id_raw": "E2",
            "entity_name_raw": "\\N",
            "account_key": "internal-b",
            "bank_name_raw": "Bank B",
        },
        ("1", "000A"): {
            "bank_raw": "001",
            "entity_id_raw": "E2",
            "entity_name_raw": "\\N",
            "account_key": "internal-a",
            "bank_name_raw": "Bank A",
        },
    }


def test_service_dimensions_preserve_order_aliases_and_partition_boundaries(tmp_path):
    referenced = {("2", "000C"), ("01", "000B"), ("001", "000A"), ("1", "000A")}
    writers, dimensions = pipeline._write_service_dimensions(
        tmp_path, 1, {"purpose": "service"}, referenced, _dimension_master()
    )

    assert list(writers) == ["service_accounts", "service_entities"]
    assert dimensions == {
        "accounts": 3,
        "entities": 2,
        "references_complete": True,
        "synthetic_customer_attributes_created": False,
        "bank_alias_resolution": "normalized service bank ID; original bank IDs in transactions "
        "and master_bank_id_raw; internal ML keys unchanged",
    }
    for name, writer in writers.items():
        expected_rows = 3 if name == "service_accounts" else 2
        assert writer.manifest["status"] == "RUNNING"
        assert [part["rows"] for part in writer.manifest["parts"]] == [1] * expected_rows
        assert writer.finish()["rows"] == expected_rows
        assert verify_dataset(writer.folder)["rows"] == expected_rows
    assert table(tmp_path, "service/accounts").to_pylist() == [
        {
            "account_key": "1_000A",
            "bank_id": "1",
            "account_no": "000A",
            "entity_id": "E2",
            "master_bank_id_raw": "001",
            "internal_account_key": "internal-a",
            "bank_name": "Bank A",
        },
        {
            "account_key": "1_000B",
            "bank_id": "1",
            "account_no": "000B",
            "entity_id": "E2",
            "master_bank_id_raw": "01",
            "internal_account_key": "internal-b",
            "bank_name": "Bank B",
        },
        {
            "account_key": "2_000C",
            "bank_id": "2",
            "account_no": "000C",
            "entity_id": "E1",
            "master_bank_id_raw": "002",
            "internal_account_key": "internal-c",
            "bank_name": "Bank C",
        },
    ]
    assert table(tmp_path, "service/entities").to_pylist() == [
        {"entity_id": "E1", "entity_name": ""},
        {"entity_id": "E2", "entity_name": "\\N"},
    ]


@pytest.mark.parametrize(
    ("problem", "message"),
    [
        ("missing_owner", "Selected service transaction has no original account owner"),
        ("entity_name", "Original entity has inconsistent names"),
    ],
)
def test_service_dimensions_validate_all_references_before_creating_writers(
    tmp_path, problem, message
):
    master = _dimension_master()
    referenced = {("1", "000A"), ("1", "000B")}
    if problem == "missing_owner":
        referenced.add(("9", "missing"))
    else:
        master[("1", "000B")]["entity_name_raw"] = "conflicting name"

    with pytest.raises(ValueError, match=message):
        pipeline._write_service_dimensions(tmp_path, 1, {"purpose": "service"}, referenced, master)

    assert not (tmp_path / "service").exists()


def test_service_dimension_write_failure_preserves_prior_parts(tmp_path, monkeypatch):
    write = DatasetWriter.write

    def fail_entities(writer, output):
        if writer.folder.name == "entities":
            raise OSError("injected entity write failure")
        return write(writer, output)

    monkeypatch.setattr(DatasetWriter, "write", fail_entities)
    with pytest.raises(OSError, match="injected entity write failure"):
        pipeline._write_service_dimensions(
            tmp_path,
            1,
            {"purpose": "service"},
            {("1", "000A"), ("2", "000C")},
            _dimension_master(),
        )

    accounts = tmp_path / "service" / "accounts"
    account_manifest = json.loads((accounts / "manifest.json").read_text())
    assert account_manifest["status"] == "RUNNING"
    assert account_manifest["rows"] == 2
    assert len(list(accounts.glob("part-*.parquet"))) == 2
    assert (tmp_path / "service" / "entities" / "schema.arrow").is_file()
    assert not (tmp_path / "service" / "entities" / "manifest.json").exists()
    assert not (tmp_path / "source_manifest.json").exists()
