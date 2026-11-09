"""실제 소형 원천으로 정형화 텍스트→ML Parquet과 HI 서비스 v4 경계를 확인합니다."""

import csv
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from dbcheck.config import CONTRACT_VERSION
from dbcheck.config import load as load_check
from dbcheck.distribution import IBM, SAML, sha256
from dbcheck.pipeline import execute as check

from assemble_db.audit import audit_run
from assemble_db.config import load
from assemble_db.exporting import DatasetWriter, reload_file, verify_dataset
from assemble_db.pipeline import execute
from assemble_db.staged import audit_staged, execute_staged, verify_staged
from assemble_db.staged_contract import Progress, RunLock, archived_config, capture_text
from assemble_db.staged_dimensions import Dimensions


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)


@pytest.fixture
def inputs(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    for short in ("hi", "li"):
        write_csv(
            raw / (short + ".csv"),
            IBM,
            [
                [
                    "2022/09/01 00:00",
                    "001",
                    "000A",
                    "2",
                    "000B",
                    "1.20",
                    "US Dollar",
                    "1.2300",
                    "US Dollar",
                    "Wire",
                    "1",
                ],
                [
                    "2022/09/01 00:01",
                    "2",
                    "000B",
                    "001",
                    "000A",
                    "0.00",
                    "Bitcoin",
                    "0.00",
                    "Bitcoin",
                    "Wire",
                    "0",
                ],
            ],
        )
        write_csv(
            raw / (short + "_accounts.csv"),
            ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"],
            [["A", "1", "000A", "01", ""], ["B", "2", "000B", "02", "\\N"]],
        )
    write_csv(
        raw / "saml.csv",
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
                "000B",
                "000A",
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
    (tmp_path / "aliases.json").write_text(
        '{"confirmed":{"US Dollar":"USD","Euro":"EUR","Bitcoin":"CRYPTO:BTC"}}', encoding="utf-8"
    )
    check_path = tmp_path / "check.toml"
    check_path.write_text(
        f'''schema_version="{CONTRACT_VERSION}"
output_root="reports"
currency_map="aliases.json"
[[sources]]
id="ibm_hi_small"
kind="ibm"
path="raw/hi.csv"
accounts="raw/hi_accounts.csv"
label_semantics="AMLWORLD_ILLICIT_FLOW"
[[sources]]
id="ibm_li_small"
kind="ibm"
path="raw/li.csv"
accounts="raw/li_accounts.csv"
label_semantics="AMLWORLD_ILLICIT_FLOW"
[[sources]]
id="saml_d"
kind="saml"
path="raw/saml.csv"
label_semantics="SOURCE_DESIGNATED_SUSPICIOUS"
amount_precision=38
amount_scale=16
''',
        encoding="utf-8",
    )
    validation, success = check(load_check(check_path))
    assert success
    warnings = sorted(
        {
            r["rule"]
            for p in validation.glob("*/validation_report.json")
            for r in read(p)["checks"]
            if r["status"] == "WARN"
        }
    )
    assembly = tmp_path / "assembly.toml"
    assembly.write_text(
        f"""schema_version="dajaba-assembly-v2"
dbcheck_config="check.toml"
output_root="old_outputs"
batch_rows=1
acknowledged_warnings={json.dumps(warnings)}
[service]
enabled=true
source="ibm_hi_small"
mode="full_source"
""",
        encoding="utf-8",
    )
    config = load(assembly)
    old, success = execute(config, validation)
    assert success
    proof_folder = tmp_path / "proof"
    proof = audit_run(config, old, proof_folder)
    assert proof["status"] == "PASS"
    execution = tmp_path / "proof_execution.json"
    execution.write_text(
        json.dumps(
            dict(
                exit_code=0,
                report=str(proof_folder / "audit.json"),
                report_sha256=sha256(proof_folder / "audit.json"),
            )
        ),
        encoding="utf-8",
    )
    return config, validation, old, proof_folder / "audit.json", execution


@pytest.mark.parametrize("fmt", ["default", "explicit"])
def test_committed_text_then_parquet_three_sources_and_hi_service_only(inputs, tmp_path, fmt):
    config, validation, old, proof, execution = inputs
    old_hashes = {p: sha256(p) for p in old.rglob("*") if p.is_file()}
    raw_hashes = {s.path: sha256(s.path) for s in config.check.sources}
    root, success = execute_staged(
        config,
        validation,
        old,
        proof,
        execution,
        output_root=tmp_path / "new_outputs",
        canonical_formats=None
        if fmt == "default"
        else [sid + "=csv+json" for sid in ("ibm_hi_small", "ibm_li_small", "saml_d")],
    )
    assert success and verify_staged(root)["status"] == "VERIFIED"
    run = read(root / "run.json")
    assert run["status"] == "COMPLETED"
    assert not run["training_executed"] and not run["source_copy_executed"]
    assert read(root / "assembly_index.json")["run_manifest_sha256"] == {
        sid: sha256(root / "run.json") for sid in run["sources"]
    }
    for sid, item in run["sources"].items():
        folder = root / item["folder"]
        info = read(folder / "source_manifest.json")
        assert info["input_rows"] == info["ml_output_rows"] == 2
        references = read(folder / "canonical_inputs.json")
        assert references["selected_format"] == "csv+json"
        for role in ("transactions", "labels"):
            source = references["tables"][role]
            source_manifest = read(Path(source["folder"]) / "manifest.json")
            target = read(folder / "ml" / role / "manifest.json")
            assert target["formats"] == ["parquet"]
            assert [p["logical_sha256"] for p in target["parts"]] == [
                p["logical_sha256"] for p in source_manifest["parts"]
            ]
            assert source_manifest["formats"] == ["csv", "json"]
            assert all(p["canonical_source"]["format"] == "csv" for p in target["parts"])
        if sid != "ibm_hi_small":
            assert info["service_output_rows"] is None and info["service_contract_version"] is None
            assert not (folder / "service").exists()
            continue
        service = read(folder / "service/transactions/manifest.json")
        assert service["formats"] == ["csv", "json"]
        assert not list((folder / "service").rglob("*.parquet"))
        rows = []
        for part in service["parts"]:
            file = next(f for f in part["files"] if f["path"].endswith(".json"))
            rows.extend(read(folder / "service/transactions" / file["path"]))
        assert rows[0]["amount_paid"] == "1.2300" and rows[0]["amount_received"] == "1.20"
        assert rows[0]["from_account_key"] == "1_000A" and rows[0]["from_bank_id_raw"] == "001"
        assert info["service_money_compatibility"]["incompatible_values"] == 0
        assert info["dimensions"]["accounts"] == 2 and info["dimensions"]["references_complete"]
    assert audit_staged(config, root, tmp_path / "new_audit", ["ibm_hi_small"])["status"] == "PASS"
    assert old_hashes == {p: sha256(p) for p in old.rglob("*") if p.is_file()}
    assert raw_hashes == {s.path: sha256(s.path) for s in config.check.sources}
    same, success = execute_staged(
        config,
        validation,
        old,
        proof,
        execution,
        output_root=tmp_path / "new_outputs",
        canonical_formats=None
        if fmt == "default"
        else [sid + "=csv+json" for sid in run["sources"]],
        resume=root,
    )
    assert same == root and success


def test_snapshot_restores_actual_bytes_without_changing_resolved_paths(inputs, tmp_path):
    config, _, _, _, _ = inputs
    snapshot = tmp_path / "genuine.toml"
    snapshot.write_bytes(config.check.path.read_bytes())
    config.check.path.write_text(
        config.check.path.read_text(encoding="utf-8") + "\n# 설명 변경\n", encoding="utf-8"
    )
    current = load(config.path)
    root = tmp_path / "restoration"
    restored, evidence = archived_config(current, snapshot, root)
    assert restored.check.sources == current.check.sources
    assert restored.check.config_sha256 == sha256(snapshot)
    assert restored.check.path.read_bytes() == snapshot.read_bytes()
    assert evidence["semantic_and_type_equality_checked"]
    config.check.path.write_text(
        config.check.path.read_text(encoding="utf-8") + "\n# 다시 변경\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="Loaded DBcheck configuration changed"):
        archived_config(current, snapshot, tmp_path / "reject_stale_load")
    config.check.path.write_text(
        snapshot.read_text(encoding="utf-8").replace("amount_scale=16", "amount_scale=12"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="differs semantically"):
        archived_config(load(config.path), snapshot, tmp_path / "reject_semantic_change")


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_selected_text_preserves_null_empty_backslash_big_id_and_decimal_spelling(tmp_path, fmt):
    schema = pa.schema(
        [("id", pa.int64()), ("text", pa.string()), ("amount", pa.decimal128(38, 12))]
    )
    table = pa.Table.from_pylist(
        [
            dict(id=9223372036854775806, text="", amount=Decimal("1.200")),
            dict(id=2, text=None, amount=None),
            dict(id=3, text="\\N", amount=Decimal("0.00")),
            dict(id=4, text="\\abc", amount=Decimal("2")),
        ],
        schema=schema,
    )
    folder = tmp_path / fmt
    writer = DatasetWriter(folder, schema, {}, formats=(fmt,))
    writer.write(table, decimal_raw={"amount": ["1.200", None, "0.00", "2"]})
    writer.finish()
    assert verify_dataset(folder)["rows"] == 4
    assert reload_file(folder / ("part-000000." + fmt), schema).equals(table)
    with Progress(tmp_path) as progress:
        captured = capture_text(folder, schema, {}, fmt, progress, "capture")
        assert captured.read(0).equals(table)
        file = folder / ("part-000000." + fmt)
        original = file.read_text(encoding="utf-8")
        file.write_text(original.replace("1.200", "1.20"), encoding="utf-8")
        # 숫자 값은 같아도 원문 표기가 달라지면 거부합니다.
        captured.manifest["parts"][0]["files"][0]["bytes"] = file.stat().st_size
        captured.manifest["parts"][0]["files"][0]["sha256"] = sha256(file)
        with pytest.raises(ValueError, match="decimal spelling"):
            captured.read(0)


def test_decimal_spelling_contract_cannot_start_after_first_batch(tmp_path):
    schema = pa.schema([("amount", pa.decimal128(20, 6))])
    table = pa.Table.from_pylist([{"amount": Decimal("1.20")}], schema=schema)
    writer = DatasetWriter(tmp_path / "late", schema, {}, formats=("json",))
    writer.write(table)
    with pytest.raises(ValueError, match="cannot start after"):
        writer.write(table, decimal_raw={"amount": ["1.20"]})
    assert len(list((tmp_path / "late").glob("*.json"))) == 2


def test_heartbeat_failure_records_failed_and_preserves_original_cause(tmp_path):
    error = OSError("heartbeat fixture")
    with pytest.raises(RuntimeError, match="Heartbeat publication failed") as caught:
        with Progress(tmp_path) as progress:
            progress.failure = error
    assert caught.value.__cause__ is error
    assert read(tmp_path / "progress.json")["status"] == "FAILED"


def test_concurrent_run_lock_is_rejected(tmp_path):
    with RunLock(tmp_path):
        with pytest.raises(ValueError, match="Another staged invocation"):
            with RunLock(tmp_path):
                pass


def test_dimensions_closes_connection_if_initialization_fails(tmp_path, monkeypatch):
    class Broken:
        closed = False

        def execute(self, command):
            raise OSError("initialization fixture")

        def close(self):
            self.closed = True

    connection = Broken()
    monkeypatch.setattr("assemble_db.staged_dimensions.sqlite3.connect", lambda _: connection)
    with pytest.raises(OSError, match="initialization fixture"):
        Dimensions(tmp_path / "dimensions")
    assert connection.closed


def test_failed_adoption_proof_is_not_accepted(inputs, tmp_path):
    config, validation, old, proof, execution = inputs
    record = read(execution)
    record["exit_code"] = 2
    execution.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError, match="exit zero"):
        execute_staged(config, validation, old, proof, execution, output_root=tmp_path / "reject")


def test_changed_canonical_bytes_fail_before_decoding(tmp_path):
    schema = pa.schema([("text", pa.string())])
    writer = DatasetWriter(tmp_path / "input", schema, {}, formats=("csv",))
    writer.write(pa.Table.from_pylist([{"text": "a"}], schema=schema))
    writer.finish()
    with Progress(tmp_path) as progress:
        captured = capture_text(tmp_path / "input", schema, {}, "csv", progress, "capture")
        file = captured.path(0)
        file.write_bytes(file.read_bytes() + b"unexpected growth")
        with pytest.raises(ValueError, match="before bounded decoding"):
            captured.read(0)


def test_legacy_resume_and_verify_are_read_only_rejections(inputs, tmp_path):
    config, validation, old, proof, execution = inputs
    before = {p.relative_to(old): sha256(p) for p in old.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="Resume must target"):
        execute_staged(
            config, validation, old, proof, execution, output_root=old.parent, resume=old
        )
    with pytest.raises(ValueError, match="Not a completed staged"):
        verify_staged(old)
    assert before == {p.relative_to(old): sha256(p) for p in old.rglob("*") if p.is_file()}


def test_self_consistent_modified_canonical_manifest_is_rejected(inputs, tmp_path):
    config, validation, old, proof, execution = inputs
    run = read(old / "run.json")
    folder = old / run["sources"]["saml_d"]["folder"]
    manifest = folder / "ml/transactions/manifest.json"
    content = read(manifest)
    content["extra"] = "changed after audit"
    manifest.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(ValueError, match="metadata differs"):
        execute_staged(
            config,
            validation,
            old,
            proof,
            execution,
            selected=["saml_d"],
            output_root=tmp_path / "reject_modified",
        )
