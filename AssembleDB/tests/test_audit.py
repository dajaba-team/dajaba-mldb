"""생성한 작은 fixture만 사용하며 운영 데이터는 감사하지 않습니다."""

import csv
import json
import traceback
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from dbcheck.config import Source
from dbcheck.distribution import IBM, SAML, sha256

from assemble_db import audit, file_commit
from assemble_db.exporting import DatasetWriter
from assemble_db.normalize import normalize, service_tables


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def build(tmp_path, monkeypatch):
    def make(kind="ibm", version="v4", mutate=None, rows=None, part_rows=None):
        base = tmp_path / kind
        base.mkdir()
        raw_path = base / "raw" / "transactions.csv"
        raw_path.parent.mkdir()
        if rows is None:
            rows = (
                [
                    [
                        "2022/09/01 00:00",
                        "001",
                        "000A",
                        "02",
                        "000B",
                        "1.000001",
                        "US Dollar",
                        "1.000001",
                        "US Dollar",
                        "Wire",
                        "1",
                    ],
                    [
                        "2022/09/01 00:00",
                        "001",
                        "000A",
                        "02",
                        "000B",
                        "0.0000001",
                        "Euro",
                        "0.0000001",
                        "Euro",
                        "ACH",
                        "0",
                    ],
                ]
                if kind == "ibm"
                else [
                    [
                        "12:00:00",
                        "2022-01-01",
                        "000A",
                        "000B",
                        "1.1234567890123456",
                        "US Dollar",
                        "Euro",
                        "UK",
                        "USA",
                        "ACH",
                        "1",
                        "Layering",
                    ],
                    [
                        "12:00:00",
                        "2022-01-01",
                        "000A",
                        "000B",
                        "2.0000000000000000",
                        "Dirham",
                        "Dirham",
                        "UK",
                        "USA",
                        "ACH",
                        "0",
                        "Normal",
                    ],
                ]
            )
        header = IBM if kind == "ibm" else SAML
        with raw_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        source = Source(
            "ibm_hi_small" if kind == "ibm" else "saml_d",
            kind,
            raw_path,
            "fixture",
            amount_scale=12 if kind == "ibm" else 16,
        )
        digest = sha256(raw_path)
        config_path = base / "config.toml"
        config_path.write_text("fixture-config", encoding="utf-8")
        validation = base / "validation"
        write_json(validation / "run.json", {"fixture": True})
        validation_report = validation / source.id / "validation_report.json"
        write_json(validation_report, {"input": {"rows": len(rows)}})
        aliases = {"US Dollar": "USD", "Euro": "EUR"}
        config = SimpleNamespace(
            path=config_path,
            output=base / "outputs",
            check=SimpleNamespace(
                output=validation,
                sources=(source,),
                currency_aliases=aliases,
                config_sha256="check",
                aliases_sha256="aliases",
            ),
        )
        root = config.output / "run"
        folder = root / source.id / "attempt-01"
        metadata: dict[str, str | int | None | dict[str, str]] = dict(
            source_id=source.id,
            source_kind=kind,
            source_sha256=digest,
            raw_input_rows=len(rows),
            config_sha256=sha256(config_path),
            dbcheck_config_sha256="check",
            aliases_sha256="aliases",
            validation_report_sha256=sha256(validation_report),
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([r[i] for r in rows], type=pa.string()) for i in range(len(header))],
            names=[f"c{i}" for i in range(len(header))],
        )
        tx, labels = normalize(source, digest, 0, batch, aliases)
        tables = {"ml_transactions": tx, "ml_labels": labels}
        selection = {}
        service_version = None
        if kind == "ibm":
            service, mapping, _ = service_tables(source, tx, datetime.min)
            service_version = "dajaba-service-candidate-" + version
            selection = {"mode": "full_source"}
            if version == "v3":
                data = service.to_pydict()
                data["from_account_key"] = [f"{r[1]}_{r[2]}" for r in rows]
                data["to_account_key"] = [f"{r[3]}_{r[4]}" for r in rows]
                for field in ("from_bank_id_raw", "to_bank_id_raw"):
                    data.pop(field)
                service = pa.Table.from_pydict(
                    data,
                    schema=pa.schema(
                        [
                            f
                            for f in service.schema
                            if f.name not in {"from_bank_id_raw", "to_bank_id_raw"}
                        ]
                    ),
                )
                mapping = mapping.drop(["source_row_number"])
            tables.update(service_transactions=service, service_mapping=mapping)
        if mutate:
            mutate(tables)
        datasets = {}
        for name, table in tables.items():
            relative = Path("ml" if name.startswith("ml_") else "service") / (
                "transaction_mapping" if name == "service_mapping" else name.split("_", 1)[1]
            )
            details = dict(metadata)
            if name.startswith("service_"):
                details.update(
                    service_contract_version=service_version,
                    selection=selection,
                    id_namespace="OFFLINE_CANDIDATE_NOT_BACKEND_OPS",
                )
            writer = DatasetWriter(folder / relative, table.schema, details)
            # 전체 순서를 검사해야 하므로 의도적으로 각 행을 별도의 part로 나눕니다.
            size = (part_rows or {}).get(name, 1)
            for i in range(0, len(table), size):
                writer.write(table.slice(i, size))
            writer.finish()
            datasets[name] = dict(path=str(relative), rows=len(table))
        fingerprints = {str(raw_path): digest}
        source_manifest = dict(
            status="VERIFIED",
            source_id=source.id,
            input_fingerprints=fingerprints,
            input_rows=len(rows),
            datasets=datasets,
            service_contract_version=service_version,
            service_selection=selection,
        )
        write_json(folder / "source_manifest.json", source_manifest)
        run = dict(
            status="COMPLETED",
            contract_version=audit.VERSION,
            binding=dict(
                config_sha256=sha256(config_path),
                validation_run=str(validation),
                validation_run_sha256=sha256(validation / "run.json"),
                selected_sources=[source.id],
            ),
            sources={
                source.id: dict(
                    status="VERIFIED",
                    folder=str(folder.relative_to(root)),
                    datasets=datasets,
                    input_rows=len(rows),
                )
            },
        )
        write_json(root / "run.json", run)
        monkeypatch.setattr(
            audit, "bind", lambda *_: ({"input": {"rows": len(rows)}}, fingerprints)
        )
        return config, root, base / "audit", folder

    return make


@pytest.mark.parametrize("version", ["v3", "v4"])
def test_full_ordered_audit_separates_fidelity_money_and_version(build, version):
    config, root, output, folder = build(version=version)
    dataset = folder / "ml" / "transactions"
    manifest = json.loads((dataset / "manifest.json").read_text(encoding="utf-8"))
    assert all(
        item["path"] != "manifest.json" for part in manifest["parts"] for item in part["files"]
    )
    metadata_hashes = {name: sha256(dataset / name) for name in ("manifest.json", "schema.arrow")}
    result = audit.audit_run(config, root, output)
    source = result["sources"]["ibm_hi_small"]
    assert result["status"] == "PASS"
    assert source["fidelity"] == "PASS"
    assert source["verified_rows"] == 2
    assert "numeric_20_6" in source
    assert source["numeric_20_6"]["incompatible_values"] == 2
    assert "service_v4" in source
    assert source["service_v4"] == ("PRESENT" if version == "v4" else "NOT_PRESENT")
    assert "direct_copy_ready" in source
    assert source["direct_copy_ready"] is False
    assert {name: sha256(dataset / name) for name in metadata_hashes} == metadata_hashes


@pytest.mark.parametrize(
    "name", ["unlisted.csv", "unlisted.json", "unlisted.parquet", "manifest.extra.json"]
)
def test_unlisted_data_file_is_rejected_even_with_metadata_prefix(build, name):
    config, root, output, folder = build()
    extra = folder / "ml" / "transactions" / name
    extra.write_bytes(b"unexpected fixture file")
    with pytest.raises(ValueError, match="Unlisted or missing dataset data file"):
        audit.audit_run(config, root, output)
    report = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["sources"]["ibm_hi_small"]["verified_rows"] == 0
    assert extra.read_bytes() == b"unexpected fixture file"


@pytest.mark.parametrize("suffix", [".csv", ".json", ".parquet"])
def test_missing_declared_data_file_is_rejected_without_changing_manifest(build, suffix):
    config, root, output, folder = build()
    dataset = folder / "ml" / "transactions"
    manifest_path = dataset / "manifest.json"
    manifest_hash = sha256(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    item = next(item for item in manifest["parts"][0]["files"] if item["path"].endswith(suffix))
    missing = dataset / item["path"]
    # 이 테스트가 만든 합성 part만 제거해 누락 검사를 확인합니다.
    missing.unlink()
    with pytest.raises(FileNotFoundError):
        audit.audit_run(config, root, output)
    report = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["sources"]["ibm_hi_small"]["verified_rows"] == 0
    assert sha256(manifest_path) == manifest_hash and not missing.exists()


@pytest.mark.parametrize("name", ["manifest.json", "schema.arrow"])
def test_metadata_changed_after_inventory_is_still_rejected(build, monkeypatch, name):
    config, root, output, folder = build()
    metadata = folder / "ml" / "transactions" / name
    original = audit.expected

    def change_after_codec(source, fingerprint, ordinal, raw, aliases):
        if ordinal == 0:
            # 초기 경로·codec 검사 뒤의 메타데이터 변경도 종료 시 해시로 잡아야 합니다.
            with metadata.open("ab") as stream:
                stream.write(b" ")
        return original(source, fingerprint, ordinal, raw, aliases)

    monkeypatch.setattr(audit, "expected", change_after_codec)
    with pytest.raises(ValueError, match="Artifact metadata changed during audit"):
        audit.audit_run(config, root, output)
    report = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["sources"]["ibm_hi_small"]["fidelity"] != "PASS"


def test_saml_cross_currency_and_native_currency_are_preserved(build):
    config, root, output, _ = build(kind="saml")
    result = audit.audit_run(config, root, output)
    assert result["sources"]["saml_d"]["fidelity"] == "PASS"
    state = result["sources"]["saml_d"]
    assert "account_values" in state
    assert state["account_values"].startswith("NOT_AUDITED")


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount_paid_raw", "01.000001"),
        ("currency_paid_raw", "US Dollar"),
        ("currency_paid_iso", "USD"),
        ("ts", datetime(2022, 9, 1, 0, 1)),
    ],
)
def test_independent_raw_comparison_catches_self_consistent_codec_corruption(build, field, value):
    def mutate(tables):
        table = tables["ml_transactions"]
        data = table.to_pylist()
        assert data[1][field] != value, "Corruption fixture must change the original value"
        data[1][field] = value
        tables["ml_transactions"] = pa.Table.from_pylist(data, schema=table.schema)

    config, root, output, _ = build(mutate=mutate)
    with pytest.raises(ValueError, match="Value/type mismatch"):
        audit.audit_run(config, root, output)
    result = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAILED"
    assert result["sources"]["ibm_hi_small"]["verified_rows"] == 1


@pytest.mark.parametrize("mode", ["duplicate", "missing", "extra", "swap_labels", "mapping"])
def test_cross_part_duplicate_missing_extra_or_join_failure(build, mode):
    def mutate(tables):
        key = (
            "ml_labels"
            if mode == "swap_labels"
            else ("service_mapping" if mode == "mapping" else "ml_transactions")
        )
        table = tables[key]
        if mode == "missing":
            tables[key] = table.slice(0, 1)
        elif mode == "extra":
            tables[key] = pa.concat_tables([table, table.slice(0, 1)])
        else:
            indices = [1, 0] if mode == "swap_labels" else [0, 0]
            tables[key] = table.take(pa.array(indices))

    config, root, output, _ = build(mutate=mutate)
    with pytest.raises(ValueError):
        audit.audit_run(config, root, output)


def test_repeated_manifest_part_rejected_without_global_uid_set(build):
    config, root, output, folder = build()
    path = folder / "ml" / "transactions" / "manifest.json"
    m = json.loads(path.read_text(encoding="utf-8"))
    m["parts"][1] = m["parts"][0]
    write_json(path, m)
    with pytest.raises(ValueError, match="Repeated manifest file"):
        audit.audit_run(config, root, output)


def test_declared_uint64_schema_is_not_trusted(build):
    def mutate(tables):
        table = tables["ml_transactions"]
        schema = pa.schema(
            [
                pa.field(f.name, pa.uint64()) if f.name == "source_row_id" else f
                for f in table.schema
            ]
        )
        tables["ml_transactions"] = table.cast(schema)

    config, root, output, _ = build(mutate=mutate)
    with pytest.raises(ValueError, match="Canonical role schema"):
        audit.audit_run(config, root, output)


@pytest.mark.parametrize("destination", ["artifact", "raw", "existing"])
def test_report_never_writes_into_inputs_or_existing_directory(build, destination):
    config, root, output, _ = build()
    if destination == "artifact":
        output = root / "new-audit"
    elif destination == "raw":
        output = config.check.sources[0].path.parent / "new-audit"
    else:
        output.mkdir()
    with pytest.raises(ValueError):
        audit.audit_run(config, root, output)
    assert not (output / "audit.json").exists()


def test_strict_scalar_types_and_large_ordinals():
    with pytest.raises(ValueError):
        audit.match({"x": False}, {"x": 0}, "typed")
    with pytest.raises(ValueError):
        audit.integer(1 << 63, 0, audit.INT64_MAX - 1, "ordinal")
    with pytest.raises(ValueError):
        audit.amount("1.0000001", 20, 6)


def test_independent_part_and_batch_boundaries_preserve_duplicate_content(build):
    row = [
        "2022/09/01 00:00",
        "001",
        "000A",
        "02",
        "000B",
        "1",
        "US Dollar",
        "1",
        "US Dollar",
        "Wire",
        "0",
    ]
    config, root, output, _ = build(
        rows=[list(row) for _ in range(600)],
        part_rows={
            "ml_transactions": 300,
            "ml_labels": 137,
            "service_transactions": 200,
            "service_mapping": 257,
        },
    )
    result = audit.audit_run(config, root, output)
    assert result["sources"]["ibm_hi_small"]["verified_rows"] == 600
    assert result["status"] == "PASS"


def test_late_mapping_corruption_after_multiple_batches_is_not_skipped(build):
    row = [
        "2022/09/01 00:00",
        "001",
        "000A",
        "02",
        "000B",
        "1",
        "US Dollar",
        "1",
        "US Dollar",
        "Wire",
        "0",
    ]

    def mutate(tables):
        table = tables["service_mapping"]
        data = table.to_pylist()
        data[513]["source_row_number"] = 513
        tables["service_mapping"] = pa.Table.from_pylist(data, schema=table.schema)

    config, root, output, _ = build(
        rows=[list(row) for _ in range(600)],
        mutate=mutate,
        part_rows={
            name: 300
            for name in ("ml_transactions", "ml_labels", "service_transactions", "service_mapping")
        },
    )
    with pytest.raises(ValueError, match="source_row_number"):
        audit.audit_run(config, root, output)
    result = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert result["sources"]["ibm_hi_small"]["verified_rows"] == 513


def test_config_fingerprint_change_is_rejected_before_creating_report(build):
    config, root, output, _ = build()
    config.path.write_text("changed-config", encoding="utf-8")
    with pytest.raises(ValueError, match="Assembly config changed"):
        audit.audit_run(config, root, output)
    assert not output.exists()


def test_post_audit_original_fingerprint_change_is_detected(build, monkeypatch):
    config, root, output, _ = build()
    original_finish = audit.Rows.finish
    changed = False

    def finish(stream):
        nonlocal changed
        original_finish(stream)
        if not changed:
            # 파싱 값은 그대로 두고 바이트만 바꿉니다. 행 비교만으로는 잡을 수 없는 변경입니다.
            source = config.check.sources[0].path
            source.write_bytes(source.read_bytes().replace(b"\r\n", b"\n"))
            changed = True

    monkeypatch.setattr(audit.Rows, "finish", finish)
    with pytest.raises(ValueError, match="Original changed during audit"):
        audit.audit_run(config, root, output)
    result = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAILED"
    assert result["sources"]["ibm_hi_small"]["fidelity"] != "PASS"


@pytest.mark.parametrize(
    "relative", ["nested/part-000000.parquet", "../outside.parquet", "absolute"]
)
def test_bad_part_path_rejected_before_any_parquet_metadata_read(build, monkeypatch, relative):
    config, root, output, folder = build()
    manifest_path = folder / "ml" / "transactions" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    item = next(f for f in manifest["parts"][0]["files"] if f["path"].endswith(".parquet"))
    item["path"] = str(root.parent / "outside.parquet") if relative == "absolute" else relative
    write_json(manifest_path, manifest)

    def forbidden(*args, **kwargs):
        pytest.fail("Parquet metadata read happened before rejecting the invalid part path")

    monkeypatch.setattr(audit.pq, "ParquetFile", forbidden)
    with pytest.raises(ValueError, match="direct child"):
        audit.audit_run(config, root, output)
    assert json.loads((output / "audit.json").read_text(encoding="utf-8"))["status"] == "FAILED"


def test_source_parent_escape_rejected_before_read(build, monkeypatch):
    config, root, output, _ = build()
    path = root / "run.json"
    run = json.loads(path.read_text(encoding="utf-8"))
    run["sources"]["ibm_hi_small"]["folder"] = "../outside"
    write_json(path, run)
    original = audit.read_json

    def guarded(path):
        assert Path(path).resolve().is_relative_to(root.resolve()), "Escaped source metadata opened"
        return original(path)

    monkeypatch.setattr(audit, "read_json", guarded)
    with pytest.raises(ValueError, match="parent traversal"):
        audit.audit_run(config, root, output)


@pytest.mark.parametrize("role", ["run", "source", "manifest", "schema"])
def test_metadata_symlink_escape_rejected_before_content_read(build, monkeypatch, role):
    import errno

    config, root, output, folder = build()
    paths = {
        "run": root / "run.json",
        "source": folder / "source_manifest.json",
        "manifest": folder / "ml" / "transactions" / "manifest.json",
        "schema": folder / "ml" / "transactions" / "schema.arrow",
    }
    path = paths[role]
    outside = root.parent / ("outside-" + path.name)
    outside.write_bytes(path.read_bytes())
    path.unlink()
    try:
        path.symlink_to(outside)
    except NotImplementedError as exc:
        pytest.skip(f"Real symlink creation is not implemented on this platform: {exc}")
    except OSError as exc:
        # Windows에서는 개발자 모드나 심볼릭 링크 권한이 필요할 수 있습니다. 통과로 꾸미지 않습니다.
        if getattr(exc, "winerror", None) == 1314 or exc.errno in {
            errno.EPERM,
            errno.EACCES,
            errno.ENOTSUP,
        }:
            pytest.skip(f"Real symlink creation unsupported/unauthorized: {exc}")
        raise
    original = audit.read_json

    def guarded(candidate):
        assert Path(candidate).resolve() != outside.resolve(), "Escaped JSON content opened"
        return original(candidate)

    monkeypatch.setattr(audit, "read_json", guarded)
    if role == "schema":

        def forbidden(*args, **kwargs):
            pytest.fail("Escaped Arrow schema content opened")

        monkeypatch.setattr(audit.pa.ipc, "read_schema", forbidden)
    with pytest.raises(ValueError, match="escapes artifact root"):
        audit.audit_run(config, root, output)


def test_keyboard_interrupt_records_interrupted_and_cli_non_success(build, monkeypatch, capsys):
    from assemble_db import cli

    config, root, output, _ = build()
    original = audit.expected

    def interrupt(source, fingerprint, ordinal, raw, aliases):
        if ordinal == 1:
            raise KeyboardInterrupt
        return original(source, fingerprint, ordinal, raw, aliases)

    monkeypatch.setattr(audit, "expected", interrupt)
    monkeypatch.setattr(cli, "load", lambda _: config)
    exit_code = cli.main(
        ["audit", "--config", str(config.path), "--run", str(root), "--output", str(output)]
    )
    assert exit_code == 130
    result = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert result["status"] == "INTERRUPTED"
    assert result["sources"]["ibm_hi_small"]["verified_rows"] == 1
    assert result["sources"]["ibm_hi_small"]["fidelity"] != "PASS"
    assert "AUDIT=PASS" not in capsys.readouterr().out


@pytest.mark.parametrize("mode", ["random_rows", "time_window"])
def test_unsupported_service_selection_is_explicit_failure(build, mode):
    config, root, output, folder = build()
    path = folder / "source_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["service_selection"] = {"mode": mode}
    write_json(path, manifest)
    with pytest.raises(ValueError, match="full-source service only"):
        audit.audit_run(config, root, output)
    assert json.loads((output / "audit.json").read_text(encoding="utf-8"))["status"] == "FAILED"


@pytest.mark.parametrize("version", [None, 4, "dajaba-service-candidate-v5"])
def test_invalid_service_version_preserves_partial_failure_report(build, version):
    config, root, output, folder = build()
    path = folder / "source_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["service_contract_version"] = version
    write_json(path, manifest)
    with pytest.raises(ValueError, match="Unsupported service version"):
        audit.audit_run(config, root, output)
    report = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    state = report["sources"]["ibm_hi_small"]
    assert report["status"] == "FAILED" and report["error_type"] == "ValueError"
    assert "finished_at" in report
    assert state["verified_rows"] == 0 and state["fidelity"] == "NOT_CHECKED"
    # 실행 전 실패했으므로 금액 판정이나 완료 필드를 만들어 채우지 않습니다.
    assert "numeric_20_6" not in state and "direct_copy_ready" not in state


@pytest.mark.parametrize("outcome", ["PASS", "FAILED", "INTERRUPTED"])
def test_terminal_report_is_committed_once_with_finished_at(build, monkeypatch, outcome):
    config, root, output, _ = build()
    original_error = (
        KeyboardInterrupt("cancelled") if outcome == "INTERRUPTED" else ValueError("bad source")
    )

    def stop_source(*args):
        raise original_error

    if outcome != "PASS":
        monkeypatch.setattr(audit, "audit_source", stop_source)
    original_replace = Path.replace
    terminal = []

    def record_replace(path, target):
        if path == output / "audit.json.tmp":
            report = json.loads(path.read_text(encoding="utf-8"))
            if report["status"] != "RUNNING":
                assert "finished_at" in report
                terminal.append(report)
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", record_replace)
    if outcome == "PASS":
        assert audit.audit_run(config, root, output)["status"] == "PASS"
    else:
        with pytest.raises(type(original_error)) as caught:
            audit.audit_run(config, root, output)
        assert caught.value is original_error
    assert len(terminal) == 1
    assert terminal[0]["status"] == outcome
    assert json.loads((output / "audit.json").read_text(encoding="utf-8")) == terminal[0]
    assert not (output / "audit.json.tmp").exists()


@pytest.mark.parametrize("mode", ["value_error", "interrupt", "cli_interrupt"])
def test_failed_final_replace_preserves_original_error_and_report(
    build, monkeypatch, caplog, capsys, mode
):
    from assemble_db import cli

    config, root, output, _ = build()
    original_error = (
        ValueError("bad source") if mode == "value_error" else KeyboardInterrupt("cancelled")
    )
    denied = PermissionError(13, "injected report replace denied", str(output / "audit.json"), 5)

    def stop_source(*args):
        raise original_error

    monkeypatch.setattr(audit, "audit_source", stop_source)
    original_replace = Path.replace
    previous_reports = []

    def deny_final_replace(path, target):
        if path == output / "audit.json.tmp":
            report = json.loads(path.read_text(encoding="utf-8"))
            if "finished_at" in report:
                previous_reports.append((output / "audit.json").read_bytes())
                raise denied
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", deny_final_replace)
    if mode == "cli_interrupt":
        monkeypatch.setattr(cli, "load", lambda _: config)
        assert (
            cli.main(
                ["audit", "--config", str(config.path), "--run", str(root), "--output", str(output)]
            )
            == 130
        )
    else:
        with pytest.raises(type(original_error)) as caught:
            audit.audit_run(config, root, output)
        assert caught.value is original_error
    assert "stop_source" in [
        frame.name for frame in traceback.extract_tb(original_error.__traceback__)
    ]
    assert any("PermissionError" in note for note in original_error.__notes__)
    assert "Audit report update was not committed" in caplog.text
    assert "injected report replace denied" in caplog.text
    assert "AUDIT=PASS" not in capsys.readouterr().out
    assert len(previous_reports) == (4 if file_commit._WINDOWS else 1)
    assert (output / "audit.json").read_bytes() == previous_reports[0]
    assert json.loads(previous_reports[0])["status"] == "RUNNING"
    pending = json.loads((output / "audit.json.tmp").read_text(encoding="utf-8"))
    assert pending["status"] == ("FAILED" if mode == "value_error" else "INTERRUPTED")
    assert pending["error_type"] == type(original_error).__name__
    assert "finished_at" in pending


@pytest.mark.parametrize("operation", ["write", "replace"])
def test_successful_audit_cli_fails_if_final_report_cannot_be_saved(
    build, monkeypatch, caplog, capsys, operation
):
    from assemble_db import cli

    config, root, output, _ = build()
    denied = PermissionError(13, "injected final report save denied", str(output / "audit.json"), 5)
    original_write = Path.write_text
    original_replace = Path.replace
    previous_reports = []

    def deny_final_write(path, text, *args, **kwargs):
        if path == output / "audit.json.tmp" and operation == "write":
            report = json.loads(text)
            if "finished_at" in report:
                assert report["status"] == "PASS"
                previous_reports.append((output / "audit.json").read_bytes())
                raise denied
        return original_write(path, text, *args, **kwargs)

    def deny_final_replace(path, target):
        if path == output / "audit.json.tmp" and operation == "replace":
            report = json.loads(path.read_text(encoding="utf-8"))
            if "finished_at" in report:
                assert report["status"] == "PASS"
                previous_reports.append((output / "audit.json").read_bytes())
                raise denied
        return original_replace(path, target)

    monkeypatch.setattr(Path, "write_text", deny_final_write)
    monkeypatch.setattr(Path, "replace", deny_final_replace)
    monkeypatch.setattr(cli, "load", lambda _: config)
    assert (
        cli.main(
            ["audit", "--config", str(config.path), "--run", str(root), "--output", str(output)]
        )
        == 2
    )
    assert "AUDIT=PASS" not in capsys.readouterr().out
    assert "Audit report update was not committed" in caplog.text
    assert len(previous_reports) == (4 if operation == "replace" and file_commit._WINDOWS else 1)
    assert (output / "audit.json").read_bytes() == previous_reports[0]
    saved = json.loads(previous_reports[0])
    assert saved["status"] == "RUNNING" and "finished_at" not in saved
    assert (output / "audit.json.tmp").exists() == (operation == "replace")


def test_initial_report_replace_failure_stops_before_audit(build, monkeypatch, caplog):
    config, root, output, _ = build()
    denied = PermissionError(
        13, "injected initial report replace denied", str(output / "audit.json"), 5
    )
    original_replace = Path.replace
    attempts = []

    def deny_replace(path, target):
        if path == output / "audit.json.tmp":
            attempts.append(path)
            raise denied
        return original_replace(path, target)

    def forbidden(*args):
        pytest.fail("Source audit started without an initial report")

    monkeypatch.setattr(Path, "replace", deny_replace)
    monkeypatch.setattr(audit, "audit_source", forbidden)
    with pytest.raises(PermissionError) as caught:
        audit.audit_run(config, root, output)
    assert caught.value is denied and len(attempts) == (4 if file_commit._WINDOWS else 1)
    assert "Audit report update was not committed" in caplog.text
    assert not (output / "audit.json").exists()
    pending = json.loads((output / "audit.json.tmp").read_text(encoding="utf-8"))
    assert pending["status"] == "RUNNING" and "finished_at" not in pending
