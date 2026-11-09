"""원천별로 조립을 격리하고 발췌를 명시하며 완료된 원천을 활용해 재개합니다."""

import hashlib
import json
import logging
import random
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
from dbcheck.data_contract import POLICY_VERSION, bank_alias, service_account_key
from dbcheck.distribution import IBM, SAML, sha256, write_json

from .config import VERSION, Config, ServiceSelection
from .contracts import bind, read_json
from .exporting import DatasetWriter, primary_part_path, reload_file, verify_dataset
from .normalize import (
    ACCOUNT_HEADER,
    ACCOUNT_SCHEMA,
    SERVICE_MAPPING_SCHEMA,
    batches,
    label_schema,
    normalize,
    normalize_accounts,
    service_schema,
    service_tables,
    transaction_schema,
)
from .service_contract import (
    SERVICE_ACCOUNT_SCHEMA,
    SERVICE_CONTRACT_VERSION,
    SERVICE_ENTITY_SCHEMA,
    SERVICE_NUMERIC_TYPE,
    SUPPORTED_SERVICE_CONTRACTS,
    audit_service_money,
    new_money_audit,
    validate_service_handoff,
)

LOG = logging.getLogger("assemble_db")


def code_fingerprint() -> str:
    files = {p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")}
    import dbcheck.data_contract as contract

    files["dbcheck.data_contract"] = sha256(Path(contract.__file__))
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def make_writer(folder: Path, table: str, schema: pa.Schema, metadata: dict) -> DatasetWriter:
    return DatasetWriter(folder / table, schema, dict(**metadata, table=table))


def ensure_space(path: Path) -> None:
    if shutil.disk_usage(path).free < 5 * 1024**3:
        raise OSError(
            "Less than 5 GiB free; source stopped without deleting originals or completed outputs"
        )


def _service_selection(
    selection: ServiceSelection, input_rows: int
) -> tuple[set[int] | None, dict]:
    chosen = None
    if selection.mode == "random_rows":
        assert selection.count is not None
        if selection.count >= input_rows:
            raise ValueError("Random service count must be smaller than the HI-Small source")
        chosen = set(random.Random(selection.seed).sample(range(input_rows), selection.count))
        metadata = dict(
            mode=selection.mode,
            count=selection.count,
            seed=selection.seed,
            replacement=False,
            label_blind=True,
            history_complete=False,
            purpose="BACKEND_INTEGRATION_SAMPLE",
            sampled_row_ids_sha256=hashlib.sha256(
                json.dumps(sorted(chosen), separators=(",", ":")).encode()
            ).hexdigest(),
        )
    elif selection.mode == "full_source":
        metadata = dict(
            mode="full_source",
            purpose="FULL_HI_SMALL_SERVICE_CANDIDATE",
            source_rows_complete=True,
            backend_history_coverage="UNVERIFIED",
        )
    else:
        metadata = dict(
            mode="time_window",
            start=selection.start.isoformat(),
            end=selection.end.isoformat(),
            history_start=selection.history_start.isoformat(),
            interval="[history_start,end); TARGET >= start",
        )
    return chosen, metadata


def _write_service_dimensions(
    folder: Path,
    batch_rows: int,
    metadata: dict,
    referenced: set[tuple[str, str]],
    master: dict,
) -> tuple[dict[str, DatasetWriter], dict]:
    accounts = []
    entities = {}
    # 여러 원본 표기가 하나의 서비스 계좌로 연결될 수 있습니다. 원본 거래의
    # 표기는 키가 아니라 거래 출력에 보존합니다.
    for bank, account in sorted({(bank_alias(b), a) for b, a in referenced}):
        match = master.get((bank_alias(bank), account))
        if match is None:
            raise ValueError("Selected service transaction has no original account owner")
        accounts.append(
            dict(
                account_key=service_account_key(bank, account),
                bank_id=bank,
                account_no=account,
                entity_id=match["entity_id_raw"],
                master_bank_id_raw=match["bank_raw"],
                internal_account_key=match["account_key"],
                bank_name=match["bank_name_raw"],
            )
        )
        old = entities.setdefault(match["entity_id_raw"], match["entity_name_raw"])
        if old != match["entity_name_raw"]:
            raise ValueError("Original entity has inconsistent names")
    writers = {}
    for table_name, schema, rows in [
        ("accounts", SERVICE_ACCOUNT_SCHEMA, accounts),
        (
            "entities",
            SERVICE_ENTITY_SCHEMA,
            [dict(entity_id=k, entity_name=v) for k, v in sorted(entities.items())],
        ),
    ]:
        writer = make_writer(folder / "service", table_name, schema, metadata)
        for start in range(0, len(rows), batch_rows):
            writer.write(pa.Table.from_pylist(rows[start : start + batch_rows], schema=schema))
        writers["service_" + table_name] = writer
    dimensions = dict(
        accounts=len(accounts),
        entities=len(entities),
        references_complete=True,
        synthetic_customer_attributes_created=False,
        bank_alias_resolution="normalized service bank ID; original bank IDs in transactions and master_bank_id_raw; internal ML keys unchanged",
    )
    return writers, dimensions


def assemble_source(source, config: Config, validation: Path, folder: Path, purpose: str) -> dict:
    report, fingerprints = bind(source, config, validation)
    fingerprint = fingerprints[str(source.path)]
    release = f"{source.id}:{fingerprint}"
    common = dict(
        contract_version=VERSION,
        source_id=source.id,
        source_kind=source.kind,
        source_sha256=fingerprint,
        source_release=release,
        raw_input_rows=report["input"]["rows"],
        validation_report=str(validation / source.id / "validation_report.json"),
        validation_report_sha256=sha256(validation / source.id / "validation_report.json"),
        config_sha256=sha256(config.path),
        dbcheck_config_sha256=config.check.config_sha256,
        aliases_sha256=config.check.aliases_sha256,
        code_sha256=code_fingerprint(),
        money_identity_policy_version=POLICY_VERSION,
        timestamp_precision="MINUTE" if source.kind == "ibm" else "SECOND",
        timezone="UNKNOWN",
    )
    ml = purpose in {"ml", "all"}
    service = purpose in {"service", "all"} and source.id == "ibm_hi_small"
    if service and config.service is None:
        raise ValueError(
            "Service excerpt is not specified; configure start/end/history_start, or build --purpose ml"
        )
    writers = {}
    chosen: set[int] | None = None
    random_batches: list[pa.Table] = []
    selection_metadata = None
    if ml:
        metadata = dict(**common, purpose="ml", scope="FULL_SOURCE", labels_separate=True)
        writers["ml_transactions"] = make_writer(
            folder / "ml", "transactions", transaction_schema(source), metadata
        )
        writers["ml_labels"] = make_writer(folder / "ml", "labels", label_schema(source), metadata)
    if service:
        selection = config.service
        assert selection is not None
        chosen, selection_metadata = _service_selection(selection, report["input"]["rows"])
        metadata = dict(
            **common,
            purpose="service",
            scope="FULL_SOURCE_CANDIDATE" if selection.mode == "full_source" else "EXCERPT",
            selection=selection_metadata,
            service_contract_version=SERVICE_CONTRACT_VERSION,
            service_account_key_policy="NORMALIZED_BANK_ID_ORIGINAL_ACCOUNT_NUMBER",
            raw_bank_fields=["from_bank_id_raw", "to_bank_id_raw"],
            id_namespace="OFFLINE_CANDIDATE_NOT_BACKEND_OPS",
            candidate_ids="candidate_transaction_id = source_row_id + 1; never ops.transaction_id",
            source_row_convention="source_row_id is zero-based; mapping source_row_number is one-based, header excluded; neither is backend transaction_id",
            backend_identity_mapping="REQUIRED_FROM_BACKEND; match original file SHA-256 and agreed row convention",
            direct_copy_ready=False,
            source_copy_accepted=False,
        )
        writers["service_transactions"] = make_writer(
            folder / "service", "transactions", service_schema(source), metadata
        )
        writers["service_mapping"] = make_writer(
            folder / "service", "transaction_mapping", SERVICE_MAPPING_SCHEMA, metadata
        )
    processed = positives = selected = target = 0
    money_unsupported = 0
    service_money = new_money_audit() if service else None
    referenced = set()
    for offset, batch in batches(
        source.path, IBM if source.kind == "ibm" else SAML, config.batch_rows
    ):
        ensure_space(folder)
        tx, labels = normalize(source, fingerprint, offset, batch, config.check.currency_aliases)
        processed += len(tx)
        positives += sum(labels.column("source_positive").to_pylist())
        if ml:
            writers["ml_transactions"].write(tx)
            writers["ml_labels"].write(labels)
        if source.kind == "saml":
            money_unsupported += sum(
                not v for v in tx.column("money_operations_allowed").to_pylist()
            )
        if service:
            assert config.service is not None
            if config.service.mode == "full_source":
                keep = [True] * len(tx)
            elif chosen is not None:
                keep = [i in chosen for i in tx.column("source_row_id").to_pylist()]
            else:
                times = tx.column("ts").to_pylist()
                keep = [config.service.history_start <= t < config.service.end for t in times]
            subset = tx.filter(pa.array(keep, type=pa.bool_()))
            if len(subset):
                if chosen is not None:
                    random_batches.append(subset)
                    selected += len(subset)
                    target += len(subset)
                else:
                    output, mapping, keys = service_tables(source, subset, config.service.start)
                    assert service_money is not None
                    audit_service_money(output, service_money)
                    writers["service_transactions"].write(output)
                    writers["service_mapping"].write(mapping)
                    referenced.update(keys)
                    selected += len(output)
                    target += sum(v == "TARGET" for v in mapping.column("role").to_pylist())
        if processed % 250000 < len(tx):
            LOG.info(
                "%s: %s/%s transactions; selected service=%s",
                source.id,
                f"{processed:,}",
                f"{report['input']['rows']:,}",
                f"{selected:,}",
            )
            write_json(
                folder / "progress.json",
                dict(
                    stage="TRANSACTIONS",
                    source=source.id,
                    processed=processed,
                    service_selected=selected,
                    expected=report["input"]["rows"],
                ),
            )
    if processed != report["input"]["rows"]:
        raise ValueError("Original row count differs from validation")
    profile = read_json(validation / source.id / f"{source.id}_profile.json")
    if positives != profile["labels"].get("1", 0):
        raise ValueError("Source label count differs from validation")
    if service:
        assert config.service is not None
        if (
            selected == 0
            or target == 0
            or (config.service.mode != "full_source" and selected >= processed)
            or (config.service.mode == "full_source" and selected != processed)
        ):
            raise ValueError(
                "Service selection must match its explicit full-source or excerpt scope"
            )
    if chosen is not None:
        assert config.service is not None and selected == config.service.count
        output, mapping, keys = service_tables(
            source, pa.concat_tables(random_batches), datetime.min
        )
        assert service_money is not None
        audit_service_money(output, service_money)
        referenced.update(keys)
        for offset in range(0, len(output), config.batch_rows):
            writers["service_transactions"].write(output.slice(offset, config.batch_rows))
            writers["service_mapping"].write(mapping.slice(offset, config.batch_rows))
    dimensions = {}
    if source.kind == "ibm":
        assert source.accounts is not None
        master = {}
        account_writer = (
            make_writer(
                folder / "ml",
                "accounts",
                ACCOUNT_SCHEMA,
                dict(**common, purpose="ml", scope="FULL_SOURCE"),
            )
            if ml
            else None
        )
        for _, batch in batches(source.accounts, ACCOUNT_HEADER, config.batch_rows):
            normalized = normalize_accounts(source, release, batch)
            if account_writer:
                account_writer.write(normalized)
            if service:
                for item in normalized.to_pylist():
                    key = (bank_alias(item["bank_raw"]), item["account_raw"])
                    if key in master:
                        raise ValueError("Nonunique account master key")
                    master[key] = item
        if account_writer:
            writers["ml_accounts"] = account_writer
        if service:
            dimension_writers, dimensions = _write_service_dimensions(
                folder, config.batch_rows, metadata, referenced, master
            )
            writers.update(dimension_writers)
    datasets = {
        name: dict(path=str(writer.folder.relative_to(folder)), rows=writer.finish()["rows"])
        for name, writer in writers.items()
    }
    for path, digest in fingerprints.items():
        if sha256(Path(path)) != digest:
            raise ValueError("An original file changed during transformation")
    result = dict(
        status="VERIFIED",
        source_id=source.id,
        source_release=release,
        purpose=purpose,
        input_fingerprints=fingerprints,
        input_rows=processed,
        positive_rows=positives,
        ml_output_rows=processed if ml else None,
        service_output_rows=selected if service else None,
        service_target_rows=target if service else None,
        service_history_rows=selected - target if service else None,
        service_outside_scope_rows=processed - selected if service else None,
        excluded_rows=0,
        quarantined_rows=0,
        duplicate_occurrences_preserved=True,
        unsupported_amount_currency_rows=money_unsupported,
        dimensions=dimensions,
        datasets=datasets,
        reconciliation=dict(
            ml="input = transactions = labels" if ml else "not selected",
            service="input = selected + outside_scope; selected = target + history"
            if service
            else "not selected",
        ),
        source_copy_accepted=False,
        service_selection=selection_metadata,
        service_contract_version=SERVICE_CONTRACT_VERSION if service else None,
        service_money_compatibility=service_money,
        direct_copy_ready=False if service else None,
        scoring_api_implemented=False,
        training_executed=False,
    )
    if service_money is not None and service_money["incompatible_rows"]:
        LOG.warning(
            "%s: %s selected rows cannot fit %s exactly; original amounts retained. "
            "Service output is a candidate, not COPY-ready; see source_manifest.json.",
            source.id,
            service_money["incompatible_rows"],
            SERVICE_NUMERIC_TYPE,
        )
    write_json(folder / "source_manifest.json", result)
    if service:
        expected = service_expectations(result)
        write_json(folder / "service" / "expected.json", expected)
        result["service_expected"] = dict(
            path="service/expected.json", sha256=sha256(folder / "service" / "expected.json")
        )
        write_json(folder / "source_manifest.json", result)
    return result


def service_expectations(source: dict) -> dict:
    """오프라인 건수를 인계합니다. 룰 정답이나 백엔드 인수 완료의 증거가 아닙니다."""
    return dict(
        version="dajaba-service-counts-v1",
        source_id=source["source_id"],
        source_release=source["source_release"],
        original_rows=source["input_rows"],
        selection=source["service_selection"],
        expected_table_rows={
            k.removeprefix("service_"): v["rows"]
            for k, v in source["datasets"].items()
            if k.startswith("service_")
        },
        target_rows=source["service_target_rows"],
        history_rows=source["service_history_rows"],
        outside_scope_rows=source["service_outside_scope_rows"],
        account_scope=(
            "TRANSACTION_REFERENCED_NORMALIZED_KEYS; not the entire account master"
            if source.get("service_contract_version") == SERVICE_CONTRACT_VERSION
            else "TRANSACTION_REFERENCED_RAW_KEYS; not the entire account master"
        ),
        id_namespace="OFFLINE_CANDIDATE_NOT_BACKEND_OPS",
        backend_copy_verified=False,
        backend_history_coverage="UNVERIFIED",
        rule_expectations=None,
        rule_expectations_status="PENDING_FINAL_RULE_SPEC_AND_ACTUAL_BACKEND_ID_MAPPING",
    )


def verify_source(folder: Path) -> dict:
    source = read_json(folder / "source_manifest.json")
    if source["status"] != "VERIFIED":
        raise ValueError("Source is incomplete")
    for name, item in source["datasets"].items():
        path = (folder / item["path"]).resolve()
        if not path.is_relative_to(folder.resolve()):
            raise ValueError("Unsafe dataset path")
        result = verify_dataset(path)
        if result["rows"] != item["rows"]:
            raise ValueError(f"Source dataset count mismatch: {name}")
    if source["ml_output_rows"] is not None:
        if (
            source["datasets"]["ml_transactions"]["rows"] != source["input_rows"]
            or source["datasets"]["ml_labels"]["rows"] != source["input_rows"]
        ):
            raise ValueError("ML input/transaction/label reconciliation failed")
    if source.get("service_output_rows") is not None and (
        source.get("service_contract_version") not in SUPPORTED_SERVICE_CONTRACTS
    ):
        raise ValueError("Unsupported service contract version")
    if source.get("service_contract_version") in SUPPORTED_SERVICE_CONTRACTS:
        if source.get("service_expected"):
            handoff = source["service_expected"]
            path = folder / "service" / "expected.json"
            if handoff["path"] != "service/expected.json" or sha256(path) != handoff["sha256"]:
                raise ValueError("Service expected.json fingerprint mismatch")
            if read_json(path) != service_expectations(source):
                raise ValueError("Service expected.json count/scope mismatch")
        selected = source["service_output_rows"]
        if (
            selected != source["datasets"]["service_transactions"]["rows"]
            or selected != source["datasets"]["service_mapping"]["rows"]
            or selected != source["service_target_rows"] + source["service_history_rows"]
            or source["input_rows"] != selected + source["service_outside_scope_rows"]
        ):
            raise ValueError("Service row reconciliation failed")
        dataset = folder / source["datasets"]["service_transactions"]["path"]
        manifest = read_json(dataset / "manifest.json")
        if manifest.get("id_namespace") != "OFFLINE_CANDIDATE_NOT_BACKEND_OPS":
            raise ValueError("Service candidate namespace mismatch")
        mapping_dataset = folder / source["datasets"]["service_mapping"]["path"]
        mapping_manifest = read_json(mapping_dataset / "manifest.json")
        if len(manifest["parts"]) != len(mapping_manifest["parts"]):
            raise ValueError("Service transaction/mapping part alignment mismatch")
        audit = new_money_audit(service_contract_version=source["service_contract_version"])
        table_schema = pa.ipc.read_schema(pa.BufferReader((dataset / "schema.arrow").read_bytes()))
        mapping_schema = pa.ipc.read_schema(
            pa.BufferReader((mapping_dataset / "schema.arrow").read_bytes())
        )
        for part, mapping_part in zip(manifest["parts"], mapping_manifest["parts"]):
            table = reload_file(primary_part_path(dataset, part), table_schema)
            if "transaction_id" in table.schema.names:
                raise ValueError("Offline candidates must not claim backend transaction_id")
            validate_service_handoff(
                table,
                reload_file(primary_part_path(mapping_dataset, mapping_part), mapping_schema),
                service_contract_version=source["service_contract_version"],
                source_release=source["source_release"],
            )
            audit_service_money(table, audit)
        if audit != source["service_money_compatibility"]:
            raise ValueError("Service money compatibility audit mismatch")
        if source.get("direct_copy_ready") is not False:
            raise ValueError("Backend mapping and COPY acceptance have not been verified")
    return source


def execute(
    config: Config, validation: Path, purpose="all", selected=None, resume: Path | None = None
) -> tuple[Path, bool]:
    selected_ids = set(selected or [s.id for s in config.check.sources])
    if selected_ids - {s.id for s in config.check.sources}:
        raise ValueError("Unknown selected source")
    if purpose == "service":
        selected_ids &= {"ibm_hi_small"}
    if purpose in {"all", "service"} and "ibm_hi_small" in selected_ids and config.service is None:
        raise ValueError(
            "Specify the HI excerpt in config before service/all; --purpose ml is independent"
        )
    if not selected_ids:
        raise ValueError("No eligible sources selected")
    validation = validation.resolve()
    binding = dict(
        config_sha256=sha256(config.path),
        code_sha256=code_fingerprint(),
        validation_run=str(validation),
        validation_run_sha256=sha256(validation / "run.json"),
        purpose=purpose,
        selected_sources=sorted(selected_ids),
    )
    config.output.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any]
    if resume:
        root = resume.resolve()
        if not root.is_relative_to(config.output) or root == config.output:
            raise ValueError("Resume path must be a run inside output_root")
        manifest = read_json(root / "run.json")
        if manifest["binding"] != binding:
            raise ValueError("Inputs/config/code changed: resume is forbidden; create a new run")
    else:
        root = config.output / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
        )
        root.mkdir(exist_ok=False)
        manifest = dict(
            contract_version=VERSION,
            status="RUNNING",
            binding=binding,
            scope="FULL_ML_SOURCE_AND_EXPLICIT_HI_SELECTION",
            sources={},
            started_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            training_executed=False,
            source_copy_executed=False,
        )
    write_json(root / "run.json", manifest)
    for source in config.check.sources:
        if source.id not in selected_ids:
            continue
        previous = manifest["sources"].get(source.id)
        if previous and previous["status"] == "VERIFIED":
            bind(source, config, validation)
            verify_source(root / previous["folder"])
            LOG.info("%s: verified completed source reused explicitly", source.id)
            continue
        attempt = 1
        folder = root / source.id / f"attempt-{attempt:02d}"
        while folder.exists():
            attempt += 1
            folder = root / source.id / f"attempt-{attempt:02d}"
        folder.mkdir(parents=True)
        try:
            result = assemble_source(source, config, validation, folder, purpose)
            manifest["sources"][source.id] = dict(
                status=result["status"],
                folder=str(folder.relative_to(root)),
                input_rows=result["input_rows"],
                datasets=result["datasets"],
            )
        except Exception as exc:
            LOG.exception("%s failed; independent sources continue", source.id)
            write_json(
                folder / "failure.json",
                dict(source=source.id, error_type=type(exc).__name__, error=str(exc)),
            )
            manifest["sources"][source.id] = dict(
                status="FAILED", folder=str(folder.relative_to(root)), error=str(exc)
            )
        write_json(root / "run.json", manifest)
    success = all(s["status"] == "VERIFIED" for s in manifest["sources"].values())
    manifest.update(
        status="COMPLETED" if success else "FAILED",
        finished_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    write_json(root / "run.json", manifest)
    return root, success


def verify_run(root: Path) -> dict:
    manifest = read_json(root / "run.json")
    if manifest.get("contract_version") != VERSION or manifest.get("status") != "COMPLETED":
        raise ValueError("Run is incomplete or unsupported")
    result = {}
    for source, item in manifest["sources"].items():
        folder = (root / item["folder"]).resolve()
        if not folder.is_relative_to(root.resolve()):
            raise ValueError("Unsafe source folder")
        result[source] = verify_source(folder)
    return dict(status="VERIFIED", sources=result)
