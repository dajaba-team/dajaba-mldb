"""검증된 canonical 텍스트에서 ML Parquet과 HI 서비스 v4를 분리 생성합니다."""

import hashlib
import platform
import shutil
import tomllib
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from dbcheck.data_contract import POLICY_VERSION
from dbcheck.distribution import sha256

from .audit import AuditReport
from .config import Config
from .contracts import bind
from .exporting import DatasetWriter
from .normalize import (
    ACCOUNT_SCHEMA,
    SERVICE_MAPPING_SCHEMA,
    label_schema,
    service_schema,
    service_tables,
    transaction_schema,
)
from .pipeline import code_fingerprint, service_expectations, verify_run, verify_source
from .service_contract import (
    SERVICE_CONTRACT_VERSION,
    audit_service_money,
    new_money_audit,
    validate_service_handoff,
)
from .staged_contract import (
    SOURCE_IDS,
    STAGED_VERSION,
    Progress,
    RunLock,
    TextInput,
    archived_config,
    atomic_json,
    capture_text,
    read_metadata,
    require,
    utc,
)
from .staged_dimensions import Dimensions


def _format_map(
    config: Config, selected: list[str] | None, options: list[str] | None
) -> dict[str, str]:
    ids = set(selected or [source.id for source in config.check.sources])
    require(bool(ids) and not ids - SOURCE_IDS, "Only explicit project sources are supported")
    require(
        not ids - {source.id for source in config.check.sources},
        "Selected source missing from configuration",
    )
    result = {sid: "csv+json" for sid in sorted(ids)}
    for option in options or []:
        parts = option.split("=")
        require(
            len(parts) == 2 and parts[0] in result and parts[1] == "csv+json",
            "Both canonical CSV and JSON are required",
        )
        result[parts[0]] = parts[1]
    return result


def _proof(
    old_run: Path, proof_path: Path, execution_path: Path, validation: Path
) -> tuple[dict, dict]:
    manifest = read_metadata(old_run / "run.json")
    proof = read_metadata(proof_path)
    execution = read_metadata(execution_path)
    require(
        manifest.get("status") == "COMPLETED"
        and manifest.get("contract_version") == "dajaba-assembly-v2",
        "Canonical normalization run is not complete",
    )
    require(
        proof.get("status") == "PASS" and proof.get("report_is_not_checkpoint") is True,
        "A completed original-to-canonical fidelity audit is required",
    )
    require(
        Path(execution.get("report", "")).resolve() == proof_path.resolve()
        and execution.get("report_sha256") == sha256(proof_path),
        "Fidelity execution record is not bound to this exact audit report",
    )
    require(
        type(execution.get("exit_code")) is int and execution["exit_code"] == 0,
        "Fidelity audit actual exit zero is required",
    )
    require(
        Path(proof["assembly_run"]).resolve() == old_run.resolve()
        and proof["assembly_run_sha256"] == sha256(old_run / "run.json"),
        "Fidelity proof belongs to another normalization run",
    )
    require(
        Path(manifest["binding"]["validation_run"]).resolve() == validation.resolve()
        and proof["validation_run_sha256"] == sha256(validation / "run.json"),
        "Original DBcheck validation binding differs",
    )
    return manifest, proof


def _common(
    source, config: Config, report: dict, fingerprints: dict, validation: Path, code: str
) -> dict:
    fingerprint = fingerprints[str(source.path)]
    return dict(
        contract_version="dajaba-assembly-v2",
        source_id=source.id,
        source_kind=source.kind,
        source_sha256=fingerprint,
        source_release=source.id + ":" + fingerprint,
        raw_input_rows=report["input"]["rows"],
        validation_report=str(validation / source.id / "validation_report.json"),
        validation_report_sha256=sha256(validation / source.id / "validation_report.json"),
        config_sha256=sha256(config.path),
        dbcheck_config_sha256=config.check.config_sha256,
        aliases_sha256=config.check.aliases_sha256,
        code_sha256=code,
        money_identity_policy_version=POLICY_VERSION,
        timestamp_precision="MINUTE" if source.kind == "ibm" else "SECOND",
        timezone="UNKNOWN",
    )


def _decimal_raw(table: pa.Table) -> dict[str, list[str | None]] | None:
    result = {}
    for field in table.schema:
        if pa.types.is_decimal(field.type):
            original = field.name + "_raw"
            require(
                original in table.schema.names,
                "Canonical JSON requires preserved original decimal text",
            )
            result[field.name] = table.column(original).to_pylist()
    return result or None


def _canonical_inputs(
    source,
    old_folder: Path,
    folder: Path,
    config: Config,
    common: dict,
    selected_format: str,
    progress: Progress,
    audited: dict,
) -> tuple[dict[str, TextInput], dict]:
    require(selected_format == "csv+json", "Both canonical CSV and JSON are required")
    require(
        sha256(old_folder / "source_manifest.json") == audited["source_manifest_sha256"],
        "Prior source manifest is not bound to the completed audit",
    )
    old_source = read_metadata(old_folder / "source_manifest.json")
    require(
        old_source["status"] == "VERIFIED"
        and old_source["source_id"] == source.id
        and old_source["source_release"] == common["source_release"]
        and old_source["ml_output_rows"] == common["raw_input_rows"],
        "Prior normalized source binding differs",
    )
    metadata = dict(common, purpose="ml", scope="FULL_SOURCE", labels_separate=True)
    roles = {
        "transactions": ("ml_transactions", transaction_schema(source)),
        "labels": ("ml_labels", label_schema(source)),
    }
    if source.accounts is not None:
        roles["accounts"] = ("ml_accounts", ACCOUNT_SCHEMA)
    result = {}
    datasets = {}
    expected = {
        name: metadata[name]
        for name in (
            "source_id",
            "source_kind",
            "source_sha256",
            "source_release",
            "raw_input_rows",
            "purpose",
            "scope",
            "aliases_sha256",
            "money_identity_policy_version",
        )
    }
    for name, (role, schema) in roles.items():
        item = old_source["datasets"][role]
        old_table = (old_folder / item["path"]).resolve()
        require(old_table.is_relative_to(old_folder.resolve()), "Unsafe prior canonical table path")
        original = capture_text(
            old_table,
            schema,
            expected,
            "csv",
            progress,
            source.id + "/verified_normalization_reuse/" + name,
        )
        require(original.manifest["rows"] == item["rows"], "Prior table row count differs")
        require(
            original.manifest_sha256 == audited["tables"][role]["manifest_sha256"]
            and original.schema_sha256 == audited["tables"][role]["schema_sha256"],
            "Prior canonical metadata differs from its completed fidelity audit",
        )
        table_folder = folder / "canonical" / name
        writer = DatasetWriter(
            table_folder,
            schema,
            dict(
                metadata,
                table=name,
                physical_stage="CANONICAL_CSV_AND_JSON_FROM_VERIFIED_NORMALIZATION",
            ),
            formats=("csv", "json"),
        )
        for index in range(len(original.manifest["parts"])):
            table = original.read(index)
            require(
                shutil.disk_usage(folder).free > 5 * 1024**3,
                "Canonical output free-space floor reached",
            )
            writer.write(table, decimal_raw=_decimal_raw(table))
            progress.emit(
                source.id + "/canonical_csv_json/" + name,
                index + 1,
                len(original.manifest["parts"]),
                "parts",
            )
        writer.finish()
        original.verify_metadata()
        result[name] = capture_text(
            table_folder,
            schema,
            expected,
            "csv",
            progress,
            source.id + "/canonical_pair_commit/" + name,
        )
        datasets["canonical_" + name] = dict(
            path=str(table_folder.relative_to(folder)), rows=item["rows"]
        )
    require(
        [p["rows"] for p in result["transactions"].manifest["parts"]]
        == [p["rows"] for p in result["labels"].manifest["parts"]],
        "Canonical transaction and label part alignment differs",
    )
    return result, datasets


def _derive(
    name: str,
    canonical: TextInput,
    folder: Path,
    metadata: dict,
    reference_sha: str,
    progress: Progress,
) -> dict:
    writer = DatasetWriter(
        folder,
        canonical.schema,
        dict(
            metadata,
            table=name,
            physical_stage="ML_PARQUET_DERIVED_FROM_COMMITTED_TEXT",
            canonical_input_reference_sha256=reference_sha,
        ),
        formats=("parquet",),
    )
    require(
        canonical.manifest["rows"] == 0 or all(p["rows"] > 0 for p in canonical.manifest["parts"]),
        "Nonempty canonical input contains an empty part",
    )
    for index in range(len(canonical.manifest["parts"])):
        # 반드시 커밋된 텍스트 파일을 다시 읽습니다. 원본 변환 중 Arrow 테이블은 사용하지 않습니다.
        table = canonical.read(index)
        paired = TextInput(
            canonical.folder,
            canonical.schema,
            canonical.manifest,
            "json",
            canonical.manifest_sha256,
            canonical.schema_sha256,
        )
        require(
            paired.read(index).equals(table), "Committed canonical CSV/JSON values or order differ"
        )
        require(
            shutil.disk_usage(folder).free > 5 * 1024**3,
            "ML Parquet output free-space floor reached",
        )
        writer.write(table)
        progress.emit(
            metadata["source_id"] + "/derive_parquet/" + name,
            index + 1,
            len(canonical.manifest["parts"]),
            "parts",
        )
    manifest = writer.finish()
    require(
        len(manifest["parts"]) == len(canonical.manifest["parts"]), "Derived part alignment differs"
    )
    for index, (target, original) in enumerate(zip(manifest["parts"], canonical.manifest["parts"])):
        require(
            target["rows"] == original["rows"]
            and target["logical_sha256"] == original["logical_sha256"],
            "Derived Parquet differs from canonical typed logical values",
        )
        item = next(
            f for f in original["files"] if Path(f["path"]).suffix == "." + canonical.format
        )
        target["canonical_source"] = dict(
            path=str(canonical.path(index)),
            sha256=item["sha256"],
            format=canonical.format,
            logical_sha256=original["logical_sha256"],
        )
    atomic_json(folder / "manifest.json", manifest)
    canonical.verify_metadata()
    return manifest


def _service(
    source, canonical: dict[str, TextInput], folder: Path, common: dict, progress: Progress
) -> tuple[dict, dict]:
    require(
        source.id == "ibm_hi_small" and "accounts" in canonical,
        "HI service requires its original master",
    )
    selection = dict(
        mode="full_source",
        purpose="FULL_HI_SMALL_SERVICE_CANDIDATE",
        source_rows_complete=True,
        backend_history_coverage="UNVERIFIED",
    )
    metadata = dict(
        common,
        purpose="service",
        scope="FULL_SOURCE_CANDIDATE",
        selection=selection,
        service_contract_version=SERVICE_CONTRACT_VERSION,
        service_account_key_policy="NORMALIZED_BANK_ID_ORIGINAL_ACCOUNT_NUMBER",
        raw_bank_fields=["from_bank_id_raw", "to_bank_id_raw"],
        id_namespace="OFFLINE_CANDIDATE_NOT_BACKEND_OPS",
        candidate_ids="candidate_transaction_id = source_row_id + 1; never ops.transaction_id",
        source_row_convention="source_row_id zero-based; source_row_number one-based excluding header",
        backend_identity_mapping="REQUIRED_FROM_BACKEND",
        direct_copy_ready=False,
        source_copy_accepted=False,
    )
    tx_writer = DatasetWriter(
        folder / "service" / "transactions",
        service_schema(source),
        dict(metadata, table="transactions"),
        formats=("csv", "json"),
    )
    map_writer = DatasetWriter(
        folder / "service" / "transaction_mapping",
        SERVICE_MAPPING_SCHEMA,
        dict(metadata, table="transaction_mapping"),
        formats=("csv", "json"),
    )
    dimensions = Dimensions(folder / "work" / "service_dimensions")
    money = new_money_audit()
    processed = 0
    try:
        dimensions.load_master(canonical["accounts"], common["source_release"], progress)
        for index in range(len(canonical["transactions"].manifest["parts"])):
            require(
                shutil.disk_usage(folder).free > 5 * 1024**3,
                "Staged output free-space floor reached",
            )
            table = canonical["transactions"].read(index)
            transactions, mapping, _ = service_tables(source, table, datetime.min)
            require(
                transactions.column("source_row_id").equals(table.column("source_row_id")),
                "Service transaction original row order differs",
            )
            validate_service_handoff(
                transactions,
                mapping,
                service_contract_version=SERVICE_CONTRACT_VERSION,
                source_release=common["source_release"],
            )
            audit_service_money(transactions, money)
            originals = {
                name: table.column(name + "_raw").to_pylist()
                for name in ("amount_paid", "amount_received")
            }
            tx_writer.write(transactions, decimal_raw=originals)
            map_writer.write(mapping)
            dimensions.add_references(transactions)
            processed += len(table)
            progress.emit(
                source.id + "/service_v4", processed, common["raw_input_rows"], "transactions"
            )
        require(processed == common["raw_input_rows"], "Full HI service row coverage differs")
        require(
            money["incompatible_values"] == 0,
            "HI service values cannot fit numeric(20,6) without loss",
        )
        tx = tx_writer.finish()
        mapping = map_writer.finish()
        datasets, counts = dimensions.export(folder / "service", metadata, progress)
    finally:
        dimensions.close()
    datasets.update(
        service_transactions=dict(path="service/transactions", rows=tx["rows"]),
        service_mapping=dict(path="service/transaction_mapping", rows=mapping["rows"]),
    )
    return datasets, dict(
        service_selection=selection,
        service_contract_version=SERVICE_CONTRACT_VERSION,
        service_output_rows=processed,
        service_target_rows=processed,
        service_history_rows=0,
        service_outside_scope_rows=0,
        service_money_compatibility=money,
        direct_copy_ready=False,
        dimensions=counts,
        source_copy_accepted=False,
    )


def _build_source(
    source,
    config: Config,
    validation: Path,
    old_folder: Path,
    folder: Path,
    fmt: str,
    report: dict,
    fingerprints: dict,
    code: str,
    proof_sha: str,
    progress: Progress,
    audited: dict,
) -> dict:
    common = _common(source, config, report, fingerprints, validation, code)
    canonical, datasets = _canonical_inputs(
        source, old_folder, folder, config, common, fmt, progress, audited
    )
    references = {name: item.reference() for name, item in canonical.items()}
    atomic_json(
        folder / "canonical_inputs.json",
        dict(
            version=STAGED_VERSION,
            status="VALIDATED_CANONICAL_INPUTS_COMMITTED",
            source_id=source.id,
            selected_format=fmt,
            raw_fidelity_proof_sha256=proof_sha,
            reused_normalization_run=str(old_folder),
            tables=references,
            committed_at=utc(),
        ),
    )
    reference_sha = sha256(folder / "canonical_inputs.json")
    # 모든 canonical 입력이 커밋된 뒤에만 ML Parquet 파생을 시작합니다.
    ml_metadata = dict(common, purpose="ml", scope="FULL_SOURCE", labels_separate=True)
    for name, item in canonical.items():
        manifest = _derive(name, item, folder / "ml" / name, ml_metadata, reference_sha, progress)
        datasets["ml_" + name] = dict(path="ml/" + name, rows=manifest["rows"])
    result = deepcopy(read_metadata(old_folder / "source_manifest.json"))
    result.update(
        status="VERIFIED",
        purpose="all" if source.id == "ibm_hi_small" else "ml",
        datasets=datasets,
        ml_output_rows=common["raw_input_rows"],
        normalization_reuse=dict(
            run=str(old_folder),
            source_manifest_sha256=sha256(old_folder / "source_manifest.json"),
            raw_fidelity_proof_sha256=proof_sha,
            source_reparsed=False,
        ),
        staged_format_version=STAGED_VERSION,
        canonical_format=fmt,
        canonical_inputs=dict(path="canonical_inputs.json", sha256=reference_sha),
        training_executed=False,
        source_copy_accepted=False,
    )
    if source.id == "ibm_hi_small":
        require(
            config.service is not None and config.service.mode == "full_source",
            "Explicit full-source HI service configuration is required; default sample is insufficient",
        )
        service_datasets, service_result = _service(source, canonical, folder, common, progress)
        result["datasets"].update(service_datasets)
        result.update(service_result)
        expected = service_expectations(result)
        atomic_json(folder / "service" / "expected.json", expected)
        result["service_expected"] = dict(
            path="service/expected.json", sha256=sha256(folder / "service" / "expected.json")
        )
    for item in canonical.values():
        item.verify_metadata()
    for path, expected in fingerprints.items():
        require(sha256(Path(path)) == expected, "Original source changed during staged production")
    require(
        code_fingerprint() == code and sha256(config.path) == common["config_sha256"],
        "Implementation/configuration changed while producing data",
    )
    atomic_json(folder / "source_manifest.json", result)
    return result


def _inside(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    require(path != root and path.is_relative_to(root), "Staged source escapes run")
    return path


def _verify_references(folder: Path, source: dict, progress: Progress) -> None:
    reference = folder / "canonical_inputs.json"
    require(
        sha256(reference) == source["canonical_inputs"]["sha256"], "Canonical reference changed"
    )
    document = read_metadata(reference)
    for role, item in document["tables"].items():
        table_folder = Path(item["folder"]).resolve()
        manifest = read_metadata(table_folder / "manifest.json")
        schema = pa.ipc.read_schema(pa.BufferReader((table_folder / "schema.arrow").read_bytes()))
        captured = capture_text(
            table_folder,
            schema,
            {},
            item["format"],
            progress,
            source["source_id"] + "/verify_canonical/" + role,
        )
        require(captured.reference() == item, "Canonical input reference differs")
        derived_folder = folder / source["datasets"]["ml_" + role]["path"]
        derived = read_metadata(derived_folder / "manifest.json")
        require(
            derived["canonical_input_reference_sha256"] == sha256(reference)
            and len(derived["parts"]) == len(manifest["parts"]),
            "Derived canonical binding differs",
        )
        for index, (part, original) in enumerate(zip(derived["parts"], manifest["parts"])):
            canonical_file = next(
                f for f in original["files"] if Path(f["path"]).suffix == "." + item["format"]
            )
            require(
                part["rows"] == original["rows"]
                and part["logical_sha256"] == original["logical_sha256"],
                "Derived canonical logical receipt differs",
            )
            require(
                part["canonical_source"]
                == dict(
                    path=str(captured.path(index)),
                    sha256=canonical_file["sha256"],
                    format=item["format"],
                    logical_sha256=original["logical_sha256"],
                ),
                "Derived canonical physical receipt differs",
            )


def execute_staged(
    config: Config,
    validation: Path,
    canonical_run: Path,
    reuse_proof: Path,
    proof_execution: Path,
    *,
    output_root: Path | None = None,
    check_snapshot: Path | None = None,
    selected: list[str] | None = None,
    canonical_formats: list[str] | None = None,
    resume: Path | None = None,
) -> tuple[Path, bool]:
    formats = _format_map(config, selected, canonical_formats)
    validation, canonical_run = validation.resolve(), canonical_run.resolve()
    output = (output_root or config.output).resolve()
    protected = [canonical_run, validation]
    protected.extend(s.path.parent.resolve() for s in config.check.sources)
    require(
        not any(output == p or output.is_relative_to(p) for p in protected),
        "New output root overlaps original or prior artifact directory",
    )
    output.mkdir(parents=True, exist_ok=True)
    require(shutil.disk_usage(output).free > 5 * 1024**3, "Less than 5 GiB free")
    old_run, proof = _proof(
        canonical_run, reuse_proof.resolve(), proof_execution.resolve(), validation
    )
    root = (
        resume.resolve()
        if resume
        else output / (datetime.now().strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex)
    )
    require(
        root != output and root.is_relative_to(output) and not root.is_relative_to(canonical_run),
        "Resume must target a new staged run inside output root",
    )
    if not resume:
        root.mkdir(exist_ok=False)
    require(root.is_dir(), "Staged resume directory is absent")
    require(
        not any(root == p or root.is_relative_to(p) for p in protected),
        "Run overlaps a protected input",
    )
    original_config = config
    config, restoration = archived_config(config, check_snapshot, root, commit=False)
    binding: dict = dict(
        config_sha256=sha256(config.path),
        code_sha256=code_fingerprint(),
        dbcheck_config_sha256=config.check.config_sha256,
        aliases_sha256=config.check.aliases_sha256,
        validation_run=str(validation),
        validation_run_sha256=sha256(validation / "run.json"),
        canonical_run=str(canonical_run),
        canonical_run_sha256=sha256(canonical_run / "run.json"),
        reuse_proof=str(reuse_proof.resolve()),
        reuse_proof_sha256=sha256(reuse_proof),
        proof_execution=str(proof_execution.resolve()),
        proof_execution_sha256=sha256(proof_execution),
        selected_sources=sorted(formats),
        canonical_formats=formats,
        service_formats=["csv", "json"],
        version=STAGED_VERSION,
        runtime=dict(python=platform.python_version(), arrow=pa.__version__),
        purpose="STAGED_CANONICAL_CSV_JSON_ML_PARQUET_AND_HI_SERVICE",
    )
    manifest: dict
    if resume:
        existing = read_metadata(root / "run.json")
        require(
            existing.get("staged_format_version") == STAGED_VERSION
            and existing["binding"] == binding,
            "Staged source/config/code/runtime/proof mismatch; resume forbidden",
        )
        existing_digest = sha256(root / "run.json")
    with RunLock(root):
        if resume:
            require(sha256(root / "run.json") == existing_digest, "Run changed before lock")
            manifest = read_metadata(root / "run.json")
        else:
            manifest = dict(
                contract_version="dajaba-assembly-v2",
                staged_format_version=STAGED_VERSION,
                status="RUNNING",
                binding=binding,
                sources={},
                started_at=utc(),
                configuration_restoration=restoration,
                training_executed=False,
                source_copy_executed=False,
            )
        config, restoration = archived_config(original_config, check_snapshot, root)
        manifest["status"] = "RUNNING"
        atomic_json(root / "run.json", manifest)
        try:
            with Progress(root) as progress:
                for source in config.check.sources:
                    if source.id not in formats:
                        continue
                    progress.emit(source.id + "/bind_original_fingerprints")
                    report, fingerprints = bind(source, config, validation)
                    old_item = old_run["sources"][source.id]
                    fidelity = proof["sources"][source.id]
                    require(
                        old_item["status"] == "VERIFIED"
                        and fidelity["fidelity"] == "PASS"
                        and fidelity["stage"] == "COMPLETE"
                        and fidelity["verified_rows"] == report["input"]["rows"]
                        and fidelity["input_fingerprints"] == fingerprints,
                        "Original source differs from the completed canonical fidelity audit",
                    )
                    require(
                        "artifact_binding" in fidelity,
                        "Completed audit lacks a frozen artifact inventory; repeat independent audit",
                    )
                    if source.accounts is not None:
                        require(
                            fidelity.get("account_values", "").startswith("PASS"),
                            "Original account values were not independently audited",
                        )
                    old_folder = _inside(canonical_run, old_item["folder"])
                    previous = manifest["sources"].get(source.id)
                    if previous and previous["status"] == "VERIFIED":
                        folder = _inside(root, previous["folder"])
                        require(
                            sha256(folder / "source_manifest.json")
                            == previous["source_manifest_sha256"],
                            "Completed staged source metadata changed",
                        )
                        _verify_references(
                            folder, read_metadata(folder / "source_manifest.json"), progress
                        )
                        verify_source(folder)
                        progress.emit(source.id + "/completed_source_reused")
                        continue
                    attempt = 1
                    folder = root / source.id / f"attempt-{attempt:02d}"
                    while folder.exists():
                        attempt += 1
                        folder = root / source.id / f"attempt-{attempt:02d}"
                    folder.mkdir(parents=True, exist_ok=False)
                    try:
                        result = _build_source(
                            source,
                            config,
                            validation,
                            old_folder,
                            folder,
                            formats[source.id],
                            report,
                            fingerprints,
                            binding["code_sha256"],
                            binding["reuse_proof_sha256"],
                            progress,
                            fidelity["artifact_binding"],
                        )
                        progress.emit(source.id + "/verify_completed_source")
                        verify_source(folder)
                        _verify_references(folder, result, progress)
                        manifest["sources"][source.id] = dict(
                            status="VERIFIED",
                            folder=str(folder.relative_to(root)),
                            input_rows=result["input_rows"],
                            datasets=result["datasets"],
                            source_manifest_sha256=sha256(folder / "source_manifest.json"),
                        )
                    except Exception as error:
                        atomic_json(
                            folder / "failure.json",
                            dict(
                                status="FAILED",
                                source=source.id,
                                at=utc(),
                                error_type=type(error).__name__,
                                error=str(error),
                            ),
                        )
                        manifest["sources"][source.id] = dict(
                            status="FAILED", folder=str(folder.relative_to(root)), error=str(error)
                        )
                        raise
                    atomic_json(root / "run.json", manifest)
                require(
                    set(manifest["sources"]) == set(formats)
                    and all(v["status"] == "VERIFIED" for v in manifest["sources"].values()),
                    "Not all selected sources completed",
                )
                require(
                    code_fingerprint() == binding["code_sha256"]
                    and sha256(config.path) == binding["config_sha256"],
                    "Source implementation/config changed before final commit",
                )
                progress.emit(
                    "all_selected_sources_verified", len(formats), len(formats), "sources"
                )
            manifest.update(status="COMPLETED", finished_at=utc())
            atomic_json(root / "run.json", manifest)
            digest = sha256(root / "run.json")
            atomic_json(
                root / "assembly_index.json",
                dict(
                    version="ml-assembly-input-v1",
                    ml_runs={sid: str(root) for sid in formats},
                    run_manifest_sha256={sid: digest for sid in formats},
                    assembly_verification="SELECTED_FORMAT_READ_BACK_AND_CANONICAL_PARQUET_PARITY",
                    service_precision_status=(
                        "HI numeric(20,6) exact; no LI/SAML service generated"
                        if "ibm_hi_small" in formats
                        else "HI_SERVICE_NOT_SELECTED"
                    ),
                ),
            )
        except BaseException as error:
            manifest.update(
                status="CANCELLED" if isinstance(error, KeyboardInterrupt) else "FAILED",
                finished_at=utc(),
                error_type=type(error).__name__,
                error=str(error),
            )
            atomic_json(root / "run.json", manifest)
            raise
    return root, True


def verify_staged(root: Path) -> dict:
    root = root.resolve()
    preliminary = read_metadata(root / "run.json")
    require(
        preliminary.get("staged_format_version") == STAGED_VERSION
        and preliminary.get("status") == "COMPLETED",
        "Not a completed staged run",
    )
    require(
        preliminary["binding"]["code_sha256"] == code_fingerprint(),
        "Staged implementation fingerprint differs",
    )
    with RunLock(root):
        manifest = read_metadata(root / "run.json")
        require(manifest.get("staged_format_version") == STAGED_VERSION, "Not a staged run")
        require(
            manifest["binding"]["code_sha256"] == code_fingerprint(),
            "Staged implementation fingerprint differs",
        )
        with Progress(root / "verification") as progress:
            result = verify_run(root)
            for sid, source in result["sources"].items():
                folder = _inside(root, manifest["sources"][sid]["folder"])
                require(
                    sha256(folder / "source_manifest.json")
                    == manifest["sources"][sid]["source_manifest_sha256"],
                    "Completed staged source metadata differs",
                )
                _verify_references(folder, source, progress)
                progress.emit(sid + "/verified")
    return result


def audit_staged(
    config: Config, root: Path, output: Path, selected: list[str] | None = None
) -> AuditReport:
    from .audit import audit_run
    from .staged_contract import same_toml

    manifest = read_metadata(root / "run.json")
    require(
        manifest.get("staged_format_version") == STAGED_VERSION
        and manifest["status"] == "COMPLETED",
        "Only completed staged runs can be independently audited",
    )
    require(
        sha256(config.path) == manifest["binding"]["config_sha256"],
        "Staged assembly config changed",
    )
    snapshot = root / "provenance" / "validated_dbcheck_snapshot.toml"
    if snapshot.exists():
        data = snapshot.read_bytes()
        current = config.check.path.read_bytes()
        require(
            hashlib.sha256(current).hexdigest() == config.check.config_sha256,
            "Current DBcheck config changed after load",
        )
        require(
            hashlib.sha256(data).hexdigest() == manifest["binding"]["dbcheck_config_sha256"]
            and same_toml(
                tomllib.loads(data.decode("utf-8-sig")), tomllib.loads(current.decode("utf-8-sig"))
            ),
            "Genuine DBcheck snapshot/config semantic binding differs",
        )
        config = replace(
            config,
            check=replace(
                config.check, path=snapshot, config_sha256=hashlib.sha256(data).hexdigest()
            ),
        )
    return audit_run(config, root, output, selected)
