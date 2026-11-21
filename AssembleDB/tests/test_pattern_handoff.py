"""Small source-backed artifacts exercise the handoff without a full-source rebuild."""

import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dbcheck.config import CONTRACT_VERSION, Source
from dbcheck.distribution import IBM, SAML, sha256
from dbcheck.patterns import PATTERN_CONTRACT, PATTERN_REPORT, PatternMembership

from assemble_db.cli import main
from assemble_db.config import VERSION
from assemble_db.exporting import DatasetWriter
from assemble_db.normalize import normalize
from assemble_db.pattern_handoff import (
    DEFAULT_SOURCE_IDS,
    PATTERN_TYPE_CONTRACT,
    create_handoff,
    verify_handoff,
)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def ibm_row(minute, label="1"):
    return [
        f"2022/09/01 00:{minute:02d}", "001", "000A", "2", "000B",
        "1.20", "US Dollar", "1.2300", "US Dollar", "Wire", label,
    ]


def make_artifacts(
    tmp_path, *, rows=None, groups=None, formats=("parquet",), source_ids=DEFAULT_SOURCE_IDS,
    group_headers=None,
):
    """Use the real pattern parser, normalization and writer; only run metadata is a fixture."""
    raw, validation, assembly = [tmp_path / name for name in ("raw", "check", "assembly")]
    for folder in (raw, validation, assembly):
        folder.mkdir()
    rows = rows if rows is not None else [ibm_row(i) for i in range(3)]
    groups = groups if groups is not None else [[rows[0], rows[2]], [rows[1]]]
    group_headers = group_headers if group_headers is not None else ["FLOW"] * len(groups)
    check_sources, assembly_sources = [], {}
    for sid in source_ids:
        saml = sid == "saml_d"
        records = ([
            ["12:00:00", "2022-01-01", "000A", "000B", "3469.0028040888333",
             "US Dollar", "Euro", "UK", "USA", "ACH", "1", "Layering"]
        ] if saml else rows)
        path = raw / f"{sid}.csv"
        header = SAML if saml else IBM
        with path.open("w", encoding="utf-8", newline="") as stream:
            raw_writer = csv.writer(stream)
            raw_writer.writerow(header)
            raw_writer.writerows(records)
        patterns = None
        if not saml:
            patterns = raw / f"{sid}_patterns.txt"
            with patterns.open("w", encoding="utf-8", newline="") as stream:
                for group, group_header in zip(groups, group_headers, strict=True):
                    stream.write(f"BEGIN LAUNDERING ATTEMPT - {group_header}\n")
                    csv.writer(stream).writerows(group)
                    name = group_header.partition(":")[0].strip()
                    stream.write(f"END LAUNDERING ATTEMPT - {name}\n")
        source = Source(
            sid, "saml" if saml else "ibm", path,
            "SOURCE_DESIGNATED_SUSPICIOUS" if saml else "AMLWORLD_ILLICIT_FLOW",
            patterns=patterns, amount_scale=16 if saml else 12,
        )
        digest, release = sha256(path), f"{sid}:{sha256(path)}"
        columns = [pa.array(values, type=pa.string()) for values in zip(*records)]
        batch = pa.RecordBatch.from_arrays(columns, names=[f"c{i}" for i in range(len(header))])
        membership = PatternMembership(source)
        membership.observe(columns, 0)
        payload = membership.report(source, dict(
            sha256=digest, rows=len(records), raw_header=header,
            timestamp_precision="SECOND" if saml else "MINUTE", source_timezone="UNKNOWN",
            label_semantics=source.label_semantics,
            labels={"1": sum(row[10] == "1" for row in records)},
        ))
        check_folder, folder = validation / sid, assembly / sid / "attempt-01"
        check_folder.mkdir()
        folder.mkdir(parents=True)
        write(check_folder / PATTERN_REPORT, payload)
        auxiliary = []
        if patterns is not None:
            auxiliary.append(dict(role="patterns", path=str(patterns), sha256=sha256(patterns)))
        report = dict(
            contract_version=CONTRACT_VERSION, status="WARN", source_id=sid,
            full_scan_complete=True, source_unchanged=True, scope="FULL_ORIGINAL",
            input=dict(path=str(path), sha256=digest, rows=len(records)),
            auxiliary_inputs=auxiliary,
            checks=[dict(rule="EXPECTED_TIME_RANGE", status="WARN")],
            pattern_membership=dict(contract_version=PATTERN_CONTRACT, path=PATTERN_REPORT,
                                    sha256=sha256(check_folder / PATTERN_REPORT),
                                    status=payload["status"], reason=payload["reason"]),
        )
        write(check_folder / "validation_report.json", report)
        check_sources.append(dict(id=sid, status="WARN", scan_complete=True,
                                  sha256=digest, rows=len(records)))
        tx, _ = normalize(source, digest, 0, batch, {"US Dollar": "USD", "Euro": "EUR"})
        writer = DatasetWriter(folder / "ml" / "transactions", tx.schema, dict(
            contract_version=VERSION, source_id=sid, source_release=release,
            scope="FULL_SOURCE", table="transactions",
        ), formats=formats)
        for offset in range(0, len(tx), 2):
            writer.write(tx.slice(offset, 2))
        writer.finish()
        datasets = dict(ml_transactions=dict(path="ml/transactions", rows=len(records)))
        fingerprints = {str(path): digest}
        if patterns is not None:
            fingerprints[str(patterns)] = sha256(patterns)
        write(folder / "source_manifest.json", dict(
            status="VERIFIED", source_id=sid, source_release=release,
            input_rows=len(records), positive_rows=sum(row[10] == "1" for row in records),
            ml_output_rows=len(records), input_fingerprints=fingerprints, datasets=datasets,
        ))
        assembly_sources[sid] = dict(status="VERIFIED", folder=f"{sid}/attempt-01",
                                     input_rows=len(records), datasets=datasets)
    write(validation / "run.json", dict(contract_version=CONTRACT_VERSION,
                                       status="COMPLETED", sources=check_sources))
    write(assembly / "run.json", dict(contract_version=VERSION, status="COMPLETED",
                                     sources=assembly_sources))
    return assembly, validation, tmp_path / "handoff"


def rebind_pattern(validation, mutate, sid="ibm_hi_small"):
    path = validation / sid / PATTERN_REPORT
    payload = read(path)
    mutate(payload)
    write(path, payload)
    report_path = path.with_name("validation_report.json")
    report = read(report_path)
    report["pattern_membership"].update(sha256=sha256(path), status=payload["status"],
                                        reason=payload["reason"])
    write(report_path, report)


@pytest.mark.parametrize("formats", [("parquet",), None, ("csv", "json")])
def test_preserves_sources_and_maps_actual_uids_across_part_boundaries(tmp_path, formats):
    assembly, validation, output = make_artifacts(tmp_path, formats=formats)
    before = {path: sha256(path) for path in tmp_path.rglob("*") if path.is_file()}
    index = create_handoff(assembly, validation, output)
    assert index["status"] == "RECORDED" and not index["event_membership_complete"]
    assert verify_handoff(output)["sources"] == {
        "ibm_hi_small": "VERIFIED", "ibm_li_small": "VERIFIED", "saml_d": "NOT_EVALUABLE",
    }
    for sid in DEFAULT_SOURCE_IDS[:2]:
        report = read(output / sid / "pattern_handoff.json")
        assert [row["source_row_id"] for row in report["verified_links"]] == [0, 2, 1]
        for row in report["verified_links"]:
            assert row["transaction_uid"] == f"{report['source_release']}:{row['source_row_id']}"
            assert row["source_row_number"] == row["source_row_id"] + 1
            assert "transaction_id" not in row and "event_id" not in row
        assert (output / sid / "dbcheck_pattern_membership.json").read_bytes() == (
            validation / sid / PATTERN_REPORT
        ).read_bytes()
    saml = read(output / "saml_d" / "pattern_handoff.json")
    assert saml["counts"] is None and saml["verified_links"] == []
    assert saml["unresolved_pattern_rows"] is None and saml["patterns"] is None
    assert saml["pattern_types_preserved"] is False
    assert {path: sha256(path) for path in before} == before


def test_unmatched_ambiguous_and_other_members_of_blocked_group_are_retained(tmp_path):
    a, b, c, absent = [ibm_row(i) for i in range(4)]
    assembly, validation, output = make_artifacts(
        tmp_path, rows=[a, b, b, c], groups=[[a], [b, c], [absent]]
    )
    create_handoff(assembly, validation, output)
    report = read(output / "ibm_hi_small" / "pattern_handoff.json")
    original = read(validation / "ibm_hi_small" / PATTERN_REPORT)
    assert report["status"] == "INCOMPLETE"
    assert [row["source_row_id"] for row in report["verified_links"]] == [0]
    assert [
        {key: value for key, value in row.items()
         if key not in {"pattern_type", "pattern_group_header_raw"}}
        for row in report["unresolved_pattern_rows"]
    ] == original["unresolved_pattern_rows"]
    assert all(row["pattern_type"] == "FLOW" for row in report["unresolved_pattern_rows"])
    assert all("transaction_uid" not in row for row in report["unresolved_pattern_rows"])
    assert {row["reason"] for row in report["unresolved_pattern_rows"]} == {
        "MULTIPLE_EXACT_SOURCE_ROWS", "GROUP_NOT_FULLY_VERIFIED", "NO_EXACT_SOURCE_ROW",
    }
    assert verify_handoff(output)["status"] == "VERIFIED"


def test_reused_source_member_does_not_become_two_events(tmp_path):
    a, b = ibm_row(0), ibm_row(1)
    assembly, validation, output = make_artifacts(tmp_path, rows=[a, b], groups=[[a], [a, b]])
    create_handoff(assembly, validation, output)
    report = read(output / "ibm_hi_small" / "pattern_handoff.json")
    assert report["status"] == "INCOMPLETE" and report["verified_links"] == []
    assert report["counts"]["reused_source_row_pattern_rows"] == 2


@pytest.mark.parametrize(
    "target", ["pattern", "release", "missing_report", "repeated_line", "partial_group", "saml"]
)
def test_invalid_handoff_evidence_is_rejected_without_output(tmp_path, target):
    assembly, validation, output = make_artifacts(tmp_path)
    sid = "ibm_hi_small"
    if target == "pattern":
        with (validation / sid / PATTERN_REPORT).open("a", encoding="utf-8") as stream:
            stream.write(" ")
    elif target == "release":
        rebind_pattern(validation, mutate=lambda p: p.update(source_release="another:release"))
    elif target == "repeated_line":
        def mutate(payload):
            line = payload["verified_links"][0]["pattern_line_number"]
            payload["verified_links"][2]["pattern_line_number"] = line
        rebind_pattern(validation, mutate=mutate)
    elif target == "partial_group":
        def mutate(payload):
            payload["status"] = "INCOMPLETE"
            payload["reason"] = "ONE_OR_MORE_PATTERN_GROUPS_NOT_VERIFIED"
            payload["unresolved_pattern_rows"].append(dict(
                pattern_group_ordinal=0, pattern_group_begin_line=1, pattern_line_number=100,
                reason="NO_EXACT_SOURCE_ROW", exact_candidate_rows=0,
            ))
        rebind_pattern(validation, mutate=mutate)
    elif target == "missing_report":
        path = validation / sid / "validation_report.json"
        report = read(path)
        del report["pattern_membership"]
        write(path, report)
    else:
        rebind_pattern(
            validation, lambda p: p.update(status="VERIFIED", verified_links=[]), sid="saml_d"
        )
    with pytest.raises(ValueError):
        create_handoff(assembly, validation, output)
    assert not output.exists()


@pytest.mark.parametrize("column,value", [("transaction_uid", "backend:1"), ("source_row_id", 100)])
@pytest.mark.parametrize("sid", ["ibm_hi_small", "ibm_hi_medium", "ibm_li_medium"])
def test_rehashed_part_with_wrong_identity_is_rejected(tmp_path, column, value, sid):
    assembly, validation, output = make_artifacts(tmp_path, source_ids=(sid,))
    folder = assembly / sid / "attempt-01" / "ml" / "transactions"
    path = folder / "part-000000.parquet"
    table = pq.read_table(path)
    values = table.column(column).to_pylist()
    values[0] = value
    table = table.set_column(table.schema.get_field_index(column), column,
                             pa.array(values, type=table.schema.field(column).type))
    pq.write_table(table, path)
    manifest_path = folder / "manifest.json"
    meta = read(manifest_path)
    meta["parts"][0]["files"][0]["sha256"] = sha256(path)
    write(manifest_path, meta)
    with pytest.raises(ValueError, match="row order/transaction UID"):
        create_handoff(assembly, validation, output, [sid])
    assert not output.exists()


@pytest.mark.parametrize("target", ["output", "snapshot", "input"])
def test_verify_detects_changed_links_or_evidence(tmp_path, target):
    assembly, validation, output = make_artifacts(tmp_path)
    create_handoff(assembly, validation, output)
    if target == "input":
        path = validation / "ibm_hi_small" / "validation_report.json"
    elif target == "snapshot":
        path = output / "ibm_hi_small" / "dbcheck_pattern_membership.json"
    else:
        path = output / "ibm_hi_small" / "pattern_handoff.json"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(" ")
    with pytest.raises(ValueError):
        verify_handoff(output)


def test_originals_existing_outputs_and_unknown_sources_are_not_write_targets(tmp_path):
    assembly, validation, output = make_artifacts(tmp_path)
    for forbidden in (assembly / "new", validation / "new", tmp_path / "raw" / "new"):
        with pytest.raises(ValueError, match="outside"):
            create_handoff(assembly, validation, forbidden)
        assert not forbidden.exists()
    with pytest.raises(ValueError, match="supported"):
        create_handoff(assembly, validation, output, ["ibm_hi_large"])
    output.mkdir()
    (output / "keep.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(ValueError, match="already exists"):
        create_handoff(assembly, validation, output)
    assert (output / "keep.txt").read_text(encoding="utf-8") == "preserve"


def test_cli_reports_unevaluable_saml_without_claiming_complete_membership(tmp_path, capsys):
    assembly, validation, output = make_artifacts(tmp_path)
    assert main(["pattern-handoff", "--run", str(assembly), "--validation-run", str(validation),
                 "--output", str(output)]) == 0
    text = capsys.readouterr().out
    assert "EVENT_MEMBERSHIP_COMPLETE=False" in text and "saml_d: NOT_EVALUABLE" in text
    assert main(["verify-pattern-handoff", "--run", str(output)]) == 0
    assert "PATTERN_HANDOFF=VERIFIED" in capsys.readouterr().out


def test_medium_cli_handoff_preserves_source_namespaces_and_originals(tmp_path, capsys):
    ids = ["ibm_hi_medium", "ibm_li_medium"]
    assembly, validation, output = make_artifacts(tmp_path, source_ids=ids)
    before = {path: sha256(path) for path in tmp_path.rglob("*") if path.is_file()}
    assert main([
        "pattern-handoff", "--run", str(assembly), "--validation-run", str(validation),
        "--output", str(output), "--source", ids[0], "--source", ids[1],
    ]) == 0
    assert "EVENT_MEMBERSHIP_COMPLETE=True" in capsys.readouterr().out
    index = read(output / "handoff.json")
    assert index["selected_sources"] == ids and set(index["sources"]) == set(ids)
    uids = []
    for sid in ids:
        result = read(output / sid / "pattern_handoff.json")
        original = read(validation / sid / PATTERN_REPORT)
        assert result["source_release"].startswith(sid + ":")
        assert result["patterns"] == original["patterns"]
        assert result["source_csv"] == original["source_csv"]
        assert result["counts"] == original["counts"]
        assert [row["source_row_id"] for row in result["verified_links"]] == [0, 2, 1]
        for row in result["verified_links"]:
            assert row["transaction_uid"] == f"{result['source_release']}:{row['source_row_id']}"
            assert row["source_row_number"] == row["source_row_id"] + 1
            assert "transaction_id" not in row and "event_id" not in row
        uids.append({row["transaction_uid"] for row in result["verified_links"]})
        assert (output / sid / "dbcheck_pattern_membership.json").read_bytes() == (
            validation / sid / PATTERN_REPORT
        ).read_bytes()
    assert uids[0].isdisjoint(uids[1])
    assert verify_handoff(output)["sources"] == dict.fromkeys(ids, "VERIFIED")
    assert {path: sha256(path) for path in before} == before


@pytest.mark.parametrize("target", ["release", "pattern_hash", "duplicate_selection"])
def test_medium_handoff_rejects_wrong_provenance_without_output(tmp_path, target):
    sid = "ibm_hi_medium"
    assembly, validation, output = make_artifacts(tmp_path, source_ids=(sid,))
    if target == "release":
        rebind_pattern(validation, lambda p: p.update(source_release="ibm_hi_small:wrong"), sid)
    elif target == "pattern_hash":
        rebind_pattern(validation, lambda p: p["patterns"].update(sha256="0" * 64), sid)
    selected = [sid, sid] if target == "duplicate_selection" else [sid]
    with pytest.raises(ValueError):
        create_handoff(assembly, validation, output, selected)
    assert not output.exists()


def test_default_handoff_selection_does_not_add_medium_sources(tmp_path):
    assembly, validation, output = make_artifacts(
        tmp_path, source_ids=(*DEFAULT_SOURCE_IDS, "ibm_hi_medium", "ibm_li_medium")
    )
    index = create_handoff(assembly, validation, output)
    assert index["selected_sources"] == list(DEFAULT_SOURCE_IDS)
    assert set(index["sources"]) == set(DEFAULT_SOURCE_IDS)
    assert not (output / "ibm_hi_medium").exists()


def test_medium_types_and_full_headers_are_preserved_without_reclassifying_groups(tmp_path):
    sid = "ibm_hi_medium"
    headers = ["STACK: original details: keep  001", "STACK: another independent group"]
    assembly, validation, output = make_artifacts(
        tmp_path, source_ids=(sid,), group_headers=headers
    )
    index = create_handoff(assembly, validation, output, [sid])
    assert index["pattern_type_contract_version"] == PATTERN_TYPE_CONTRACT
    report = read(output / sid / "pattern_handoff.json")
    assert report["pattern_types_preserved"] is True
    assert report["pattern_type_contract_version"] == PATTERN_TYPE_CONTRACT
    assert [row["pattern_group_ordinal"] for row in report["verified_links"]] == [0, 0, 1]
    assert [row["pattern_type"] for row in report["verified_links"]] == ["STACK"] * 3
    assert [row["pattern_group_header_raw"] for row in report["verified_links"]] == [
        "BEGIN LAUNDERING ATTEMPT - " + headers[i] for i in [0, 0, 1]
    ]
    original = read(validation / sid / PATTERN_REPORT)
    assert [
        {name: row[name] for name in original["verified_links"][0]}
        for row in report["verified_links"]
    ] == original["verified_links"]
    assert any(item["path"] == original["patterns"]["path"] for item in index["input_evidence"])
    assert verify_handoff(output)["event_membership_complete"] is True


@pytest.mark.parametrize("target", ["original_hash", "location", "output_type", "codec"])
def test_pattern_type_preservation_rejects_wrong_evidence(tmp_path, target):
    sid = "ibm_li_medium"
    assembly, validation, output = make_artifacts(tmp_path, source_ids=(sid,))
    if target == "original_hash":
        path = tmp_path / "raw" / f"{sid}_patterns.txt"
        path.write_text(path.read_text(encoding="utf-8").replace("FLOW", "STACK"), encoding="utf-8")
    elif target == "location":
        rebind_pattern(
            validation, lambda p: p["verified_links"][-1].update(pattern_line_number=100), sid
        )
    if target in {"original_hash", "location"}:
        with pytest.raises(ValueError):
            create_handoff(assembly, validation, output, [sid])
        assert not output.exists()
        return
    index = create_handoff(assembly, validation, output, [sid])
    if target == "codec":
        index["pattern_type_contract_version"] = "unknown"
    else:
        path = output / sid / "pattern_handoff.json"
        report = read(path)
        report["verified_links"][0]["pattern_type"] = "INVENTED"
        write(path, report)
        index["sources"][sid]["sha256"] = sha256(path)
    write(output / "handoff.json", index)
    with pytest.raises(ValueError):
        verify_handoff(output)


def test_legacy_handoff_without_type_metadata_keeps_its_verification_contract(tmp_path):
    assembly, validation, output = make_artifacts(tmp_path)
    index = create_handoff(assembly, validation, output)
    pattern_paths = set()
    for sid in DEFAULT_SOURCE_IDS:
        path = output / sid / "pattern_handoff.json"
        report = read(path)
        if report["patterns"] is not None:
            pattern_paths.add(report["patterns"]["path"])
        del report["pattern_type_contract_version"]
        del report["pattern_types_preserved"]
        for name in ("verified_links", "unresolved_pattern_rows"):
            for row in report[name] or []:
                row.pop("pattern_type", None)
                row.pop("pattern_group_header_raw", None)
        write(path, report)
        index["sources"][sid]["sha256"] = sha256(path)
    del index["pattern_type_contract_version"]
    index["input_evidence"] = [
        item for item in index["input_evidence"] if item["path"] not in pattern_paths
    ]
    write(output / "handoff.json", index)
    assert verify_handoff(output)["status"] == "VERIFIED"
