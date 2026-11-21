"""Bind supplied pattern members to existing ML UIDs without rebuilding any dataset."""

import hashlib
import io
import json
from bisect import bisect_left
from collections import Counter
from pathlib import Path

import pyarrow as pa
from dbcheck.config import CONTRACT_VERSION
from dbcheck.distribution import sha256
from dbcheck.patterns import BEGIN, PATTERN_CONTRACT, PATTERN_REPORT, _read_members

from .config import VERSION
from .exporting import load_payload, primary_part_path

HANDOFF_VERSION = "dajaba-assembly-pattern-handoff-v1"
PATTERN_TYPE_CONTRACT = "dajaba-pattern-type-preservation-v1"
DEFAULT_SOURCE_IDS: tuple[str, ...] = ("ibm_hi_small", "ibm_li_small", "saml_d")
SOURCE_IDS = (*DEFAULT_SOURCE_IDS, "ibm_hi_medium", "ibm_li_medium")
ROW_CONVENTION = "ZERO_BASED_CSV_RECORD_EXCLUDING_HEADER"
UNRESOLVED_REASONS = {
    "NO_EXACT_SOURCE_ROW": "unmatched_pattern_rows",
    "MULTIPLE_EXACT_SOURCE_ROWS": "multiple_match_pattern_rows",
    "SOURCE_ROW_REFERENCED_BY_MULTIPLE_PATTERN_MEMBERS": "reused_source_row_pattern_rows",
    "GROUP_NOT_FULLY_VERIFIED": "other_rows_in_blocked_groups",
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _inside(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    _require(path != root.resolve() and path.is_relative_to(root.resolve()), "Unsafe input path")
    return path


def _integer(value, minimum=0) -> bool:
    return type(value) is int and value >= minimum


class _Evidence:
    """Fingerprint the actual bytes consumed, including every referenced data part."""

    def __init__(self):
        self.files: dict[str, dict] = {}

    def read(self, path: Path, expected: str | None = None) -> bytes:
        path = path.resolve()
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        _require(expected is None or digest == expected, f"Fingerprint mismatch: {path}")
        receipt = dict(path=str(path), sha256=digest, bytes=len(data))
        previous = self.files.setdefault(str(path), receipt)
        _require(previous == receipt, f"Input changed while reading: {path}")
        return data

    def json(self, path: Path, expected: str | None = None) -> dict:
        value = json.loads(self.read(path, expected).decode("utf-8-sig"))
        _require(isinstance(value, dict), f"Expected a JSON object: {path}")
        return value

    def unchanged(self) -> None:
        for item in self.files.values():
            _require(sha256(Path(item["path"])) == item["sha256"], "Handoff input changed")


def _members(payload: dict, source: dict) -> list[dict]:
    """Reject ambiguous report structure; retain DBCheck's unresolved groups verbatim."""
    status = payload["status"]
    if source["source_id"] == "saml_d":
        _require(
            status == "NOT_EVALUABLE"
            and payload["reason"] == "SAML_NO_SUPPLIED_PATTERNS_OR_VERIFIED_EVENT_ID"
            and payload["patterns"] is None
            and payload["match_policy"] is None
            and payload["source_fields"]["source_event_id_column"] is None,
            "SAML must not acquire inferred event IDs or pattern membership",
        )
    if status in {"NOT_EVALUABLE", "INVALID_PATTERN_FILE"}:
        _require(
            all(
                payload[name] is None
                for name in ("counts", "verified_links", "unresolved_pattern_rows")
            ),
            "Unevaluable patterns must not contain invented links or counts",
        )
        return []
    _require(status in {"VERIFIED", "INCOMPLETE"}, "Unsupported pattern status")
    _require(
        payload["match_policy"] == "EXACT_11_PARSED_ORIGINAL_CSV_FIELDS"
        and payload["pattern_identity"] == "PATTERNS_SHA256_AND_ZERO_BASED_SUPPLIED_GROUP_ORDINAL"
        and payload["pattern_line_convention"] == "ONE_BASED_PATTERN_FILE_LINE"
        and payload["patterns"] is not None,
        "Pattern matching/identity contract differs",
    )
    links = payload["verified_links"]
    unresolved = payload["unresolved_pattern_rows"]
    counts = payload["counts"]
    _require(isinstance(links, list) and isinstance(unresolved, list), "Invalid pattern rows")
    _require(
        isinstance(counts, dict) and all(_integer(v) for v in counts.values()), "Invalid counts"
    )
    lines, row_ids, group_starts = set(), set(), {}
    verified_groups, blocked_groups = set(), set()
    failures: Counter[str] = Counter()
    for rows, verified in ((links, True), (unresolved, False)):
        for row in rows:
            group, begin, line = (
                row["pattern_group_ordinal"],
                row["pattern_group_begin_line"],
                row["pattern_line_number"],
            )
            _require(
                _integer(group) and _integer(begin, 1) and _integer(line, 1) and begin < line,
                "Invalid supplied pattern location",
            )
            _require(line not in lines, "Repeated pattern line")
            lines.add(line)
            _require(group_starts.setdefault(group, begin) == begin, "Inconsistent pattern group")
            if verified:
                row_id = row["source_row_id"]
                _require(
                    _integer(row_id) and row_id < source["input_rows"] and row_id not in row_ids,
                    "Repeated or out-of-range verified source row",
                )
                row_ids.add(row_id)
                verified_groups.add(group)
            else:
                _require(
                    row["reason"] in UNRESOLVED_REASONS
                    and _integer(row["exact_candidate_rows"])
                    and "source_row_id" not in row,
                    "Unresolved pattern must not claim a unique source row",
                )
                blocked_groups.add(group)
                failures[row["reason"]] += 1
    _require(not verified_groups & blocked_groups, "Partly verified pattern group")
    _require(
        counts["pattern_groups"] > 0
        and counts["pattern_rows"] > 0
        and sorted(group_starts) == list(range(counts["pattern_groups"]))
        and counts["pattern_rows"] == len(lines)
        and counts["verified_pattern_rows"] == len(links)
        and counts["verified_pattern_groups"] == len(verified_groups)
        and counts["blocked_pattern_groups"] == len(blocked_groups)
        and all(counts[name] == failures[reason] for reason, name in UNRESOLVED_REASONS.items())
        and counts["verified_positive_source_rows"] <= len(links)
        and counts["verified_positive_source_rows"]
        + counts["positive_source_rows_without_verified_pattern"]
        == source["positive_rows"]
        and (status == "VERIFIED") == (not blocked_groups)
        and payload["reason"]
        == ("ONE_OR_MORE_PATTERN_GROUPS_NOT_VERIFIED" if blocked_groups else None),
        "Pattern counts/status do not reconcile",
    )
    return links


def _bind_source(sid: str, assembly: Path, validation: Path, runs: tuple, evidence: _Evidence):
    assembly_run, check_run = runs
    entry = assembly_run["sources"][sid]
    _require(entry["status"] == "VERIFIED", "Assembly source is incomplete")
    folder = _inside(assembly, entry["folder"])
    source = evidence.json(folder / "source_manifest.json", entry.get("source_manifest_sha256"))
    _require(
        source["source_id"] == sid
        and source["status"] == "VERIFIED"
        and _integer(source["input_rows"], 1)
        and _integer(source["positive_rows"])
        and source["input_rows"] == entry["input_rows"]
        and source["datasets"] == entry["datasets"],
        "Assembly source index differs",
    )
    report = evidence.json(validation / sid / "validation_report.json")
    raw = report["input"]
    entries = [item for item in check_run["sources"] if item["id"] == sid]
    _require(
        len(entries) == 1
        and entries[0]["scan_complete"] is True
        and entries[0]["status"] == report["status"]
        and entries[0]["sha256"] == raw["sha256"]
        and entries[0]["rows"] == raw["rows"]
        and report["source_id"] == sid
        and report["contract_version"] == CONTRACT_VERSION
        and report["status"] in {"PASS", "WARN"}
        and report["full_scan_complete"] is True
        and report["source_unchanged"] is True
        and report["scope"] == "FULL_ORIGINAL"
        and not any(rule["status"] == "FAIL" for rule in report["checks"]),
        "DBCheck source is incomplete or differs from its run",
    )
    fingerprints = {
        str(Path(p).resolve()): digest for p, digest in source["input_fingerprints"].items()
    }
    for item in [raw, *report["auxiliary_inputs"]]:
        _require(
            fingerprints.get(str(Path(item["path"]).resolve())) == item["sha256"],
            "DBCheck and assembly original fingerprints differ",
        )
    _require(
        source["source_release"] == f"{sid}:{raw['sha256']}"
        and source["input_rows"] == raw["rows"],
        "DBCheck and assembly source release/count differ",
    )
    ref = report.get("pattern_membership")
    if not isinstance(ref, dict):
        raise ValueError("DBCheck has no pattern handoff; run the updated DBCheck first")
    _require(
        ref["path"] == PATTERN_REPORT and ref["contract_version"] == PATTERN_CONTRACT,
        "Unsupported pattern report reference",
    )
    pattern_path = validation / sid / PATTERN_REPORT
    payload = evidence.json(pattern_path, ref["sha256"])
    _require(
        payload["contract_version"] == PATTERN_CONTRACT
        and payload["source_id"] == sid
        and payload["source_release"] == source["source_release"]
        and payload["source_csv"] == raw
        and payload["status"] == ref["status"]
        and payload["reason"] == ref["reason"]
        and payload["scope"] == "FULL_ORIGINAL"
        and payload["source_row_id_convention"] == ROW_CONVENTION
        and payload["transaction_uid_format"] == "{source_release}:{source_row_id}",
        "Pattern report is not bound to this source/row contract",
    )
    patterns = [item for item in report["auxiliary_inputs"] if item["role"] == "patterns"]
    _require(
        [dict(path=item["path"], sha256=item["sha256"]) for item in patterns]
        == ([payload["patterns"]] if payload["patterns"] is not None else []),
        "Pattern original fingerprint differs",
    )
    return folder, source, report, payload, pattern_path


def _map_uids(folder: Path, source: dict, links: list[dict], evidence: _Evidence) -> list[dict]:
    if not links:
        return []
    item = source["datasets"].get("ml_transactions")
    if item is None:
        raise ValueError("Pattern handoff requires full-source ML transactions")
    _require(
        item["rows"] == source["input_rows"] and source["ml_output_rows"] == source["input_rows"],
        "Pattern handoff requires full-source ML transactions; a service sample is insufficient",
    )
    dataset = _inside(folder, item["path"])
    meta = evidence.json(dataset / "manifest.json")
    _require(
        meta["status"] == "VERIFIED"
        and meta["contract_version"] == VERSION
        and meta["source_id"] == source["source_id"]
        and meta["source_release"] == source["source_release"]
        and meta["scope"] == "FULL_SOURCE"
        and meta["table"] == "transactions"
        and meta["rows"] == source["input_rows"],
        "ML transaction manifest differs from source",
    )
    schema = pa.ipc.read_schema(pa.BufferReader(evidence.read(dataset / "schema.arrow")))
    _require(
        schema.field("source_row_id").type == pa.int64()
        and schema.field("transaction_uid").type == pa.string(),
        "ML source row/UID schema differs",
    )
    wanted = sorted(row["source_row_id"] for row in links)
    found, offset, paths = {}, 0, set()
    for part in meta["parts"]:
        size = part["rows"]
        _require(_integer(size, 1), "Invalid ML part row count")
        path = primary_part_path(dataset, part)
        _require(path not in paths, "Repeated ML part")
        paths.add(path)
        first, last = bisect_left(wanted, offset), bisect_left(wanted, offset + size)
        if first != last:
            receipt = next(f for f in part["files"] if _inside(dataset, f["path"]) == path)
            data = evidence.read(path, receipt["sha256"])
            table, _ = load_payload(path, schema, data=data)
            _require(
                table.schema.equals(schema) and len(table) == size, "ML part schema/count differs"
            )
            row_ids = table.column("source_row_id").to_pylist()
            uids = table.column("transaction_uid").to_pylist()
            _require(
                row_ids == list(range(offset, offset + size))
                and uids == [f"{source['source_release']}:{row_id}" for row_id in row_ids],
                "ML source row order/transaction UID differs; no inferred repair is allowed",
            )
            for row_id in wanted[first:last]:
                found[row_id] = uids[row_id - offset]
        offset += size
    _require(offset == meta["rows"] and len(found) == len(wanted), "ML pattern members are missing")
    return [
        dict(
            row,
            transaction_uid=found[row["source_row_id"]],
            source_row_number=row["source_row_id"] + 1,
        )
        for row in links
    ]


def _with_pattern_types(payload: dict, links: list[dict], evidence: _Evidence):
    """동일 해시의 원문 그룹 위치에서 유형과 상세 헤더를 보존합니다. 룰로 추정하지 않습니다."""
    unresolved = payload["unresolved_pattern_rows"]
    if payload["status"] not in {"VERIFIED", "INCOMPLETE"}:
        return links, unresolved
    reference = payload["patterns"]
    data = evidence.read(Path(reference["path"]), reference["sha256"])
    members = _read_members(data)
    headers = [line.rstrip("\r\n") for line in io.StringIO(data.decode("utf-8-sig"), newline="")]
    actual = {
        (member.group_ordinal, member.group_begin_line, member.line_number)
        for member in members
    }

    def position(row):
        return (
            row["pattern_group_ordinal"], row["pattern_group_begin_line"],
            row["pattern_line_number"],
        )

    _require(
        {position(row) for row in [*links, *unresolved]} == actual,
        "DBCheck pattern locations differ from the fingerprinted original",
    )

    def annotate(rows):
        result = []
        for row in rows:
            header = headers[row["pattern_group_begin_line"] - 1]
            result.append(dict(
                row,
                pattern_type=header[len(BEGIN) :].partition(":")[0].strip(),
                pattern_group_header_raw=header,
            ))
        return result

    return annotate(links), annotate(unresolved)


def _collect(assembly: Path, validation: Path, selected: list[str], *, include_pattern_types=True):
    _require(
        bool(selected)
        and len(set(selected)) == len(selected)
        and not set(selected) - set(SOURCE_IDS),
        "Only distinct supported HI/LI Small/Medium and SAML sources are allowed",
    )
    evidence = _Evidence()
    assembly_run = evidence.json(assembly / "run.json")
    check_run = evidence.json(validation / "run.json")
    _require(
        assembly_run["contract_version"] == VERSION
        and check_run["contract_version"] == CONTRACT_VERSION
        and assembly_run["status"] in {"COMPLETED", "FAILED"}
        and check_run["status"] in {"COMPLETED", "FAILED"},
        "Unsupported or unfinished input run",
    )
    results, snapshots, protected = {}, {}, [assembly, validation]
    for sid in selected:
        folder, source, report, payload, pattern_path = _bind_source(
            sid, assembly, validation, (assembly_run, check_run), evidence
        )
        protected.extend(
            Path(item["path"]).resolve().parent
            for item in [report["input"], *report["auxiliary_inputs"]]
        )
        links = _members(payload, source)
        unresolved = payload["unresolved_pattern_rows"]
        if include_pattern_types:
            links, unresolved = _with_pattern_types(payload, links, evidence)
        links = _map_uids(folder, source, links, evidence)
        results[sid] = dict(
            contract_version=HANDOFF_VERSION,
            source_id=sid,
            source_release=source["source_release"],
            source_csv=payload["source_csv"],
            patterns=payload["patterns"],
            status=payload["status"],
            reason=payload["reason"],
            pattern_identity=payload["pattern_identity"],
            source_fields=payload["source_fields"],
            counts=payload["counts"],
            mapped_pattern_rows=len(links),
            verified_links=links,
            unresolved_pattern_rows=unresolved,
            dbcheck_report=dict(
                path="dbcheck_pattern_membership.json", sha256=sha256(pattern_path)
            ),
            use="OFFLINE_SUPPLIED_PATTERN_MEMBERSHIP_AUDIT_ONLY; never inference features",
            source_row_id_convention=ROW_CONVENTION,
            source_row_number_convention="ONE_BASED_CSV_RECORD_EXCLUDING_HEADER",
            assembly_check_scope="PATTERN_REFERENCED_ML_PARTS_ONLY",
            full_assembly_reverified=False,
            backend_transaction_ids="NOT_PROVIDED; source rows and ML UIDs are not backend IDs",
            db_write_executed=False,
            training_executed=False,
        )
        if include_pattern_types:
            results[sid].update(
                pattern_type_contract_version=PATTERN_TYPE_CONTRACT,
                pattern_types_preserved=payload["status"] in {"VERIFIED", "INCOMPLETE"},
            )
        snapshots[sid] = evidence.read(pattern_path, results[sid]["dbcheck_report"]["sha256"])
    evidence.unchanged()
    return results, snapshots, list(evidence.files.values()), protected


def _write_new(path: Path, data: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(data)


def _json_bytes(value: dict) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")


def create_handoff(assembly: Path, validation: Path, output: Path, selected=None) -> dict:
    assembly, validation, output = assembly.resolve(), validation.resolve(), output.resolve()
    _require(not output.exists(), "Handoff output already exists; choose a new directory")
    selected = list(DEFAULT_SOURCE_IDS) if selected is None else selected
    results, snapshots, evidence, protected = _collect(assembly, validation, selected)
    _require(
        not any(output == path or output.is_relative_to(path) for path in protected),
        "Handoff output must be outside original data and existing input runs",
    )
    output.mkdir(parents=True, exist_ok=False)
    sources = {}
    for sid, result in results.items():
        folder = output / sid
        folder.mkdir()
        _write_new(folder / "dbcheck_pattern_membership.json", snapshots[sid])
        path = folder / "pattern_handoff.json"
        _write_new(path, _json_bytes(result))
        sources[sid] = dict(
            path=f"{sid}/pattern_handoff.json", sha256=sha256(path), status=result["status"]
        )
    index = dict(
        contract_version=HANDOFF_VERSION,
        pattern_type_contract_version=PATTERN_TYPE_CONTRACT,
        status="RECORDED",
        assembly_run=str(assembly),
        validation_run=str(validation),
        selected_sources=selected,
        sources=sources,
        input_evidence=evidence,
        event_membership_complete=all(r["status"] == "VERIFIED" for r in results.values()),
        training_executed=False,
        db_write_executed=False,
    )
    _write_new(output / "handoff.json", _json_bytes(index))
    return index


def verify_handoff(output: Path) -> dict:
    output = output.resolve()
    index = json.loads((output / "handoff.json").read_text(encoding="utf-8"))
    _require(
        index["contract_version"] == HANDOFF_VERSION and index["status"] == "RECORDED",
        "Incomplete or unsupported pattern handoff",
    )
    _require(
        "pattern_type_contract_version" not in index
        or index["pattern_type_contract_version"] == PATTERN_TYPE_CONTRACT,
        "Unsupported pattern type preservation contract",
    )
    results, snapshots, evidence, _ = _collect(
        Path(index["assembly_run"]), Path(index["validation_run"]), index["selected_sources"],
        include_pattern_types="pattern_type_contract_version" in index,
    )
    _require(
        index["input_evidence"] == evidence and set(index["sources"]) == set(results),
        "Handoff input evidence/source set changed",
    )
    _require(
        index["event_membership_complete"]
        == all(r["status"] == "VERIFIED" for r in results.values())
        and index["training_executed"] is False
        and index["db_write_executed"] is False,
        "Handoff scope/status changed",
    )
    for sid, expected in results.items():
        item = index["sources"][sid]
        _require(
            item["path"] == f"{sid}/pattern_handoff.json" and item["status"] == expected["status"],
            "Handoff source reference changed",
        )
        path = _inside(output, item["path"])
        data = path.read_bytes()
        _require(
            hashlib.sha256(data).hexdigest() == item["sha256"] and json.loads(data) == expected,
            "Handoff links/status changed",
        )
        _require(
            (path.parent / "dbcheck_pattern_membership.json").read_bytes() == snapshots[sid],
            "Preserved DBCheck evidence changed",
        )
    return dict(
        status="VERIFIED",
        event_membership_complete=index["event_membership_complete"],
        sources={sid: result["status"] for sid, result in results.items()},
    )
