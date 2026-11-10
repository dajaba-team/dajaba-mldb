"""Verify supplied IBM pattern members against original CSV records."""

import csv
import hashlib
import io
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc

from .config import Source

PATTERN_CONTRACT = "dajaba-pattern-membership-v1"
PATTERN_REPORT = "pattern_membership.json"
BEGIN = "BEGIN LAUNDERING ATTEMPT - "
END = "END LAUNDERING ATTEMPT - "
IBM_MINUTE = "%Y/%m/%d %H:%M"


@dataclass(frozen=True)
class _Member:
    group_ordinal: int
    group_begin_line: int
    line_number: int
    fields: tuple[str, ...]


def _read_members(data: bytes) -> list[_Member]:
    members = []
    active = None
    ordinal = -1
    group_begin = group_start = 0
    with io.StringIO(data.decode("utf-8-sig"), newline="") as stream:
        for number, text in enumerate(stream, 1):
            line = text.rstrip("\r\n")
            if not line.strip():
                continue
            if line.startswith(BEGIN):
                name = line[len(BEGIN) :].partition(":")[0].strip()
                if active is not None or not name:
                    raise ValueError(f"Invalid pattern group start at line {number}")
                active = name
                ordinal += 1
                group_begin, group_start = number, len(members)
            elif line.startswith(END):
                if active is None or line[len(END) :].strip() != active:
                    raise ValueError(f"Unmatched pattern group end at line {number}")
                if group_start == len(members):
                    raise ValueError(f"Empty pattern group starting at line {group_begin}")
                active = None
            else:
                try:
                    fields = tuple(next(csv.reader([line], strict=True)))
                    if active is None or len(fields) != 11 or fields[-1] not in {"0", "1"}:
                        raise ValueError("Expected 11 original fields and a binary source label")
                    if datetime.strptime(fields[0], IBM_MINUTE).strftime(IBM_MINUTE) != fields[0]:
                        raise ValueError("Expected the original IBM minute timestamp format")
                except (ValueError, csv.Error) as exc:
                    raise ValueError(f"Invalid IBM pattern member at line {number}") from exc
                members.append(_Member(ordinal, group_begin, number, fields))
    if active is not None:
        raise ValueError(f"Unclosed pattern group starting at line {group_begin}")
    if not members:
        raise ValueError("The pattern file contains no members")
    return members


class PatternMembership:
    """Collect matches during the existing Arrow scan, before any value transformation."""

    def __init__(self, source: Source):
        self.pattern_path = source.patterns if source.kind == "ibm" else None
        self.pattern_sha256: str | None = None
        self._format_error: str | None = None
        self._members: list[_Member] = []
        self._references: Counter[tuple[str, ...]] = Counter()
        self._hits: dict[tuple[str, ...], tuple[int, int]] = {}
        self._timestamps: pa.Array | None = None
        if self.pattern_path is not None and self.pattern_path.is_file():
            data = self.pattern_path.read_bytes()
            self.pattern_sha256 = hashlib.sha256(data).hexdigest()
            try:
                self._members = _read_members(data)
            except UnicodeDecodeError:
                self._format_error = "The pattern file is not valid UTF-8"
            except ValueError as exc:
                self._format_error = str(exc)
            else:
                self._references.update(member.fields for member in self._members)
                self._timestamps = pa.array(
                    sorted({member.fields[0] for member in self._members}), type=pa.string()
                )

    @property
    def membership_scanned(self) -> bool:
        return self._timestamps is not None

    def observe(self, columns: list[pa.Array], offset: int) -> None:
        if self._timestamps is None:
            return
        offsets = pc.indices_nonzero(pc.is_in(columns[0], value_set=self._timestamps))
        if not len(offsets):
            return
        values = [pc.take(column, offsets).to_pylist() for column in columns]
        for index, fields in zip(offsets.to_pylist(), zip(*values, strict=True), strict=True):
            if fields in self._references:
                count, first_row = self._hits.get(fields, (0, offset + index))
                self._hits[fields] = count + 1, first_row

    def report(self, source: Source, raw: dict) -> dict:
        release = f"{source.id}:{raw['sha256']}"
        ibm = source.kind == "ibm"
        result = dict(
            contract_version=PATTERN_CONTRACT,
            source_id=source.id,
            source_release=release,
            source_csv=dict(path=str(source.path), sha256=raw["sha256"], rows=raw["rows"]),
            patterns=(
                dict(path=str(self.pattern_path), sha256=self.pattern_sha256)
                if self.pattern_sha256 is not None
                else None
            ),
            source_fields=dict(
                csv_header=raw["raw_header"],
                timestamp_precision=raw["timestamp_precision"],
                timestamp_timezone=raw["source_timezone"],
                amount_fields=["Amount Received", "Amount Paid"] if ibm else ["Amount"],
                amount_precision=source.amount_precision,
                amount_scale=source.amount_scale,
                currency_fields=(
                    ["Receiving Currency", "Payment Currency"]
                    if ibm
                    else ["Payment_currency", "Received_currency"]
                ),
                different_currency_single_amount_basis=None if ibm else "UNSPECIFIED",
                label_semantics=raw["label_semantics"],
                source_event_id_column=None,
            ),
            scope="FULL_ORIGINAL",
            use="OFFLINE_SOURCE_ROW_AND_PATTERN_AUDIT_ONLY",
            match_policy="EXACT_11_PARSED_ORIGINAL_CSV_FIELDS" if ibm else None,
            source_row_id_convention="ZERO_BASED_CSV_RECORD_EXCLUDING_HEADER",
            transaction_uid_format="{source_release}:{source_row_id}",
            pattern_identity="PATTERNS_SHA256_AND_ZERO_BASED_SUPPLIED_GROUP_ORDINAL" if ibm else None,
            pattern_line_convention="ONE_BASED_PATTERN_FILE_LINE" if ibm else None,
            counts=None,
            verified_links=None,
            unresolved_pattern_rows=None,
        )
        if self._format_error is not None:
            return dict(
                result,
                status="INVALID_PATTERN_FILE",
                reason="INVALID_IBM_PATTERN_LAYOUT",
                format_error=self._format_error,
            )
        if not self.membership_scanned:
            reason = "SAML_NO_SUPPLIED_PATTERNS_OR_VERIFIED_EVENT_ID"
            if source.kind == "ibm":
                reason = "IBM_PATTERN_FILE_MISSING" if self.pattern_path else "IBM_PATTERNS_NOT_SET"
            return dict(result, status="NOT_EVALUABLE", reason=reason)

        reasons = {}
        for member in self._members:
            count, _ = self._hits.get(member.fields, (0, None))
            if count == 0:
                reasons[member.line_number] = "NO_EXACT_SOURCE_ROW"
            elif count > 1:
                reasons[member.line_number] = "MULTIPLE_EXACT_SOURCE_ROWS"
            elif self._references[member.fields] > 1:
                reasons[member.line_number] = "SOURCE_ROW_REFERENCED_BY_MULTIPLE_PATTERN_MEMBERS"
        blocked = {
            member.group_ordinal for member in self._members if member.line_number in reasons
        }
        links, unresolved = [], []
        positive_linked = 0
        for member in self._members:
            count, row_id = self._hits.get(member.fields, (0, None))
            location = dict(
                pattern_group_ordinal=member.group_ordinal,
                pattern_group_begin_line=member.group_begin_line,
                pattern_line_number=member.line_number,
            )
            if member.group_ordinal in blocked:
                unresolved.append(
                    dict(
                        location,
                        reason=reasons.get(member.line_number, "GROUP_NOT_FULLY_VERIFIED"),
                        exact_candidate_rows=count,
                    )
                )
            else:
                links.append(dict(location, source_row_id=row_id))
                positive_linked += member.fields[-1] == "1"
        failures = Counter(row["reason"] for row in unresolved)
        groups = len({member.group_ordinal for member in self._members})
        return dict(
            result,
            status="INCOMPLETE" if blocked else "VERIFIED",
            reason="ONE_OR_MORE_PATTERN_GROUPS_NOT_VERIFIED" if blocked else None,
            counts=dict(
                pattern_groups=groups,
                pattern_rows=len(self._members),
                verified_pattern_groups=groups - len(blocked),
                verified_pattern_rows=len(links),
                blocked_pattern_groups=len(blocked),
                unmatched_pattern_rows=failures["NO_EXACT_SOURCE_ROW"],
                multiple_match_pattern_rows=failures["MULTIPLE_EXACT_SOURCE_ROWS"],
                reused_source_row_pattern_rows=failures[
                    "SOURCE_ROW_REFERENCED_BY_MULTIPLE_PATTERN_MEMBERS"
                ],
                other_rows_in_blocked_groups=failures["GROUP_NOT_FULLY_VERIFIED"],
                verified_positive_source_rows=positive_linked,
                positive_source_rows_without_verified_pattern=raw["labels"].get("1", 0)
                - positive_linked,
            ),
            verified_links=links,
            unresolved_pattern_rows=unresolved,
        )
