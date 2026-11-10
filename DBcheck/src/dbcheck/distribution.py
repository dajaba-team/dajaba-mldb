"""머신러닝을 수행하지 않고 원천의 전체 기간 분포를 읽기 전용으로 조사합니다.

원본 CSV는 읽기 전용으로 열며 중간 저장·피처 생성·조립 코드를 실행하지 않습니다.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol, TypedDict, cast

import pyarrow as pa
import pyarrow.compute as _compute
import pyarrow.csv as pacsv

from .data_contract import NA_TOKENS, UNKNOWN_TOKENS
from .patterns import PatternMembership


class ArrowCompute(Protocol):
    """공개 연산 커널은 Arrow의 컴파일된 모듈이 동적으로 등록합니다."""

    binary_join_element_wise: Callable[..., Any]
    cast: Callable[..., Any]
    drop_null: Callable[..., Any]
    equal: Callable[..., Any]
    fill_null: Callable[..., Any]
    if_else: Callable[..., Any]
    indices_nonzero: Callable[..., Any]
    invert: Callable[..., Any]
    is_in: Callable[..., Any]
    is_null: Callable[..., Any]
    less: Callable[..., Any]
    match_substring_regex: Callable[..., Any]
    min_max: Callable[..., Any]
    not_equal: Callable[..., Any]
    or_: Callable[..., Any]
    strftime: Callable[..., Any]
    strptime: Callable[..., Any]
    sum: Callable[..., Any]
    utf8_trim_whitespace: Callable[..., Any]
    utf8_upper: Callable[..., Any]
    value_counts: Callable[..., Any]


pc = cast(ArrowCompute, _compute)


class DayStats(TypedDict):
    day: str
    rows: int
    normal: int
    positive: int
    invalid_label: int
    positive_rate: float | None


IBM = [
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account",
    "Amount Received",
    "Receiving Currency",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
    "Is Laundering",
]
SAML = [
    "Time",
    "Date",
    "Sender_account",
    "Receiver_account",
    "Amount",
    "Payment_currency",
    "Received_currency",
    "Sender_bank_location",
    "Receiver_bank_location",
    "Payment_type",
    "Is_laundering",
    "Laundering_type",
]
LOG = logging.getLogger("phase1")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def count(mask: pa.Array) -> int:
    return pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0


def values(array: pa.Array) -> dict[str, int]:
    return {str(r["values"]): r["counts"] for r in pc.value_counts(array).to_pylist()}


def time_rows(hours: dict) -> list[dict]:
    if not hours:
        return []
    first, last = sorted(hours)[0], sorted(hours)[-1]
    current = datetime.fromisoformat(first)
    end = datetime.fromisoformat(last)
    rows = []
    while current <= end:
        key = current.strftime("%Y-%m-%d %H:00:00")
        n, neg, pos, invalid = hours.get(key, [0, 0, 0, 0])
        rows.append(
            dict(
                hour=key,
                rows=n,
                normal=neg,
                positive=pos,
                invalid_label=invalid,
                positive_rate=pos / n if n else None,
                no_observations=n == 0,
            )
        )
        current += timedelta(hours=1)
    return rows


def profile(
    source_id: str,
    rel: str,
    kind: str,
    out: Path,
    *,
    pattern_membership: PatternMembership | None = None,
) -> dict:
    path = Path(rel).resolve()
    stat_before = path.stat()
    fingerprint = sha256(path)
    with path.open(encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f))
    expected = IBM if kind == "ibm" else SAML
    if header != expected:
        raise ValueError(f"{source_id}: HEADER_MISMATCH; inspect original contract")
    names = [f"c{i}" for i in range(len(header))]
    reader = pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(
            column_names=names, skip_rows=1, block_size=8 * 1024 * 1024, use_threads=False
        ),
        parse_options=pacsv.ParseOptions(newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in names}, strings_can_be_null=False, null_values=[]
        ),
    )
    hours = defaultdict(lambda: [0, 0, 0, 0])
    blanks, unknowns, na = Counter(), Counter(), Counter()
    quality = Counter(
        {
            key: 0
            for key in (
                "file_order_time_regressions",
                "timestamp_parse_failure",
                "invalid_label",
                "bank_1_non_digit",
                "bank_3_non_digit",
                "different_payment_receiving_currency",
            )
        }
    )
    labels, currencies, payment, types = Counter(), Counter(), Counter(), Counter()
    rows = 0
    examples = []
    minimum, maximum = None, None
    previous_timestamp = None
    start = time.perf_counter()
    for batch_no, batch in enumerate(reader, 1):
        cols = [batch.column(i) for i in range(len(names))]
        n = len(batch)
        if pattern_membership is not None:
            pattern_membership.observe(cols, rows)
        for i, col in enumerate(cols):
            stripped = pc.utf8_trim_whitespace(col)
            upper = pc.utf8_upper(stripped)
            blanks[f"{i}:{header[i]}"] += count(pc.equal(stripped, ""))
            unknowns[f"{i}:{header[i]}"] += count(
                pc.is_in(upper, value_set=pa.array(sorted(UNKNOWN_TOKENS)))
            )
            na[f"{i}:{header[i]}"] += count(pc.is_in(upper, value_set=pa.array(sorted(NA_TOKENS))))
        time_text = cols[0] if kind == "ibm" else pc.binary_join_element_wise(cols[1], cols[0], " ")
        fmt = "%Y/%m/%d %H:%M" if kind == "ibm" else "%Y-%m-%d %H:%M:%S"
        ts = pc.strptime(time_text, format=fmt, unit="s", error_is_null=True)
        # strptime은 넘어간 날짜를 허용할 수 있으므로 문자열 왕복 변환의 정확한 일치를 확인합니다.
        exact = pc.equal(pc.strftime(ts, format=fmt), time_text)
        ts = pc.if_else(pc.fill_null(exact, False), ts, None)
        badtime = pc.is_null(ts)
        quality["timestamp_parse_failure"] += count(badtime)
        valid_ts = pc.drop_null(ts)
        if len(valid_ts):
            bounds = pc.min_max(valid_ts).as_py()
            minimum = min(minimum, bounds["min"]) if minimum else bounds["min"]
            maximum = max(maximum, bounds["max"]) if maximum else bounds["max"]
            if previous_timestamp is not None:
                quality["file_order_time_regressions"] += int(
                    valid_ts[0].as_py() < previous_timestamp
                )
            if len(valid_ts) > 1:
                quality["file_order_time_regressions"] += count(
                    pc.less(valid_ts.slice(1), valid_ts.slice(0, len(valid_ts) - 1))
                )
            previous_timestamp = valid_ts[-1].as_py()
        label = cols[10]
        neg, pos = pc.equal(label, "0"), pc.equal(label, "1")
        badlabel = pc.invert(pc.or_(neg, pos))
        quality["invalid_label"] += count(badlabel)
        labels.update(values(label))
        pc_index, rc_index, payment_index = (8, 6, 9) if kind == "ibm" else (5, 6, 9)
        currencies.update(
            values(pc.binary_join_element_wise(cols[pc_index], cols[rc_index], " -> "))
        )
        payment.update(values(cols[payment_index]))
        quality["different_payment_receiving_currency"] += count(
            pc.not_equal(cols[pc_index], cols[rc_index])
        )
        if kind == "saml":
            types.update(values(cols[11]))
        for i in [5, 7] if kind == "ibm" else [4]:
            numeric = pc.match_substring_regex(cols[i], r"^[+]?[0-9]+(\.[0-9]+)?$")
            quality[f"amount_{i}_non_plain_decimal"] += n - count(numeric)
            zero = pc.match_substring_regex(cols[i], r"^[+]?0+(\.0+)?$")
            quality[f"amount_{i}_zero"] += count(zero)
        if kind == "ibm":
            for i in (1, 3):
                quality[f"bank_{i}_non_digit"] += n - count(
                    pc.match_substring_regex(cols[i], r"^[0-9]+$")
                )
        hour = pc.strftime(ts, format="%Y-%m-%d %H:00:00")
        table = pa.table(
            {
                "hour": hour,
                "normal": pc.cast(neg, pa.int64()),
                "positive": pc.cast(pos, pa.int64()),
                "invalid": pc.cast(badlabel, pa.int64()),
                "n": pa.array([1] * n, pa.int64()),
            }
        )
        grouped = table.group_by("hour", use_threads=False).aggregate(
            [("n", "sum"), ("normal", "sum"), ("positive", "sum"), ("invalid", "sum")]
        )
        for row in grouped.to_pylist():
            if row["hour"] is None:
                continue
            target = hours[row["hour"]]
            for j, col in enumerate(["n_sum", "normal_sum", "positive_sum", "invalid_sum"]):
                target[j] += row[col]
        if len(examples) < 5:
            for i in pc.indices_nonzero(pc.or_(badtime, badlabel)).to_pylist()[: 5 - len(examples)]:
                examples.append(
                    dict(
                        source_row_id=rows + i,
                        time_raw=time_text[i].as_py(),
                        label_raw=label[i].as_py(),
                    )
                )
        rows += n
        if batch_no % 15 == 0:
            LOG.info("%s scanned %s rows", source_id, f"{rows:,}")
    hourly = time_rows(hours)
    daily = defaultdict(lambda: [0, 0, 0, 0])
    for r in hourly:
        for i, col in enumerate(["rows", "normal", "positive", "invalid_label"]):
            daily[r["hour"][:10]][i] += r[col]
    days: list[DayStats] = [
        {
            "day": d,
            "rows": v[0],
            "normal": v[1],
            "positive": v[2],
            "invalid_label": v[3],
            "positive_rate": v[2] / v[0] if v[0] else None,
        }
        for d, v in sorted(daily.items())
    ]
    candidates = []
    baseline_peak = None
    for i, d in enumerate(days):
        prior = days[max(0, i - 3) : i]
        baseline = statistics.median([x["normal"] for x in prior]) if len(prior) == 3 else None
        if baseline is not None:
            baseline_peak = max(baseline_peak or 0, baseline)
        if baseline_peak and d["normal"] < baseline_peak * 0.1:
            candidates.append(
                dict(
                    start=d["day"] + " 00:00:00",
                    end=d["day"] + " 23:59:59",
                    normal=d["normal"],
                    positive=d["positive"],
                    rows=d["rows"],
                    prior_three_day_median_peak_normal=baseline_peak,
                    reason="NORMAL_VOLUME_BELOW_10_PERCENT_OF_PRIOR_THREE_DAY_MEDIAN_PEAK",
                    decision="REVIEW_ONLY_NO_EXCLUSION",
                    hypothetical_remaining_rows=rows - d["rows"],
                    hypothetical_remaining_positive=labels["1"] - d["positive"],
                )
            )
    for name, data in [("hourly", hourly), ("daily", days)]:
        if data:
            with (out / f"{source_id}_{name}.csv").open("w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(data[0]))
                w.writeheader()
                w.writerows(data)
    after = path.stat()
    final_hash = sha256(path)
    unchanged = fingerprint == final_hash and (stat_before.st_size, stat_before.st_mtime_ns) == (
        after.st_size,
        after.st_mtime_ns,
    )
    accounted = sum(r["rows"] for r in hourly) + quality["timestamp_parse_failure"]
    if accounted != rows or not unchanged:
        raise RuntimeError(f"{source_id}: reconciliation or immutable-input verification failed")
    result = dict(
        source_id=source_id,
        path=str(path),
        kind=kind,
        file_bytes=stat_before.st_size,
        sha256=fingerprint,
        scope="FULL_ORIGINAL_CSV_ALL_RECORDS",
        rows=rows,
        raw_header=header,
        header_duplicate_names=sorted({x for x in header if header.count(x) > 1}),
        labels=dict(labels),
        positive_rate=labels["1"] / rows if rows else None,
        label_semantics="AMLWORLD_ILLICIT_FLOW"
        if kind == "ibm"
        else "SOURCE_DESIGNATED_SUSPICIOUS",
        timestamp_min=minimum,
        timestamp_max=maximum,
        source_timezone="UNKNOWN",
        timestamp_precision="MINUTE" if kind == "ibm" else "SECOND",
        quality=dict(quality),
        blank_cells=dict(blanks),
        unknown_token_cells=dict(unknowns),
        na_token_cells=dict(na),
        currency_pairs=dict(currencies),
        payment_types=dict(payment),
        source_typologies=dict(types),
        daily=days,
        hourly_csv=f"{source_id}_hourly.csv",
        no_observation_hours=sum(r["no_observations"] for r in hourly),
        distribution_candidates=candidates,
        problem_examples=examples,
        source_unchanged=unchanged,
        reconciliation=dict(
            input_rows=rows,
            time_aggregated_rows=sum(r["rows"] for r in hourly),
            unparseable_time_rows=quality["timestamp_parse_failure"],
        ),
        duration_seconds=round(time.perf_counter() - start, 3),
        not_checked=[
            "full-row exact duplicates",
            "IBM accounts foreign keys",
            "pattern membership matching",
            "monetary DECIMAL overflow/scale",
            "external expected timestamp range",
            "live PostgreSQL COPY compatibility",
        ],
    )
    if pattern_membership is not None and pattern_membership.membership_scanned:
        result["not_checked"].remove("pattern membership matching")
    write_json(out / f"{source_id}_profile.json", result)
    LOG.info(
        "%s complete: %s rows, %s positives, %s .. %s",
        source_id,
        f"{rows:,}",
        labels["1"],
        minimum,
        maximum,
    )
    return result
