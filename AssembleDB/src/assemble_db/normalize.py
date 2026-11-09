"""원천별 값을 보존하는 배치를 만듭니다. 라벨은 서비스 거래에 넣지 않습니다."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
from dbcheck.config import Source
from dbcheck.data_contract import account_match_key, currency, service_account_key
from dbcheck.distribution import pc


def batches(path: Path, header: list[str], batch_rows: int):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        if next(csv.reader(stream)) != header:
            raise ValueError(f"Source header changed: {path}")
    reader = pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(
            column_names=[f"c{i}" for i in range(len(header))],
            skip_rows=1,
            block_size=4 * 1024 * 1024,
            use_threads=False,
        ),
        parse_options=pacsv.ParseOptions(newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(
            column_types={f"c{i}": pa.string() for i in range(len(header))},
            strings_can_be_null=False,
            null_values=[],
        ),
    )
    offset = 0
    for batch in reader:
        for start in range(0, len(batch), batch_rows):
            part = batch.slice(start, batch_rows)
            yield offset, part
            offset += len(part)


def transaction_schema(source: Source) -> pa.Schema:
    fields = [
        ("transaction_uid", pa.string()),
        ("source_row_id", pa.int64()),
        ("ts", pa.timestamp("us")),
        ("from_account_key", pa.string()),
        ("to_account_key", pa.string()),
        ("from_account_raw", pa.string()),
        ("to_account_raw", pa.string()),
        ("payment_format", pa.string()),
        ("is_self_transfer", pa.bool_()),
    ]
    money = pa.decimal128(source.amount_precision, source.amount_scale)
    if source.kind == "ibm":
        fields += [
            ("from_bank_raw", pa.string()),
            ("to_bank_raw", pa.string()),
            ("amount_paid", money),
            ("amount_received", money),
            ("amount_paid_raw", pa.string()),
            ("amount_received_raw", pa.string()),
        ]
        for side in ["paid", "received"]:
            fields += [
                (f"currency_{side}_raw", pa.string()),
                (f"currency_{side}_iso", pa.string()),
                (f"currency_{side}_bucket", pa.string()),
            ]
    else:
        fields += [
            ("amount", money),
            ("amount_raw", pa.string()),
            ("currency_paid_raw", pa.string()),
            ("currency_received_raw", pa.string()),
            ("amount_currency_bucket", pa.string()),
            ("amount_currency_iso", pa.string()),
            ("money_operations_allowed", pa.bool_()),
            ("sender_bank_location", pa.string()),
            ("receiver_bank_location", pa.string()),
        ]
    return pa.schema(fields)


def label_schema(source: Source) -> pa.Schema:
    fields = [
        ("transaction_uid", pa.string()),
        ("source_row_id", pa.int64()),
        ("source_positive", pa.int8()),
        ("source_label_raw", pa.string()),
    ]
    if source.kind == "saml":
        fields += [("source_typology", pa.string())]
    return pa.schema(fields)


def normalize(
    source: Source, fingerprint: str, offset: int, batch: pa.RecordBatch, aliases: dict[str, str]
):
    columns = [batch.column(i) for i in range(batch.num_columns)]
    values = [col.to_pylist() for col in columns]
    n = len(batch)
    release = f"{source.id}:{fingerprint}"
    indices = list(range(offset, offset + n))
    uids = [f"{release}:{i}" for i in indices]
    time_raw = (
        columns[0]
        if source.kind == "ibm"
        else pc.binary_join_element_wise(columns[1], columns[0], " ")
    )
    fmt = "%Y/%m/%d %H:%M" if source.kind == "ibm" else "%Y-%m-%d %H:%M:%S"
    times = pc.strptime(time_raw, format=fmt, unit="s", error_is_null=False)
    if not pc.equal(pc.strftime(times, format=fmt), time_raw).to_pylist() == [True] * n:
        raise ValueError("Timestamp cannot be represented without changing its meaning")
    times = pc.cast(times, pa.timestamp("us"))
    from_i, to_i = (2, 4) if source.kind == "ibm" else (2, 3)
    if source.kind == "ibm":
        senders = [account_match_key(release, b, a) for b, a in zip(values[1], values[2])]
        receivers = [account_match_key(release, b, a) for b, a in zip(values[3], values[4])]
    else:
        # SAML의 은행·고객 차원 테이블을 임의로 만들지 않습니다.
        import json

        from dbcheck.data_contract import is_unresolved

        if any(is_unresolved(v) for i in (2, 3) for v in values[i]):
            raise ValueError("Unresolved SAML account cannot form a relationship")
        senders = [json.dumps([release, a], separators=(",", ":")) for a in values[2]]
        receivers = [json.dumps([release, a], separators=(",", ":")) for a in values[3]]
    data: dict[str, Any] = dict(
        transaction_uid=uids,
        source_row_id=indices,
        ts=times,
        from_account_key=senders,
        to_account_key=receivers,
        from_account_raw=columns[from_i],
        to_account_raw=columns[to_i],
        payment_format=columns[9],
        is_self_transfer=[a == b for a, b in zip(senders, receivers)],
    )
    money = pa.decimal128(source.amount_precision, source.amount_scale)
    units = {
        raw: currency(source.id, raw, aliases)
        for i in ([6, 8] if source.kind == "ibm" else [5, 6])
        for raw in set(values[i])
    }
    if source.kind == "ibm":
        data.update(
            from_bank_raw=columns[1],
            to_bank_raw=columns[3],
            amount_paid=pc.cast(columns[7], money),
            amount_received=pc.cast(columns[5], money),
            amount_paid_raw=columns[7],
            amount_received_raw=columns[5],
        )
        for side, index in [("paid", 8), ("received", 6)]:
            data[f"currency_{side}_raw"] = columns[index]
            data[f"currency_{side}_iso"] = [units[r].iso_code for r in values[index]]
            data[f"currency_{side}_bucket"] = [units[r].bucket for r in values[index]]
    else:
        basis = [
            units[p] if units[p].bucket == units[r].bucket else None
            for p, r in zip(values[5], values[6])
        ]
        data.update(
            amount=pc.cast(columns[4], money),
            amount_raw=columns[4],
            currency_paid_raw=columns[5],
            currency_received_raw=columns[6],
            amount_currency_bucket=[u.bucket if u else None for u in basis],
            amount_currency_iso=[u.iso_code if u else None for u in basis],
            money_operations_allowed=[u is not None for u in basis],
            sender_bank_location=columns[7],
            receiver_bank_location=columns[8],
        )
    labels = dict(
        transaction_uid=uids,
        source_row_id=indices,
        source_positive=[int(v) for v in values[10]],
        source_label_raw=columns[10],
    )
    if source.kind == "saml":
        labels["source_typology"] = columns[11]
    transactions = pa.Table.from_pydict(data, schema=transaction_schema(source))
    label_table = pa.Table.from_pydict(labels, schema=label_schema(source))
    return transactions, label_table


ACCOUNT_HEADER = ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"]
ACCOUNT_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in [
            "account_key",
            "bank_raw",
            "account_raw",
            "entity_id_raw",
            "entity_name_raw",
            "bank_name_raw",
        ]
    ]
)


def normalize_accounts(source: Source, release: str, batch: pa.RecordBatch) -> pa.Table:
    values = [batch.column(i).to_pylist() for i in range(5)]
    return pa.Table.from_pydict(
        dict(
            account_key=[account_match_key(release, b, a) for b, a in zip(values[1], values[2])],
            bank_raw=values[1],
            account_raw=values[2],
            entity_id_raw=values[3],
            entity_name_raw=values[4],
            bank_name_raw=values[0],
        ),
        schema=ACCOUNT_SCHEMA,
    )


def service_schema(source: Source) -> pa.Schema:
    money = pa.decimal128(source.amount_precision, source.amount_scale)
    return pa.schema(
        [
            ("candidate_transaction_id", pa.int64()),
            ("source_file_hash", pa.string()),
            ("source_row_id", pa.int64()),
            ("ts", pa.timestamp("us")),
            ("from_account_key", pa.string()),
            ("to_account_key", pa.string()),
            ("from_bank_id_raw", pa.string()),
            ("to_bank_id_raw", pa.string()),
            ("amount_paid", money),
            ("currency_paid", pa.string()),
            ("amount_received", money),
            ("currency_received", pa.string()),
            ("payment_format", pa.string()),
            ("is_self_transfer", pa.bool_()),
        ]
    )


SERVICE_MAPPING_SCHEMA = pa.schema(
    [
        ("candidate_transaction_id", pa.int64()),
        ("source_file_hash", pa.string()),
        ("transaction_uid", pa.string()),
        ("source_row_id", pa.int64()),
        ("source_row_number", pa.int64()),
        ("role", pa.string()),
    ]
)


def service_tables(source: Source, table: pa.Table, start: datetime):
    data = table.to_pydict()
    ids = [i + 1 for i in data["source_row_id"]]
    # 오프라인 행 순번은 의도적으로 transaction_id라고 부르지 않습니다. 백엔드가
    # source_file_id / transaction_id를 발급합니다. 확정된 source_row_number 규칙은
    # 헤더를 제외한 데이터 행을 1부터 세는 것입니다.
    file_hashes = [uid.rsplit(":", 2)[1] for uid in data["transaction_uid"]]
    senders = [
        service_account_key(b, a) for b, a in zip(data["from_bank_raw"], data["from_account_raw"])
    ]
    receivers = [
        service_account_key(b, a) for b, a in zip(data["to_bank_raw"], data["to_account_raw"])
    ]
    result = pa.Table.from_pydict(
        dict(
            candidate_transaction_id=ids,
            source_file_hash=file_hashes,
            source_row_id=data["source_row_id"],
            ts=data["ts"],
            from_account_key=senders,
            to_account_key=receivers,
            from_bank_id_raw=data["from_bank_raw"],
            to_bank_id_raw=data["to_bank_raw"],
            amount_paid=data["amount_paid"],
            currency_paid=data["currency_paid_raw"],
            amount_received=data["amount_received"],
            currency_received=data["currency_received_raw"],
            payment_format=data["payment_format"],
            is_self_transfer=data["is_self_transfer"],
        ),
        schema=service_schema(source),
    )
    mapping = pa.Table.from_pydict(
        dict(
            candidate_transaction_id=ids,
            source_file_hash=file_hashes,
            transaction_uid=data["transaction_uid"],
            source_row_id=data["source_row_id"],
            source_row_number=ids,
            role=["TARGET" if ts >= start else "HISTORY" for ts in data["ts"]],
        ),
        schema=SERVICE_MAPPING_SCHEMA,
    )
    referenced = set(zip(data["from_bank_raw"], data["from_account_raw"])) | set(
        zip(data["to_bank_raw"], data["to_account_raw"])
    )
    return result, mapping, referenced
