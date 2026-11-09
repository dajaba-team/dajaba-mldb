"""원본 금액을 반올림하거나 ID를 발급하지 않고 numeric(20,6) 전달 감사를 수행합니다.

과거 v3 감사는 numeric(18,2)를 유지합니다. 표현 가능한 음수는 여기에서
허용하며, 원천의 부호 검증은 별도의 상위 단계 정책으로 남깁니다.
"""

from datetime import datetime
from decimal import Decimal

import pyarrow as pa
from dbcheck.data_contract import service_account_key

SERVICE_ACCOUNT_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "account_key",
            "bank_id",
            "account_no",
            "entity_id",
            "master_bank_id_raw",
            "internal_account_key",
            "bank_name",
        )
    ]
)
SERVICE_ENTITY_SCHEMA = pa.schema([("entity_id", pa.string()), ("entity_name", pa.string())])

SERVICE_CONTRACT_VERSION = "dajaba-service-candidate-v4"
SUPPORTED_SERVICE_CONTRACTS = frozenset({"dajaba-service-candidate-v3", SERVICE_CONTRACT_VERSION})
SERVICE_NUMERIC_TYPE = "numeric(20,6)"
SERVICE_VALIDATION_BATCH_ROWS = 256
INT64_MAX = (1 << 63) - 1
MAX_SERVICE_AMOUNT = Decimal("99999999999999.999999")
_NUMERIC_SPECS = {
    SERVICE_NUMERIC_TYPE: (MAX_SERVICE_AMOUNT, 6),
    "numeric(18,2)": (Decimal("9999999999999999.99"), 2),  # 과거 v3에만 사용합니다.
}


def _fits_numeric(value: Decimal, maximum: Decimal, scale: int) -> bool:
    if not isinstance(value, Decimal):
        raise TypeError("Service amount audit requires exact Decimal values")
    if not value.is_finite() or value.copy_abs() > maximum:
        return False
    # quantize를 피합니다. 호출자의 컨텍스트·트랩 설정과 극단적인 지수 때문에
    # 피연산자를 반올림하거나 InvalidOperation을 발생시키면 안 됩니다.
    parts = value.as_tuple()
    excess = -int(parts.exponent) - scale
    return excess <= 0 or not any(parts.digits[-excess:])


def fits_service_numeric(value: Decimal) -> bool:
    """numeric(20,6)으로 정확히 표현할 수 있어야 하며 끝의 0은 문제없습니다."""
    return _fits_numeric(value, MAX_SERVICE_AMOUNT, 6)


def new_money_audit(*, service_contract_version: str = SERVICE_CONTRACT_VERSION) -> dict:
    if service_contract_version not in SUPPORTED_SERVICE_CONTRACTS:
        raise ValueError("Unsupported service contract version")
    target_type = (
        "numeric(18,2)"
        if service_contract_version == "dajaba-service-candidate-v3"
        else SERVICE_NUMERIC_TYPE
    )
    return dict(
        target_type=target_type,
        status="NOT_CHECKED",
        checked_rows=0,
        incompatible_rows=0,
        incompatible_values=0,
        examples=[],
        policy="PRESERVE_ORIGINAL_NO_ROUNDING; incompatible values require team decision",
    )


def audit_service_money(table: pa.Table, audit: dict) -> None:
    """진단 정보를 제한된 크기로 집계합니다. 출력 생성은 COPY 적재 승인이 아닙니다."""
    spec = _NUMERIC_SPECS.get(audit["target_type"])
    if spec is None:
        raise ValueError("Unsupported service numeric target")
    maximum, scale = spec
    for row in table.select(
        ["source_row_id", "source_file_hash", "amount_paid", "amount_received"]
    ).to_pylist():
        invalid = [
            name
            for name in ("amount_paid", "amount_received")
            if not _fits_numeric(row[name], maximum, scale)
        ]
        audit["checked_rows"] += 1
        audit["incompatible_rows"] += bool(invalid)
        audit["incompatible_values"] += len(invalid)
        if invalid and len(audit["examples"]) < 20:
            audit["examples"].append(
                dict(
                    source_file_hash=row["source_file_hash"],
                    source_row_id=row["source_row_id"],
                    incompatible_fields={name: str(row[name]) for name in invalid},
                )
            )
    audit["status"] = (
        "BLOCKED_PRECISION_LOSS"
        if audit["incompatible_rows"]
        else ("EXACTLY_REPRESENTABLE" if audit["checked_rows"] else "NOT_CHECKED")
    )


def _service_rows(table: pa.Table, columns: set[str]):
    selected = table.select([name for name in table.column_names if name in columns])
    for batch in selected.to_batches(max_chunksize=SERVICE_VALIDATION_BATCH_ROWS):
        yield from batch.to_pylist()


def validate_service_handoff(
    transactions: pa.Table, mapping: pa.Table, *, service_contract_version: str, source_release: str
) -> None:
    """오프라인 후보의 출처를 검사하며 백엔드 원본 적재나 DB 발급 ID는 검증하지 않습니다."""
    if service_contract_version not in SUPPORTED_SERVICE_CONTRACTS:
        raise ValueError("Unsupported service contract version")
    forbidden = {
        "transaction_id",
        "source_positive",
        "source_label_raw",
        "source_typology",
        "is_laundering",
        "Laundering_type",
        "score",
        "prediction",
    }
    if forbidden & (set(transactions.schema.names) | set(mapping.schema.names)):
        raise ValueError("Offline service candidates must exclude backend IDs and labels/scores")
    tx_required = {
        "candidate_transaction_id",
        "source_file_hash",
        "source_row_id",
        "ts",
        "from_account_key",
        "to_account_key",
    }
    map_required = {
        "candidate_transaction_id",
        "source_file_hash",
        "source_row_id",
        "transaction_uid",
        "role",
    }
    current = service_contract_version == SERVICE_CONTRACT_VERSION
    if current:
        tx_required |= {"from_bank_id_raw", "to_bank_id_raw"}
        map_required.add("source_row_number")
    if not tx_required <= set(transactions.schema.names) or not map_required <= set(
        mapping.schema.names
    ):
        raise ValueError("Service handoff is missing required versioned columns")
    if len(transactions) != len(mapping):
        raise ValueError("Service transaction/mapping row alignment mismatch")
    source_hash = source_release.partition(":")[2]
    if not source_hash:
        raise ValueError("Service source release must include the original file fingerprint")
    tx_rows = _service_rows(transactions, tx_required)
    map_rows = _service_rows(mapping, map_required)
    for tx, link in zip(tx_rows, map_rows):
        ordinal = tx["source_row_id"]
        if type(ordinal) is not int or not 0 <= ordinal < INT64_MAX:
            raise ValueError(
                "Service source_row_id must be a nonnegative original ordinal below BIGINT maximum"
            )
        candidate = tx["candidate_transaction_id"]
        if (
            type(candidate) is not int
            or not 1 <= candidate <= INT64_MAX
            or candidate != ordinal + 1
        ):
            raise ValueError("Service candidate ID must equal source_row_id + 1, never a DB ID")
        if any(type(link[key]) is not int for key in ("candidate_transaction_id", "source_row_id")):
            raise ValueError("Service mapping candidate ID and source_row_id must be integers")
        if not (
            1 <= link["candidate_transaction_id"] <= INT64_MAX
            and 0 <= link["source_row_id"] < INT64_MAX
        ):
            raise ValueError("Service mapping ID is outside the signed BIGINT range")
        if any(
            tx[key] != link[key]
            for key in ("candidate_transaction_id", "source_file_hash", "source_row_id")
        ):
            raise ValueError("Service transaction/mapping provenance mismatch")
        if (
            tx["source_file_hash"] != source_hash
            or link["transaction_uid"] != f"{source_release}:{ordinal}"
        ):
            raise ValueError("Service original file/row/UID mismatch")
        if link["role"] not in {"TARGET", "HISTORY"}:
            raise ValueError("Service mapping role must be TARGET or HISTORY")
        stamp = tx["ts"]
        if not isinstance(stamp, datetime) or stamp.tzinfo is not None:
            raise ValueError("Service timestamp must remain timezone-naive")
        if current:
            if (
                type(link["source_row_number"]) is not int
                or link["source_row_number"] != ordinal + 1
            ):
                raise ValueError("Service source_row_number must be one-based, header excluded")
            for side in ("from", "to"):
                key = tx[f"{side}_account_key"]
                if not isinstance(key, str) or "_" not in key:
                    raise ValueError(
                        "Service account key must preserve the original account string"
                    )
                account = key.partition("_")[2]
                raw_bank = tx[f"{side}_bank_id_raw"]
                if not isinstance(raw_bank, str):
                    raise ValueError("Service raw bank ID must remain a string")
                if key != service_account_key(raw_bank, account):
                    raise ValueError("Service account key differs from the canonical raw-bank key")
