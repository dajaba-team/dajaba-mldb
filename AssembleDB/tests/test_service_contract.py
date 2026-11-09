from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext

import pyarrow as pa
import pytest

from assemble_db.service_contract import (
    INT64_MAX,
    SERVICE_CONTRACT_VERSION,
    SERVICE_VALIDATION_BATCH_ROWS,
    audit_service_money,
    fits_service_numeric,
    new_money_audit,
    validate_service_handoff,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("0", True),
        ("1.234567", True),
        ("0.000001", True),
        ("0.0000001", False),
        ("1.2345670", True),
        ("1.2345671", False),
        ("99999999999999", True),
        ("99999999999999.999999", True),
        ("99999999999999.9999990", True),
        ("99999999999999.9999991", False),
        ("100000000000000", False),
        ("-0.000001", True),
        ("-99999999999999.999999", True),
        ("-100000000000000", False),
        ("1E+999999999", False),
        ("1E-999999999", False),
        ("0E-999999999", True),
        ("NaN", False),
        ("sNaN", False),
        ("Infinity", False),
        ("-Infinity", False),
    ],
)
def test_numeric_20_6_exact_boundaries(value, expected):
    amount = Decimal(value)
    original = amount.as_tuple()
    with localcontext() as context:
        context.prec = 2
        context.Emax = 9
        context.Emin = -9
        context.traps[InvalidOperation] = True
        assert fits_service_numeric(amount) is expected
    assert amount.as_tuple() == original


def test_float_amount_is_rejected():
    with pytest.raises(TypeError, match="exact Decimal"):
        fits_service_numeric(0.01)  # pyright: ignore[reportArgumentType] -- 잘못된 입력의 회귀 검사


def money_table(paid, received):
    money = pa.decimal128(38, 12)
    return pa.table(
        {
            "source_row_id": list(range(len(paid))),
            "source_file_hash": ["fixture-source"] * len(paid),
            "amount_paid": pa.array([Decimal(v) for v in paid], type=money),
            "amount_received": pa.array([Decimal(v) for v in received], type=money),
            "amount_paid_raw": paid,
            "amount_received_raw": received,
        }
    )


def test_numeric_20_6_audit_counts_and_preserves_source_values():
    table = money_table(
        ["0.000001", "1.2345671", "100000000000000"],
        ["1.2345670", "0.0000001", "99999999999999.999999"],
    )
    original = table.to_pydict()
    audit = new_money_audit()
    audit_service_money(table, audit)
    assert audit["target_type"] == "numeric(20,6)"
    assert audit["checked_rows"] == 3
    assert audit["incompatible_rows"] == 2
    assert audit["incompatible_values"] == 3
    assert audit["status"] == "BLOCKED_PRECISION_LOSS"
    assert [row["source_row_id"] for row in audit["examples"]] == [1, 2]
    assert table.to_pydict() == original


def test_historical_v3_audit_remains_separate_from_current_v4():
    table = money_table(["0.000001"], ["1.2300000"])
    legacy = new_money_audit(service_contract_version="dajaba-service-candidate-v3")
    current = new_money_audit()
    audit_service_money(table, legacy)
    audit_service_money(table, current)
    assert SERVICE_CONTRACT_VERSION == "dajaba-service-candidate-v4"
    assert legacy["target_type"] == "numeric(18,2)"
    assert legacy["incompatible_values"] == 1
    assert legacy["status"] == "BLOCKED_PRECISION_LOSS"
    assert current["target_type"] == "numeric(20,6)"
    assert current["incompatible_values"] == 0
    assert current["status"] == "EXACTLY_REPRESENTABLE"


def test_unknown_numeric_contract_is_rejected():
    with pytest.raises(ValueError, match="Unsupported service contract"):
        new_money_audit(service_contract_version="unknown")
    audit = new_money_audit()
    audit["target_type"] = "numeric(10,2)"
    with pytest.raises(ValueError, match="Unsupported service numeric"):
        audit_service_money(money_table(["1"], ["1"]), audit)


def handoff_rows():
    return (
        {
            "candidate_transaction_id": 1,
            "source_file_hash": "fixture-hash",
            "source_row_id": 0,
            "ts": datetime(2022, 9, 1),
            "from_account_key": "1_000A",
            "to_account_key": "2_000B",
            "from_bank_id_raw": "001",
            "to_bank_id_raw": "02",
        },
        {
            "candidate_transaction_id": 1,
            "source_file_hash": "fixture-hash",
            "source_row_id": 0,
            "transaction_uid": "ibm_hi_small:fixture-hash:0",
            "source_row_number": 1,
            "role": "TARGET",
        },
    )


def check_handoff(tx, link, version=SERVICE_CONTRACT_VERSION):
    validate_service_handoff(
        pa.Table.from_pylist([tx]),
        pa.Table.from_pylist([link]),
        service_contract_version=version,
        source_release="ibm_hi_small:fixture-hash",
    )


def test_current_handoff_preserves_bank_aliases_account_zeroes_and_original_ids():
    tx, link = handoff_rows()
    original = (dict(tx), dict(link))
    check_handoff(tx, link)
    assert (tx, link) == original


@pytest.mark.parametrize(
    "tx_changes,map_changes,error",
    [
        ({"candidate_transaction_id": 99}, {}, "candidate ID"),
        ({"candidate_transaction_id": 1.0}, {}, "candidate ID"),
        ({"candidate_transaction_id": True}, {}, "candidate ID"),
        ({"source_row_id": 0.0}, {}, "nonnegative"),
        ({"source_row_id": False}, {}, "nonnegative"),
        ({}, {"candidate_transaction_id": 1.0}, "must be integers"),
        ({}, {"candidate_transaction_id": True}, "must be integers"),
        ({}, {"source_row_id": 0.0}, "must be integers"),
        ({}, {"source_row_id": False}, "must be integers"),
        ({}, {"source_row_number": 1.0}, "one-based"),
        ({}, {"source_row_number": True}, "one-based"),
        ({"source_row_id": -1}, {}, "nonnegative"),
        ({"source_file_hash": "changed"}, {}, "provenance"),
        ({}, {"source_row_id": 1}, "provenance"),
        ({}, {"transaction_uid": "wrong"}, "file/row/UID"),
        ({}, {"source_row_number": 0}, "one-based"),
        ({}, {"role": "UNKNOWN"}, "role"),
        ({"from_bank_id_raw": "9"}, {}, "canonical"),
        ({"from_account_key": "001_000A"}, {}, "canonical"),
        ({"ts": datetime(2022, 9, 1, tzinfo=timezone.utc)}, {}, "timezone-naive"),
    ],
)
def test_current_handoff_rejects_inconsistent_provenance(tx_changes, map_changes, error):
    tx, link = handoff_rows()
    tx.update(tx_changes)
    link.update(map_changes)
    with pytest.raises(ValueError, match=error):
        check_handoff(tx, link)


@pytest.mark.parametrize(
    "name",
    [
        "transaction_id",
        "source_positive",
        "source_label_raw",
        "source_typology",
        "is_laundering",
        "Laundering_type",
        "score",
        "prediction",
    ],
)
def test_offline_service_candidate_excludes_labels_and_backend_ids(name):
    # 이 정책은 오프라인 내보내기에 적용하며 백엔드 원본 테이블에는 적용하지 않습니다.
    tx, link = handoff_rows()
    tx[name] = 1
    with pytest.raises(ValueError, match="exclude backend IDs and labels/scores"):
        check_handoff(tx, link)


def test_v3_shape_remains_supported_but_cannot_claim_current_v4():
    tx, link = handoff_rows()
    tx.pop("from_bank_id_raw")
    tx.pop("to_bank_id_raw")
    link.pop("source_row_number")
    check_handoff(tx, link, "dajaba-service-candidate-v3")
    with pytest.raises(ValueError, match="required versioned columns"):
        check_handoff(tx, link)


def test_handoff_rejects_unaligned_row_counts():
    tx, link = handoff_rows()
    with pytest.raises(ValueError, match="row alignment"):
        validate_service_handoff(
            pa.Table.from_pylist([tx]),
            pa.Table.from_pylist([link, link]),
            service_contract_version=SERVICE_CONTRACT_VERSION,
            source_release="ibm_hi_small:fixture-hash",
        )


def multi_batch_handoff(*, bad_index=None):
    count = 2 * SERVICE_VALIDATION_BATCH_ROWS + 7
    tx_rows, map_rows = [], []
    for index in range(count):
        tx, link = handoff_rows()
        for row in (tx, link):
            row["source_row_id"] = index
            row["candidate_transaction_id"] = index + 1
        link["transaction_uid"] = f"ibm_hi_small:fixture-hash:{index}"
        link["source_row_number"] = index + 1
        if index == bad_index:
            link["candidate_transaction_id"] = index + 2
        tx_rows.append(tx)
        map_rows.append(link)
    transactions = pa.Table.from_pylist(tx_rows)
    mapping = pa.Table.from_pylist(map_rows)
    tx_cut = SERVICE_VALIDATION_BATCH_ROWS - 3
    map_cut = SERVICE_VALIDATION_BATCH_ROWS + 5
    return (
        pa.concat_tables([transactions.slice(0, tx_cut), transactions.slice(tx_cut)]),
        pa.concat_tables([mapping.slice(0, map_cut), mapping.slice(map_cut)]),
    )


def test_handoff_multiple_batches_with_different_arrow_chunk_boundaries():
    transactions, mapping = multi_batch_handoff()
    assert transactions.column("source_row_id").num_chunks == 2
    assert mapping.column("source_row_id").num_chunks == 2
    assert len(transactions.column("source_row_id").chunk(0)) != len(
        mapping.column("source_row_id").chunk(0)
    )
    validate_service_handoff(
        transactions,
        mapping,
        service_contract_version=SERVICE_CONTRACT_VERSION,
        source_release="ibm_hi_small:fixture-hash",
    )


@pytest.mark.parametrize(
    "bad_index",
    [
        SERVICE_VALIDATION_BATCH_ROWS - 1,
        SERVICE_VALIDATION_BATCH_ROWS,
        SERVICE_VALIDATION_BATCH_ROWS + 1,
        2 * SERVICE_VALIDATION_BATCH_ROWS + 3,
    ],
)
def test_handoff_rejects_bad_mapping_id_at_later_batch_boundaries(bad_index):
    transactions, mapping = multi_batch_handoff(bad_index=bad_index)
    with pytest.raises(ValueError, match="provenance"):
        validate_service_handoff(
            transactions,
            mapping,
            service_contract_version=SERVICE_CONTRACT_VERSION,
            source_release="ibm_hi_small:fixture-hash",
        )


def large_handoff(ordinal, *, unsigned=False):
    tx, link = handoff_rows()
    for row in (tx, link):
        row["source_row_id"] = ordinal
        row["candidate_transaction_id"] = ordinal + 1
    link["transaction_uid"] = f"ibm_hi_small:fixture-hash:{ordinal}"
    link["source_row_number"] = ordinal + 1
    integer = pa.uint64() if unsigned else pa.int64()
    id_fields = {"source_row_id", "candidate_transaction_id", "source_row_number"}

    def typed_table(row):
        return pa.table(
            {
                key: pa.array([value], type=integer) if key in id_fields else [value]
                for key, value in row.items()
            }
        )

    return typed_table(tx), typed_table(link)


@pytest.mark.parametrize("ordinal", [0, INT64_MAX - 1])
def test_handoff_accepts_signed_bigint_candidate_boundaries(ordinal):
    transactions, mapping = large_handoff(ordinal)
    validate_service_handoff(
        transactions,
        mapping,
        service_contract_version=SERVICE_CONTRACT_VERSION,
        source_release="ibm_hi_small:fixture-hash",
    )


@pytest.mark.parametrize("ordinal", [INT64_MAX, INT64_MAX + 1])
def test_handoff_rejects_consistent_uint64_ids_above_signed_bigint(ordinal):
    # 다른 조건은 정상인 범위 초과 fixture를 Arrow로 만들기 위해 uint64를 사용합니다.
    transactions, mapping = large_handoff(ordinal, unsigned=True)
    assert transactions.schema.field("candidate_transaction_id").type == pa.uint64()
    with pytest.raises(ValueError, match="ordinal below BIGINT maximum"):
        validate_service_handoff(
            transactions,
            mapping,
            service_contract_version=SERVICE_CONTRACT_VERSION,
            source_release="ibm_hi_small:fixture-hash",
        )


@pytest.mark.parametrize(
    "field,error",
    [
        ("candidate_transaction_id", "signed BIGINT range"),
        ("source_row_id", "signed BIGINT range"),
        ("source_row_number", "one-based"),
    ],
)
def test_handoff_rejects_overflowing_uint64_mapping_columns(field, error):
    transactions, mapping = large_handoff(0)
    index = mapping.schema.get_field_index(field)
    mapping = mapping.set_column(index, field, pa.array([INT64_MAX + 1], type=pa.uint64()))
    with pytest.raises(ValueError, match=error):
        validate_service_handoff(
            transactions,
            mapping,
            service_contract_version=SERVICE_CONTRACT_VERSION,
            source_release="ibm_hi_small:fixture-hash",
        )
