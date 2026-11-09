from decimal import Decimal

import pyarrow as pa
import pytest

from dbcheck.data_contract import (
    MISSING_TOKENS,
    Currency,
    account_match_key,
    bank_alias,
    currency,
    exact_amount,
    require_money_operation,
    saml_amount_currency,
    service_account_key,
)
from dbcheck.distribution import ArrowCompute


def test_bank_padding_normalized_account_preserved_and_internal_match_is_source_local():
    assert service_account_key("010", "000ABC") == "10_000ABC"
    assert service_account_key("010", "000ABC") == service_account_key("10", "000ABC")
    assert service_account_key("000", "000ABC") == "0_000ABC"
    assert account_match_key("IBM:HI:shaA", "010", "000ABC") == account_match_key(
        "IBM:HI:shaA", "10", "000ABC"
    )
    assert account_match_key("IBM:HI:shaA", "010", "000ABC") != account_match_key(
        "IBM:LI:shaB", "010", "000ABC"
    )
    assert bank_alias("0000") == "0"
    with pytest.raises(ValueError):
        bank_alias("10x")
    with pytest.raises(ValueError):
        service_account_key("010", "A_B")


def test_saml_thirteen_decimals_round_trip_without_rounding():
    raw = "3469.0028040888333"
    amount = exact_amount(raw, 38, 16)
    assert amount == Decimal(raw)
    assert pa.scalar(amount, type=pa.decimal128(38, 16)).as_py() == Decimal(raw)
    assert exact_amount(format(amount, "f"), 38, 16) == amount
    with pytest.raises(ValueError):
        exact_amount(raw, 38, 12)
    with pytest.raises(ValueError):
        exact_amount("1.12345678901234567", 38, 16)


def test_native_currency_is_not_guessed_or_fx_converted():
    a = currency("ibm_hi_small", "Rupee", {})
    b = currency("ibm_li_small", "Rupee", {})
    assert a.iso_code is None and a.bucket != b.bucket and a.raw == "Rupee"
    require_money_operation(a, "SUM_SAME_BUCKET")
    with pytest.raises(ValueError):
        require_money_operation(a, "FX")
    with pytest.raises(ValueError):
        currency("saml_d", "UNKNOWN", {})


def test_arrow_dynamic_kernel_interface_matches_the_installed_runtime():
    import pyarrow.compute as compute

    for name in ArrowCompute.__annotations__:
        assert callable(getattr(compute, name)), name


def test_unknown_saml_amount_unit_disables_only_monetary_operations():
    unit = saml_amount_currency("saml_d", "UK pounds", "Euro", {"UK pounds": "GBP", "Euro": "EUR"})
    assert unit is None
    with pytest.raises(ValueError):
        require_money_operation(unit, "SUM_SAME_BUCKET")
    with pytest.raises(ValueError):
        require_money_operation(unit, "FX")
    same = saml_amount_currency("saml_d", "Dirham", "Dirham", {})
    assert same is not None and same.iso_code is None
    require_money_operation(same, "SUM_SAME_BUCKET")


@pytest.mark.parametrize(
    "token", sorted(MISSING_TOKENS) + ["  unconfirmed  ", " none ", " 미확정 "]
)
def test_placeholders_never_establish_currency_or_account_identity(token):
    # 잘못된 별칭 매핑이 있어도 자리표시자를 실제 통화로 바꾸면 안 됩니다.
    with pytest.raises(ValueError):
        currency("saml_d", token, {token: "USD"})
    for operation in ["SUM_SAME_BUCKET", "FX"]:
        with pytest.raises(ValueError):
            require_money_operation(Currency(token, "USD", "USD", "CONFIRMED_CODE"), operation)
    with pytest.raises(ValueError):
        service_account_key("010", token)
    with pytest.raises(ValueError):
        account_match_key("saml_d:release", "010", token)
