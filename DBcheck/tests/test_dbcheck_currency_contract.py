import json
from pathlib import Path

from dbcheck.data_contract import currency, require_money_operation


def test_team_confirmed_aliases_and_unregistered_native_currency():
    data = json.loads((Path(__file__).parents[1] / "configs/currencies.json").read_text())
    expected = {"Yuan": "CNY", "Shekel": "ILS", "Ruble": "RUB", "Rupee": "INR"}
    for raw, code in expected.items():
        unit = currency("ibm_hi_small", raw, data["confirmed"])
        assert unit.raw == raw and unit.iso_code == code
        assert raw not in data["unverified"]
    assert currency("ibm_hi_small", "Bitcoin", data["confirmed"]).bucket == "CRYPTO:BTC"
    unit = currency("saml_d", "Dirham", data["confirmed"])
    assert unit.iso_code is None and unit.raw == "Dirham"
    require_money_operation(unit, "SUM_SAME_BUCKET")
