"""Service policy parsing stays separate from file loading and DB execution."""

from datetime import datetime

import pytest

from assemble_db.config import ServiceSelection, load_service_selection


def test_disabled_service_has_no_selection():
    assert load_service_selection({}) is None


def test_full_source_keeps_unbounded_selection():
    assert load_service_selection({"enabled": True, "mode": "full_source"}) == ServiceSelection(
        datetime.min, datetime.max, datetime.min, "full_source"
    )


def test_random_rows_keeps_count_and_seed():
    selection = load_service_selection(
        {"enabled": True, "mode": "random_rows", "count": 10000, "seed": 17}
    )
    assert selection == ServiceSelection(
        datetime.min, datetime.max, datetime.min, "random_rows", 10000, 17
    )


def test_time_window_keeps_explicit_history():
    selection = load_service_selection({
        "enabled": True, "start": "2022-09-02", "end": "2022-09-03",
        "history_start": "2022-09-01",
    })
    assert selection == ServiceSelection(
        datetime(2022, 9, 2), datetime(2022, 9, 3), datetime(2022, 9, 1)
    )


@pytest.mark.parametrize("spec, message", [
    ({"unknown": 1}, "Unknown service"),
    ({"source": "saml_d"}, "HI-Small only"),
    ({"enabled": True, "mode": "full_source", "seed": 42}, "must not mix"),
    ({"enabled": True, "mode": "random_rows", "count": True}, "integer count"),
    ({"enabled": True, "mode": "random_rows", "count": 10, "start": "2022-09-01"},
     "must not mix"),
    ({"enabled": True, "mode": "time_window"}, "requires explicit"),
    ({"enabled": True, "mode": "other"}, "Service mode"),
])
def test_invalid_service_policies_still_fail(spec, message):
    with pytest.raises(ValueError, match=message):
        load_service_selection(spec)
