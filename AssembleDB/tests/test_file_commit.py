"""제어 JSON의 제한 재시도는 이전 파일 보존과 모호한 교체 거부를 유지합니다."""

import json
from pathlib import Path

import pytest

from assemble_db import file_commit as commit


def denied(code=5):
    error = PermissionError("injected JSON replacement")
    error.winerror = code
    return error


@pytest.fixture
def files(tmp_path, monkeypatch):
    temporary, target = tmp_path / "record.partial", tmp_path / "record.json"
    temporary.write_text('{"new":true}', encoding="utf-8")
    target.write_text('{"old":true}', encoding="utf-8")
    waits = []
    monkeypatch.setattr(commit, "_WINDOWS", True)
    monkeypatch.setattr(commit, "sleep", waits.append)
    return temporary, target, waits


def test_retry_reuses_the_same_closed_bytes(files, monkeypatch):
    temporary, target, waits = files
    original, calls = Path.replace, []

    def replace(path, destination):
        calls.append(path)
        assert json.loads(path.read_text(encoding="utf-8")) == {"new": True}
        if len(calls) == 1:
            raise denied()
        return original(path, destination)

    monkeypatch.setattr(Path, "replace", replace)
    commit.replace_closed_json(temporary, target)
    assert len(calls) == 2 and len(set(calls)) == 1 and waits == [0.05]
    assert json.loads(target.read_text(encoding="utf-8")) == {"new": True}


def test_permanent_failure_preserves_previous_and_partial(files, monkeypatch):
    temporary, target, waits = files
    error, calls = denied(), []

    def replace(path, destination):
        calls.append(path)
        raise error

    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(PermissionError) as caught:
        commit.replace_closed_json(temporary, target)
    assert caught.value is error and len(calls) == 4 and waits == [0.05, 0.1, 0.2]
    assert json.loads(target.read_text(encoding="utf-8")) == {"old": True}
    assert temporary.exists()


@pytest.mark.parametrize("windows,code", [(False, 5), (True, 32), (True, 2), (True, None)])
def test_other_error_or_platform_is_single_attempt(files, monkeypatch, windows, code):
    temporary, target, waits = files
    error, calls = denied(code), []
    monkeypatch.setattr(commit, "_WINDOWS", windows)

    def replace(path, destination):
        calls.append(path)
        raise error

    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(PermissionError) as caught:
        commit.replace_closed_json(temporary, target)
    assert caught.value is error and len(calls) == 1 and waits == []


@pytest.mark.parametrize(
    "changed", ["destination", "partial", "missing_partial", "already_replaced"]
)
def test_changed_or_ambiguous_state_is_not_retried(files, monkeypatch, changed):
    temporary, target, waits = files
    original, error, calls = Path.replace, denied(), []

    def replace(path, destination):
        calls.append(path)
        if changed == "destination":
            destination.write_text('{"other":true}', encoding="utf-8")
        elif changed == "partial":
            path.write_text('{"other_partial":true}', encoding="utf-8")
        elif changed == "missing_partial":
            path.unlink()
        else:
            original(path, destination)
        raise error

    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(PermissionError) as caught:
        commit.replace_closed_json(temporary, target)
    assert caught.value is error and len(calls) == 1 and waits == []


def test_elapsed_deadline_stops_before_next_attempt(files, monkeypatch):
    temporary, target, waits = files
    now, calls = [0.0], []
    monkeypatch.setattr(commit, "monotonic", lambda: now[0])

    def replace(path, destination):
        calls.append(path)
        now[0] = 0.99
        raise denied()

    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(PermissionError):
        commit.replace_closed_json(temporary, target)
    assert len(calls) == 1 and waits == []
