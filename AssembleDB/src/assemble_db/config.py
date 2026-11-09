"""파일 기준 경로와 출력·HI 발췌 정책을 명시합니다."""

import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dbcheck.config import Config as CheckConfig
from dbcheck.config import load as load_check

VERSION = "dajaba-assembly-v2"


@dataclass(frozen=True)
class ServiceSelection:
    start: datetime
    end: datetime
    history_start: datetime
    mode: str = "time_window"
    count: int | None = None
    seed: int = 42


@dataclass(frozen=True)
class Config:
    path: Path
    check: CheckConfig
    output: Path
    batch_rows: int
    acknowledged_warnings: frozenset[str]
    service: ServiceSelection | None


def load_service_selection(spec: dict) -> ServiceSelection | None:
    """Parse only the service excerpt policy; preserve its existing validation."""
    if set(spec) - {"enabled", "source", "start", "end", "history_start", "mode", "count", "seed"}:
        raise ValueError("Unknown service excerpt setting")
    if spec.get("source", "ibm_hi_small") != "ibm_hi_small":
        raise ValueError("Service data must come from HI-Small only")
    service = None
    if spec.get("enabled", False):
        mode = spec.get("mode", "time_window")
        if mode == "full_source":
            if {"start", "end", "history_start", "count", "seed"} & set(spec):
                raise ValueError(
                    "Full-source service must not mix sampling or time-window settings"
                )
            service = ServiceSelection(datetime.min, datetime.max, datetime.min, mode)
        elif mode == "random_rows":
            count, seed = spec.get("count"), spec.get("seed", 42)
            if type(count) is not int or not 1 <= count <= 100000 or type(seed) is not int:
                raise ValueError(
                    "Random service excerpt requires integer count 1..100000 and integer seed"
                )
            if {"start", "end", "history_start"} & set(spec):
                raise ValueError("Random service excerpt must not mix time-window settings")
            service = ServiceSelection(datetime.min, datetime.max, datetime.min, mode, count, seed)
        elif mode == "time_window":
            if not {"start", "end", "history_start"} <= set(spec):
                raise ValueError("Service excerpt requires explicit start, end and history_start")
            start, end, history = [
                datetime.fromisoformat(spec[k]) for k in ("start", "end", "history_start")
            ]
            if (
                any(t.tzinfo is not None for t in [start, end, history])
                or not history <= start < end
            ):
                raise ValueError("Use timezone-naive history_start <= start < end")
            service = ServiceSelection(start, end, history)
        else:
            raise ValueError("Service mode must be full_source, random_rows or time_window")
    return service


def load(path: Path) -> Config:
    path = path.resolve()
    data = tomllib.loads(path.read_text(encoding="utf-8-sig"))
    if set(data) - {
        "schema_version",
        "dbcheck_config",
        "output_root",
        "batch_rows",
        "acknowledged_warnings",
        "service",
    }:
        raise ValueError("Unknown assembly config key; legacy model gates are retired")
    if data.get("schema_version") != VERSION:
        raise ValueError(f"Expected schema_version={VERSION}")
    check = load_check((path.parent / data["dbcheck_config"]).resolve())
    output = (path.parent / data["output_root"]).resolve()
    for source in check.sources:
        for raw in [source.path, source.accounts, source.patterns]:
            if raw and (output == raw or output.is_relative_to(raw.parent)):
                raise ValueError("Assembly output must be outside every original directory")
    rows = data.get("batch_rows", 25000)
    if type(rows) is not int or not 1 <= rows <= 100000:
        raise ValueError("batch_rows must be 1..100000")
    warnings = data.get("acknowledged_warnings", [])
    if not isinstance(warnings, list) or not all(isinstance(x, str) for x in warnings):
        raise ValueError("acknowledged_warnings must list explicit rule codes")
    service = load_service_selection(data.get("service", {}))
    return Config(path, check, output, rows, frozenset(warnings), service)
