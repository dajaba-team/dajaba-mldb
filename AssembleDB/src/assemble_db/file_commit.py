"""닫힌 JSON의 교체만 제한 재시도하며 불확실한 상태는 실패로 남깁니다."""

import logging
import os
from pathlib import Path
from time import monotonic, sleep

_WINDOWS = os.name == "nt"
_DELAYS = (0.05, 0.1, 0.2)
LOG = logging.getLogger(__name__)


def _stamp(path):
    info = path.lstat()
    return info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns


def _state(temporary, destination):
    parent = destination.parent.lstat()
    try:
        target = _stamp(destination)
    except FileNotFoundError:
        target = None
    return (parent.st_dev, parent.st_ino), _stamp(temporary), target


def replace_closed_json(temporary: Path, destination: Path) -> None:
    expected, started = _state(temporary, destination), monotonic()

    def unchanged(failure):
        try:
            current = _state(temporary, destination)
        except OSError as error:
            failure.add_note("Control JSON replacement state uncertain; no further retry")
            raise failure from error
        if current != expected:
            failure.add_note("Control JSON replacement state changed; no further retry")
            raise failure

    for attempt in range(len(_DELAYS) + 1):
        try:
            temporary.replace(destination)
            return
        except PermissionError as error:
            if not _WINDOWS or getattr(error, "winerror", None) != 5:
                raise
            unchanged(error)
            if attempt == len(_DELAYS):
                error.add_note(
                    "Control JSON retry count exhausted; previous and partial files preserved"
                )
                raise
            delay = _DELAYS[attempt]
            if monotonic() - started + delay >= 1.0:
                error.add_note("Control JSON retry start deadline reached; partial preserved")
                raise
            LOG.warning(
                "JSON 교체 WinError 5: %s, 재시도 %s/3, 대기 %.2fs",
                destination.name,
                attempt + 1,
                delay,
            )
            sleep(delay)
            unchanged(error)
            # 1초는 다음 시도 시작까지이며 OS 호출 소요 시간의 상한이 아닙니다.
            if monotonic() - started >= 1.0:
                error.add_note("Control JSON retry start deadline reached; partial preserved")
                raise
