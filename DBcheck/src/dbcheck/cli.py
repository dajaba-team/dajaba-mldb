"""검증 전용 CLI입니다. 과거의 모델 학습 명령은 의도적으로 제공하지 않습니다."""

import argparse
import logging
from pathlib import Path

from .config import load
from .pipeline import execute


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="DBCheck v2: read-only independent source validation; no ML"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser(
        "check", help="Full original quality and temporal distribution checks"
    )
    check.add_argument("--config", type=Path, required=True)
    check.add_argument("--source", action="append", help="Repeat to select independent sources")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = load(args.config)
        root, success = execute(config, args.source)
        print(f"VALIDATION_DIRECTORY={root}", flush=True)
        return 0 if success else 2
    except KeyboardInterrupt:
        logging.error("Interrupted. A RUNNING report is incomplete; rerun into a new directory.")
        return 130
    except (OSError, ValueError, KeyError) as exc:
        logging.error("Configuration/validation error: %s. Check config and source paths.", exc)
        return 2
