"""조립·검증·단계별 생성을 명시적으로 실행하며 학습이나 실제 DB 쓰기는 하지 않습니다."""

import argparse
import logging
from pathlib import Path

from .config import load
from .pipeline import execute, verify_run


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="기존 원천별 세 형식 조립")
    build.add_argument(
        "--config", type=Path, default=Path(__file__).resolve().parents[2] / "configs/default.toml"
    )
    build.add_argument("--validation-run", type=Path, required=True)
    build.add_argument("--purpose", choices=["ml", "service", "all"], default="all")
    build.add_argument("--source", action="append")
    build.add_argument("--resume", type=Path)
    verify = sub.add_parser("verify", help="선언된 출력의 형식·타입·값·지문 재검증")
    verify.add_argument("--run", type=Path, required=True)
    audit = sub.add_parser("audit", help="원본과 기존 조립 결과의 독립 감사")
    audit.add_argument("--config", type=Path, required=True)
    audit.add_argument("--run", type=Path, required=True)
    audit.add_argument("--output", type=Path, required=True)
    audit.add_argument("--source", action="append")
    staged = sub.add_parser(
        "staged-build", help="커밋된 정형화 CSV·JSON 두 형식 다음 ML Parquet과 HI 서비스 v4 생성"
    )
    staged.add_argument("--config", type=Path, required=True)
    staged.add_argument("--validation-run", type=Path, required=True)
    staged.add_argument("--canonical-run", type=Path, required=True)
    staged.add_argument("--reuse-proof", type=Path, required=True)
    staged.add_argument("--proof-execution", type=Path, required=True)
    staged.add_argument("--check-config-snapshot", type=Path)
    staged.add_argument("--output-root", type=Path)
    staged.add_argument("--source", action="append")
    staged.add_argument(
        "--canonical-format", action="append", help="source_id=csv+json; 두 형식은 항상 함께 생성"
    )
    staged.add_argument("--resume", type=Path, help="호환되는 새 staged run의 완료 원천만 재사용")
    staged_verify = sub.add_parser("staged-verify", help="단계별 입력 참조와 새 산출물 재검증")
    staged_verify.add_argument("--run", type=Path, required=True)
    staged_audit = sub.add_parser(
        "staged-audit", help="보존된 실제 DBcheck 설정으로 새 산출물 독립 감사"
    )
    staged_audit.add_argument("--config", type=Path, required=True)
    staged_audit.add_argument("--run", type=Path, required=True)
    staged_audit.add_argument("--output", type=Path, required=True)
    staged_audit.add_argument("--source", action="append")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.command == "build":
            folder, success = execute(
                load(args.config), args.validation_run, args.purpose, args.source, args.resume
            )
            print(f"ASSEMBLY_DIRECTORY={folder}", flush=True)
            return 0 if success else 2
        if args.command == "staged-build":
            from .staged import execute_staged

            folder, success = execute_staged(
                load(args.config),
                args.validation_run,
                args.canonical_run,
                args.reuse_proof,
                args.proof_execution,
                output_root=args.output_root,
                check_snapshot=args.check_config_snapshot,
                selected=args.source,
                canonical_formats=args.canonical_format,
                resume=args.resume,
            )
            print(f"ASSEMBLY_DIRECTORY={folder}", flush=True)
            print(f"ASSEMBLY_INDEX={folder / 'assembly_index.json'}", flush=True)
            return 0 if success else 2
        if args.command in {"audit", "staged-audit"}:
            if args.command == "staged-audit":
                from .staged import audit_staged

                result = audit_staged(
                    load(args.config), args.run.resolve(), args.output, args.source
                )
            else:
                from .audit import audit_run

                result = audit_run(load(args.config), args.run, args.output, args.source)
            print(f"AUDIT={result['status']}", flush=True)
            print(f"AUDIT_REPORT={args.output.resolve() / 'audit.json'}", flush=True)
            return 0
        if args.command == "staged-verify":
            from .staged import verify_staged

            result = verify_staged(args.run)
        else:
            result = verify_run(args.run.resolve())
        print(f"VERIFICATION={result['status']}", flush=True)
        return 0
    except KeyboardInterrupt:
        logging.error(
            "중단됐습니다. 완료·미완료 파일을 보존하며 재개는 명시적으로 요청해야 합니다."
        )
        return 130
    except (ValueError, OSError, KeyError) as exc:
        logging.error("Assembly error: %s", exc)
        return 2
