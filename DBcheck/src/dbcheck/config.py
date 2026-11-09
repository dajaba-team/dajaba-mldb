"""설정 파일 기준의 상대 경로를 명시하며 모델 설정은 포함하지 않습니다."""

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

CONTRACT_VERSION = "dajaba-source-validation-v2"


@dataclass(frozen=True)
class Source:
    id: str
    kind: str
    path: Path
    label_semantics: str
    accounts: Path | None = None
    patterns: Path | None = None
    expected_start: str | None = None
    expected_end: str | None = None
    amount_precision: int = 38
    amount_scale: int = 12


@dataclass(frozen=True)
class Config:
    path: Path
    output: Path
    sources: tuple[Source, ...]
    currency_aliases: dict[str, str]
    config_sha256: str
    aliases_sha256: str
    threads: int


def load(path: Path) -> Config:
    path = path.resolve()
    raw = path.read_bytes()
    data = tomllib.loads(raw.decode("utf-8-sig"))
    if set(data) - {"schema_version", "output_root", "currency_map", "threads", "sources"}:
        raise ValueError("Unsupported config keys: former ML configuration is retired")
    if data.get("schema_version") != CONTRACT_VERSION:
        raise ValueError(f"Expected schema_version={CONTRACT_VERSION}")

    def resolve(text):
        return (path.parent / text).resolve()

    alias_bytes = resolve(data["currency_map"]).read_bytes()
    aliases = json.loads(alias_bytes.decode("utf-8-sig"))["confirmed"]
    if not isinstance(aliases, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in aliases.items()
    ):
        raise ValueError("Confirmed aliases must map strings to strings")
    sources = []
    for item in data["sources"]:
        if set(item) - {
            "id",
            "kind",
            "path",
            "label_semantics",
            "accounts",
            "patterns",
            "expected_start",
            "expected_end",
            "amount_precision",
            "amount_scale",
        }:
            raise ValueError("Unknown source config key")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item["id"]) or item["kind"] not in (
            "ibm",
            "saml",
        ):
            raise ValueError("Source needs a safe lowercase id and explicit ibm/saml kind")
        semantic = (
            "AMLWORLD_ILLICIT_FLOW" if item["kind"] == "ibm" else "SOURCE_DESIGNATED_SUSPICIOUS"
        )
        if item["label_semantics"] != semantic:
            raise ValueError("Incorrect source label semantics")
        sources.append(
            Source(
                item["id"],
                item["kind"],
                resolve(item["path"]),
                semantic,
                resolve(item["accounts"]) if item.get("accounts") else None,
                resolve(item["patterns"]) if item.get("patterns") else None,
                item.get("expected_start"),
                item.get("expected_end"),
                item.get("amount_precision", 38),
                item.get("amount_scale", 12),
            )
        )
    for source in sources:
        if (
            type(source.amount_precision) is not int
            or type(source.amount_scale) is not int
            or not 1 <= source.amount_precision <= 38
            or not 0 <= source.amount_scale <= source.amount_precision
        ):
            raise ValueError("Invalid source-specific exact Decimal contract")
    if not sources or len({s.id for s in sources}) != len(sources):
        raise ValueError("Sources must be nonempty and unique")
    output = resolve(data["output_root"])
    for s in sources:
        for p in (s.path, s.accounts, s.patterns):
            if p and (output == p or output.is_relative_to(p.parent)):
                raise ValueError("Reports must be outside every original source directory")
    threads = data.get("threads", 1)
    if type(threads) is not int or not 1 <= threads <= 4:
        raise ValueError("Use 1..4 worker threads")
    return Config(
        path,
        output,
        tuple(sources),
        aliases,
        hashlib.sha256(raw).hexdigest(),
        hashlib.sha256(alias_bytes).hexdigest(),
        threads,
    )
