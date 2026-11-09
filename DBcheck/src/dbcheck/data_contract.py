"""향후 조립 어댑터와 공유할, 원본을 보존하는 금액·식별자 규칙입니다.

원천을 다시 쓰거나 통화를 추정하거나 관계를 임의로 만들지 않습니다.
서비스 은행 ID는 정규화하며 원본 은행 ID와 계좌번호는 보존합니다.
"""

import json
import re
from dataclasses import dataclass
from decimal import Decimal, localcontext

POLICY_VERSION = "source-preserving-money-identity-v2"

# 프로파일링·식별자 검사·금액 연산에서 같은 용어 집합을 사용합니다.
UNKNOWN_TOKENS = frozenset({"UNKNOWN", "UNCONFIRMED", "미확정"})
NA_TOKENS = frozenset({"N/A", "NA", "NULL", "NONE", "NOT APPLICABLE"})
MISSING_TOKENS = UNKNOWN_TOKENS | NA_TOKENS | {""}


def is_unresolved(raw: str) -> bool:
    return raw.strip().upper() in MISSING_TOKENS


def bank_alias(raw: str) -> str:
    """매칭과 확정된 서비스 키에 사용할 숫자 은행 코드의 표준 표기입니다."""
    if not re.fullmatch(r"[0-9]+", raw):
        raise ValueError("Bank identity must contain only original numeric characters")
    return raw.lstrip("0") or "0"


def service_account_key(bank_raw: str, account_raw: str) -> str:
    """은행 코드의 패딩만 정규화하고 계좌번호의 모든 문자는 보존합니다."""
    bank_id = bank_alias(bank_raw)
    if is_unresolved(account_raw) or "_" in account_raw:
        raise ValueError("External account key is empty or ambiguous; explicit mapping required")
    return f"{bank_id}_{account_raw}"


def account_match_key(source_release: str, bank_raw: str, account_raw: str) -> str:
    """모호하지 않은 내부 비교 키입니다. 마스터의 유일성은 호출자가 검증해야 합니다."""
    if not source_release or is_unresolved(account_raw):
        raise ValueError("A source release and original account are required")
    return json.dumps([source_release, bank_alias(bank_raw), account_raw], separators=(",", ":"))


def exact_amount(raw: str, precision: int, scale: int) -> Decimal:
    """정확한 Decimal을 반환하거나 실패합니다. 끝이 0이어도 초과 소수 자릿수를 검사합니다."""
    if not 1 <= precision <= 38 or not 0 <= scale <= precision:
        raise ValueError("Unsupported Decimal contract")
    if not re.fullmatch(r"[+]?[0-9]+(?:\.[0-9]+)?", raw):
        raise ValueError("Amount is not an unsigned plain decimal")
    integral, _, fractional = raw.lstrip("+").partition(".")
    if len(integral.lstrip("0")) > precision - scale or len(fractional) > scale:
        raise ValueError("Amount exceeds exact Decimal contract; do not round")
    with localcontext() as context:
        context.prec = precision + 2
        return Decimal(raw).quantize(Decimal(1).scaleb(-scale))


@dataclass(frozen=True)
class Currency:
    raw: str
    bucket: str
    iso_code: str | None
    status: str


def currency(source_id: str, raw: str, aliases: dict[str, str]) -> Currency:
    """미확인 통화명도 원천 내 단위로 보존하되 환율 환산은 금지합니다."""
    if not source_id or is_unresolved(raw):
        raise ValueError("Currency is missing or unresolved as a source unit")
    code = aliases.get(raw)
    if code:
        return Currency(raw, code, code, "CONFIRMED_CODE")
    return Currency(
        raw, json.dumps([source_id, raw], separators=(",", ":")), None, "SOURCE_NATIVE_ONLY_NO_FX"
    )


def saml_amount_currency(
    source_id: str, paid_raw: str, received_raw: str, aliases: dict[str, str]
) -> Currency | None:
    """통화 쌍이 다르면 단일 Amount가 어느 통화 단위인지 판단할 근거가 없습니다."""
    paid = currency(source_id, paid_raw, aliases)
    received = currency(source_id, received_raw, aliases)
    return paid if paid.bucket == received.bucket else None


def require_money_operation(unit: Currency | None, operation: str) -> None:
    """누락된 금액 의미를 만들어 내지 않고 합산과 환율 환산의 허용 여부를 제한합니다."""
    if operation not in {"SUM_SAME_BUCKET", "FX"}:
        raise ValueError("Unknown monetary operation")
    if unit is None:
        raise ValueError("Amount currency basis is not established; sums and FX prohibited")
    if is_unresolved(unit.raw):
        raise ValueError("Currency placeholder cannot establish a unit; sums and FX prohibited")
    if operation == "FX" and (unit.iso_code is None or unit.iso_code.startswith("CRYPTO:")):
        raise ValueError("No confirmed supported fiat code; FX prohibited")
