"""Source validation reports and summaries; no source scanning or execution."""

from pathlib import Path

from .checks import rule
from .config import CONTRACT_VERSION, Config, Source
from .data_contract import POLICY_VERSION


def _build_source_report(
    source: Source,
    config: Config,
    raw: dict,
    details: dict,
    code_digest: str,
    *,
    pattern_membership: dict | None = None,
) -> dict:
    rules = details["rules"]
    if raw["rows"] == 0:
        rules.append(
            rule(
                "EMPTY_SOURCE",
                "FAIL",
                1,
                "Provide a nonempty source; header-only files cannot be accepted",
                count_unit="sources",
            )
        )
    q = raw["quality"]
    rules.extend(
        [
            rule(
                "TIMESTAMP_PARSING",
                "FAIL",
                q["timestamp_parse_failure"],
                "Preserve original text; quarantine downstream only",
            ),
            rule(
                "BINARY_LABEL",
                "FAIL",
                q["invalid_label"],
                "Never predict, impute or rewrite source labels",
            ),
            rule(
                "TIME_FILE_ORDER",
                "WARN",
                q["file_order_time_regressions"],
                "File order is not chronology; use parsed time plus stable row identity",
            ),
            rule(
                "TIME_OBSERVATION_GAPS",
                "WARN",
                raw["no_observation_hours"],
                "Review gaps; never fabricate missing transactions",
                count_unit="hours",
            ),
            rule(
                "UNKNOWN_TOKENS",
                "WARN",
                sum(raw["unknown_token_cells"].values()),
                "Keep explicit missingness status",
                count_unit="cells",
            ),
            rule(
                "NOT_APPLICABLE_TOKENS",
                "WARN",
                sum(raw["na_token_cells"].values()),
                "Separate structural absence from parse failures",
                count_unit="cells",
            ),
        ]
    )
    if source.kind == "ibm":
        for i in (1, 3):
            rules.append(
                rule(
                    f"BANK_{i}_FORMAT",
                    "FAIL",
                    q[f"bank_{i}_non_digit"],
                    "Inspect raw bank identifier; no invented replacement",
                )
            )
    if source.kind == "saml":
        rules.append(
            rule(
                "SAML_CURRENCY_BASIS_UNRESOLVED",
                "WARN",
                q["different_payment_receiving_currency"],
                "Resolved safe contract: retain single Amount and currency pair; monetary sums/FX disabled where amount currency basis is not established; counts/time/topology retained",
                policy_version=POLICY_VERSION,
                internal_resolution="PRESERVE_ROW_DISABLE_UNSUPPORTED_MONETARY_OPERATIONS",
            )
        )
    rules.append(
        rule(
            "NORMAL_VOLUME_DROP_CANDIDATE",
            "WARN",
            sum(c["rows"] for c in raw["distribution_candidates"]),
            "Review generator period; keep all rows pending explicit exclusion approval",
        )
    )
    status = (
        "FAIL"
        if any(r["status"] == "FAIL" for r in rules)
        else "WARN"
        if any(r["status"] == "WARN" for r in rules)
        else "PASS"
    )
    result = dict(
        contract_version=CONTRACT_VERSION,
        status=status,
        source_id=source.id,
        input=dict(path=str(source.path), sha256=raw["sha256"], rows=raw["rows"]),
        auxiliary_inputs=details["auxiliary_inputs"],
        exact_amount_contract=details["exact_amount_contract"],
        bank_identity_bridge=details["bank_identity_bridge"],
        money_identity_policy_version=POLICY_VERSION,
        currency_contract=dict(
            confirmed_iso_or_source_native_bucket=True,
            raw_currency_preserved=True,
            unverified_iso_code=None,
            unverified_fx_allowed=False,
            aggregate_grouping=["source_id", "currency_bucket"],
            saml_mismatched_currency_amount_basis="UNSPECIFIED",
            saml_mismatched_monetary_operations_allowed=False,
        ),
        config_sha256=config.config_sha256,
        aliases_sha256=config.aliases_sha256,
        code_sha256=code_digest,
        scope="FULL_ORIGINAL",
        source_unchanged=True,
        rows_changed=0,
        rows_deleted=0,
        checks=rules,
        distribution_profile=source.id + "_profile.json",
        full_scan_complete=True,
        not_checked=["pattern membership matching", "actual PostgreSQL COPY acceptance"],
        next_module="AssembleDB; WARN requires policy acknowledgement; FAIL blocks this source only",
    )
    if pattern_membership is not None:
        result["pattern_membership"] = pattern_membership
        if pattern_membership["status"] in {"VERIFIED", "INCOMPLETE"}:
            result["not_checked"].remove("pattern membership matching")
    return result


def _expected_counts(source: Source, raw: dict, details: dict, status: str) -> dict:
    # 관측한 원천 건수는 비교 근거이며 DB 적재 승인 결과가 아닙니다.
    return dict(
        version="dajaba-source-counts-v1",
        source_id=source.id,
        source_sha256=raw["sha256"],
        transaction_rows=raw["rows"],
        source_label_counts=raw["labels"],
        label_counts_scope="OFFLINE_VALIDATION_ONLY; never inference input",
        daily_rows={item["day"]: item["rows"] for item in raw["daily"]},
        currency_pair_rows=raw["currency_pairs"],
        auxiliary_rows={item["role"]: item.get("rows") for item in details["auxiliary_inputs"]},
        timestamp_min=raw["timestamp_min"],
        timestamp_max=raw["timestamp_max"],
        timezone="UNKNOWN",
        scope="FULL_ORIGINAL",
        validation_status=status,
        backend_copy_verified=False,
        rule_expectations=None,
    )


def _write_source_summary(
    folder: Path, source: Source, raw: dict, status: str, rules: list[dict]
) -> None:
    lines = [
        f"# {source.id}: {status}",
        "",
        f"전체 원본 {raw['rows']:,}건. 변경·삭제 0건.",
        "",
        "| 규칙 | 판정 | 영향 건수 | 후속 조치 |",
        "|---|---|---:|---|",
    ]
    lines += [
        f"| {r['rule']} | {r['status']} | {r['affected_rows'] if r['affected_rows'] is not None else '미확정'} | {r['action']} |"
        for r in rules
    ]
    (folder / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
