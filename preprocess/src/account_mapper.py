"""DART 계정과목명 → 표준 키 매핑.

OpenDART에서 반환하는 account_nm은 기업마다 표현이 다를 수 있다.
이 모듈은 다양한 변형을 표준 키로 통합한다.

수정 이력
─────────
v2: bonds_payable 패턴 완화 (^사채$ → 사채$, 변형 계정명 대응)
    interest_expense CF 패턴 추가 (이자비용을 CF에만 기재하는 기업 대응)
    capital_surplus 주식발행초과금 패턴 추가 (자본잉여금 총계 없는 기업 대응)
    trade_receivables 매출채권및기타채권 패턴 추가
"""

from __future__ import annotations

import re
from typing import Any

ACCOUNT_PATTERNS: list[tuple[str, str | None, str]] = [
    # ─── BS ───────────────────────────────────────────────────
    ("total_assets",          "BS", r"자산\s*총계"),
    ("current_assets",        "BS", r"유동\s*자산$"),
    ("non_current_assets",    "BS", r"비유동\s*자산$"),
    ("tangible_assets",       "BS", r"유형\s*자산$"),
    ("intangible_assets",     "BS", r"무형\s*자산$|영업권\s*이외의\s*무형자산"),
    # 매출채권 — "매출채권및기타채권" 같은 변형도 포함
    ("trade_receivables",     "BS", r"매출\s*채권|단기매출채권"),
    ("inventories",           "BS", r"재고\s*자산$"),
    ("cash",                  "BS", r"현금\s*(및|과)\s*현금\s*성?\s*자산"),
    ("total_liabilities",     "BS", r"부채\s*총계"),
    ("current_liabilities",   "BS", r"유동\s*부채$"),
    ("short_term_borrowings", "BS", r"단기\s*차입금"),
    ("long_term_borrowings",  "BS", r"장기\s*차입금"),
    # [수정 v2] ^사채$ → 사채$ : "유동성사채", "전환사채" 등 변형 대응
    ("bonds_payable",         "BS", r"사채$"),
    ("total_equity",          "BS", r"자본\s*총계"),
    ("paid_in_capital",       "BS", r"^자본금$|납입\s*자본"),
    ("retained_earnings",     "BS", r"이익\s*잉여금"),
    # [수정 v2] 주식발행초과금 추가 : 자본잉여금 총계가 없고 하위 항목만 있는 기업 대응
    ("capital_surplus",       "BS", r"자본\s*잉여금|주식\s*발행\s*초과금"),

    # ─── IS ───────────────────────────────────────────────────
    ("revenue",               "IS", r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("cost_of_sales",         "IS", r"매출\s*원가"),
    ("gross_profit",          "IS", r"매출\s*총이익|매출\s*총\s*손익"),
    ("operating_income",      "IS", r"영업\s*이익|영업\s*손익"),
    ("net_income",            "IS", r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
    ("interest_expense",      "IS", r"이자\s*비용"),

    # ─── CIS ──────────────────────────────────────────────────
    ("revenue",               "CIS", r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("cost_of_sales",         "CIS", r"매출\s*원가"),
    ("gross_profit",          "CIS", r"매출\s*총이익|매출\s*총\s*손익"),
    ("operating_income",      "CIS", r"영업\s*이익|영업\s*손익"),
    ("net_income",            "CIS", r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
    ("interest_expense",      "CIS", r"이자\s*비용"),

    # ─── CF ───────────────────────────────────────────────────
    ("depreciation",          "CF", r"유형\s*자산\s*감가\s*상각비|감가\s*상각비"),
    ("amortization",          "CF", r"무형\s*자산\s*상각비|무형자산상각비"),
    # [수정 v2] IS/CIS에 이자비용이 없고 CF에만 기재하는 기업 대응
    ("interest_expense",      "CF", r"이자\s*지급|이자\s*비용"),
]


def _parse_amount(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace(" ", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_standard_items(
    dart_items: list[dict[str, Any]],
) -> dict[str, dict[str, float | None]]:
    """
    DART 재무제표 항목 리스트 → 표준 키별 금액 추출.

    Returns:
        {
          "standard_key": {
            "thstrm":    당기 금액,
            "frmtrm":    전기 금액,
            "bfefrmtrm": 전전기 금액,
          },
          ...
        }
    """
    result: dict[str, dict[str, float | None]] = {}
    matched_keys: set[str] = set()

    compiled = [
        (key, sj_div, re.compile(pattern))
        for key, sj_div, pattern in ACCOUNT_PATTERNS
    ]

    for item in dart_items:
        account_nm = (item.get("account_nm") or "").strip()
        sj_div     = (item.get("sj_div") or "").strip()
        if not account_nm:
            continue

        for std_key, filter_sj, regex in compiled:
            if std_key in matched_keys:
                continue
            if filter_sj and sj_div != filter_sj:
                continue
            if regex.search(account_nm):
                result[std_key] = {
                    "thstrm":    _parse_amount(item.get("thstrm_amount")),
                    "frmtrm":    _parse_amount(item.get("frmtrm_amount")),
                    "bfefrmtrm": _parse_amount(item.get("bfefrmtrm_amount")),
                }
                matched_keys.add(std_key)
                break

    return result
    