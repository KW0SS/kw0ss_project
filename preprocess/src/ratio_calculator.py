"""30개 재무비율 계산기.

이미지에 정의된 재무비율을 표준 키 기반 재무 항목으로부터 계산한다.

카테고리별 비율 목록
──────────────────
[성장성]  5개   총자산증가율 ~ 영업이익증가율
[수익성]  3개   매출액순이익률 ~ 자기자본순이익률
[활동성]  5개   매출채권회전율 ~ 매출원가율
[안정성] 13개   부채비율 ~ 감가상각비  (유형/무형자산 값 포함)
[가치평가] 4개  총자본영업이익률 ~ 총자본투자효율

수정 이력
─────────
v2: 유형자산회전율 공식 수정 (매출액/총자산 → 매출액/유형자산)
    비유동자산장기적합률 공식 수정 (분모: 장기차입금 → 자기자본+장기차입금)
    차입금의존도 None 처리 보완 (전체 누락 시 None 반환)
"""

from __future__ import annotations

from typing import Any

Items = dict[str, dict[str, float | None]]


def _get(items: Items, key: str, period: str = "thstrm") -> float | None:
    entry = items.get(key)
    if entry is None:
        return None
    return entry.get(period)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _pct(numerator: float | None, denominator: float | None) -> float | None:
    val = _safe_div(numerator, denominator)
    return val * 100 if val is not None else None


def _growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / previous * 100


# ═══════════════════════════════════════════════════════════════
# 성장성
# ═══════════════════════════════════════════════════════════════

def 총자산증가율(items: Items) -> float | None:
    """(기말총자산 - 기초총자산) / 기초총자산 * 100."""
    return _growth(_get(items, "total_assets", "thstrm"),
                   _get(items, "total_assets", "frmtrm"))


def 유동자산증가율(items: Items) -> float | None:
    return _growth(_get(items, "current_assets", "thstrm"),
                   _get(items, "current_assets", "frmtrm"))


def 매출액증가율(items: Items) -> float | None:
    return _growth(_get(items, "revenue", "thstrm"),
                   _get(items, "revenue", "frmtrm"))


def 순이익증가율(items: Items) -> float | None:
    return _growth(_get(items, "net_income", "thstrm"),
                   _get(items, "net_income", "frmtrm"))


def 영업이익증가율(items: Items) -> float | None:
    return _growth(_get(items, "operating_income", "thstrm"),
                   _get(items, "operating_income", "frmtrm"))


# ═══════════════════════════════════════════════════════════════
# 수익성
# ═══════════════════════════════════════════════════════════════

def 매출액순이익률(items: Items) -> float | None:
    """순이익 / 매출액 * 100."""
    return _pct(_get(items, "net_income"), _get(items, "revenue"))


def 매출총이익률(items: Items) -> float | None:
    """매출총이익 / 매출액 * 100."""
    return _pct(_get(items, "gross_profit"), _get(items, "revenue"))


def 자기자본순이익률(items: Items) -> float | None:
    """순이익 / 자기자본 * 100 (ROE)."""
    return _pct(_get(items, "net_income"), _get(items, "total_equity"))


# ═══════════════════════════════════════════════════════════════
# 활동성
# ═══════════════════════════════════════════════════════════════

def 매출채권회전율(items: Items) -> float | None:
    """매출액 / 매출채권."""
    return _safe_div(_get(items, "revenue"), _get(items, "trade_receivables"))


def 재고자산회전율(items: Items) -> float | None:
    """매출원가 / 재고자산."""
    return _safe_div(_get(items, "cost_of_sales"), _get(items, "inventories"))


def 총자본회전율(items: Items) -> float | None:
    """매출액 / 총자산."""
    return _safe_div(_get(items, "revenue"), _get(items, "total_assets"))


def 유형자산회전율(items: Items) -> float | None:
    """매출액 / 유형자산.
    [수정 v2] 기존 코드는 분모가 총자산으로 총자본회전율과 동일했음 → 유형자산으로 수정."""
    return _safe_div(_get(items, "revenue"), _get(items, "tangible_assets"))


def 매출원가율(items: Items) -> float | None:
    """매출원가 / 매출액 * 100."""
    return _pct(_get(items, "cost_of_sales"), _get(items, "revenue"))


# ═══════════════════════════════════════════════════════════════
# 안정성
# ═══════════════════════════════════════════════════════════════

def 부채비율(items: Items) -> float | None:
    """부채 / 자기자본 * 100."""
    return _pct(_get(items, "total_liabilities"), _get(items, "total_equity"))


def 유동비율(items: Items) -> float | None:
    """유동자산 / 유동부채 * 100."""
    return _pct(_get(items, "current_assets"), _get(items, "current_liabilities"))


def 자기자본비율(items: Items) -> float | None:
    """자기자본 / 총자산 * 100."""
    return _pct(_get(items, "total_equity"), _get(items, "total_assets"))


def 당좌비율(items: Items) -> float | None:
    """(유동자산 - 재고자산) / 유동부채 * 100."""
    ca  = _get(items, "current_assets")
    inv = _get(items, "inventories") or 0
    cl  = _get(items, "current_liabilities")
    if ca is None:
        return None
    return _pct(ca - inv, cl)


def 비유동자산장기적합률(items: Items) -> float | None:
    """비유동자산 / (자기자본 + 장기차입금) * 100.
    [수정 v3] total_equity 누락 → None 반환 (결측을 0으로 취급하지 않음)
             long_term_borrowings 누락 → 0 fallback (차입 없는 기업 허용)"""
    nca = _get(items, "non_current_assets")
    eq  = _get(items, "total_equity")
    ltb = _get(items, "long_term_borrowings") or 0

    if nca is None or eq is None:
        return None
    denom = eq + ltb
    if denom == 0:
        return None
    return _pct(nca, denom)


def 순운전자본비율(items: Items) -> float | None:
    """(유동자산 - 유동부채) / 총자산 * 100."""
    ca = _get(items, "current_assets")
    cl = _get(items, "current_liabilities")
    ta = _get(items, "total_assets")
    if ca is None or cl is None:
        return None
    return _pct(ca - cl, ta)


def 차입금의존도(items: Items) -> float | None:
    """(단기차입금 + 장기차입금 + 사채) / 총자산 * 100.
    [수정 v2] 세 항목이 모두 None(수집 누락)이면 None 반환.
    실제로 차입이 없어서 0인 경우와 구분하기 위함."""
    stb   = _get(items, "short_term_borrowings")
    ltb   = _get(items, "long_term_borrowings")
    bonds = _get(items, "bonds_payable")
    ta    = _get(items, "total_assets")

    # 세 항목 모두 누락이면 계산 불가
    if stb is None and ltb is None and bonds is None:
        return None

    total_borrowing = (stb or 0) + (ltb or 0) + (bonds or 0)
    return _pct(total_borrowing, ta)


def 현금비율(items: Items) -> float | None:
    """현금 / 유동부채 * 100."""
    return _pct(_get(items, "cash"), _get(items, "current_liabilities"))


def 유형자산_값(items: Items) -> float | None:
    return _get(items, "tangible_assets")


def 무형자산_값(items: Items) -> float | None:
    return _get(items, "intangible_assets")


def 무형자산상각비_값(items: Items) -> float | None:
    return _get(items, "amortization")


def 유형자산상각비_값(items: Items) -> float | None:
    return _get(items, "depreciation")


def 감가상각비(items: Items) -> float | None:
    """유형자산상각비 + 무형자산상각비. 둘 다 없으면 None."""
    dep = _get(items, "depreciation")
    amo = _get(items, "amortization")
    if dep is None and amo is None:
        return None
    return (dep or 0) + (amo or 0)


# ═══════════════════════════════════════════════════════════════
# 가치평가
# ═══════════════════════════════════════════════════════════════

def 총자본영업이익률(items: Items) -> float | None:
    """영업이익 / 총자산 * 100."""
    return _pct(_get(items, "operating_income"), _get(items, "total_assets"))


def 총자본순이익률(items: Items) -> float | None:
    """순이익 / 총자산 * 100."""
    return _pct(_get(items, "net_income"), _get(items, "total_assets"))


def 유보액_납입자본비율(items: Items) -> float | None:
    """(이익잉여금 + 자본잉여금) / 납입자본금 * 100."""
    re_  = _get(items, "retained_earnings")
    cs   = _get(items, "capital_surplus") or 0
    pic  = _get(items, "paid_in_capital")
    if re_ is None:
        return None
    return _pct(re_ + cs, pic)


def 총자본투자효율(items: Items) -> float | None:
    """(순이익 + 이자비용) / 총자산."""
    ni = _get(items, "net_income")
    ie = _get(items, "interest_expense") or 0
    ta = _get(items, "total_assets")
    if ni is None:
        return None
    return _safe_div(ni + ie, ta)


# ═══════════════════════════════════════════════════════════════
# 오케스트레이션
# ═══════════════════════════════════════════════════════════════

RATIO_DEFINITIONS: list[tuple[str, str, Any]] = [
    ("성장성",   "총자산증가율",         총자산증가율),
    ("성장성",   "유동자산증가율",       유동자산증가율),
    ("성장성",   "매출액증가율",         매출액증가율),
    ("성장성",   "순이익증가율",         순이익증가율),
    ("성장성",   "영업이익증가율",       영업이익증가율),
    ("수익성",   "매출액순이익률",       매출액순이익률),
    ("수익성",   "매출총이익률",         매출총이익률),
    ("수익성",   "자기자본순이익률",     자기자본순이익률),
    ("활동성",   "매출채권회전율",       매출채권회전율),
    ("활동성",   "재고자산회전율",       재고자산회전율),
    ("활동성",   "총자본회전율",         총자본회전율),
    ("활동성",   "유형자산회전율",       유형자산회전율),
    ("활동성",   "매출원가율",           매출원가율),
    ("안정성",   "부채비율",             부채비율),
    ("안정성",   "유동비율",             유동비율),
    ("안정성",   "자기자본비율",         자기자본비율),
    ("안정성",   "당좌비율",             당좌비율),
    ("안정성",   "비유동자산장기적합률", 비유동자산장기적합률),
    ("안정성",   "순운전자본비율",       순운전자본비율),
    ("안정성",   "차입금의존도",         차입금의존도),
    ("안정성",   "현금비율",             현금비율),
    ("안정성",   "유형자산",             유형자산_값),
    ("안정성",   "무형자산",             무형자산_값),
    ("안정성",   "무형자산상각비",       무형자산상각비_값),
    ("안정성",   "유형자산상각비",       유형자산상각비_값),
    ("안정성",   "감가상각비",           감가상각비),
    ("가치평가", "총자본영업이익률",     총자본영업이익률),
    ("가치평가", "총자본순이익률",       총자본순이익률),
    ("가치평가", "유보액/납입자본비율",  유보액_납입자본비율),
    ("가치평가", "총자본투자효율",       총자본투자효율),
]

RATIO_NAMES: list[str] = [name for _, name, _ in RATIO_DEFINITIONS]


def compute_all_ratios(items: Items) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for _cat, name, func in RATIO_DEFINITIONS:
        try:
            result[name] = func(items)
        except Exception:
            result[name] = None
    return result