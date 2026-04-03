"""
DART raw JSON → 재무비율 CSV 변환 모듈

사용법:
    # 단일 기업
    python -m src.etl.ratio_calculator \
        --raw-dir data/raw/sample/healthy/Materials \
        --ticker 001810 --year 2025 --corp-name 무림SP --label 0 \
        --output data/output/sample/Materials/001810_2025.csv

    # 디렉터리 일괄 변환
    python -m src.etl.ratio_calculator \
        --batch-dir data/raw/sample \
        --output-dir data/output/sample \
        --company-map companies.csv
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

QUARTER_ORDER = ["Q1", "H1", "Q3", "ANNUAL"]

META_COLS = ["stock_code", "corp_name", "year", "quarter", "label"]

RATIO_COLS = [
    "총자산증가율", "유동자산증가율", "매출액증가율", "순이익증가율", "영업이익증가율",
    "매출액순이익률", "매출총이익률", "자기자본순이익률",
    "매출채권회전율", "재고자산회전율", "총자본회전율", "유형자산회전율",
    "매출원가율", "부채비율", "유동비율", "자기자본비율", "당좌비율",
    "비유동자산장기적합률", "순운전자본비율", "차입금의존도", "현금비율",
    "유형자산", "무형자산", "무형자산상각비", "유형자산상각비", "감가상각비",
    "총자본영업이익률", "총자본순이익률", "유보액/납입자본비율", "총자본투자효율",
]

ALL_COLS = META_COLS + RATIO_COLS

# account_id fallback 그룹: 기업마다 사용하는 ID가 다를 수 있음
_ACCOUNT_FALLBACKS = {
    "trade_recv": [
        "ifrs-full_CurrentTradeReceivables",
        "dart_ShortTermTradeReceivable",
    ],
    "intangible": [
        "ifrs-full_IntangibleAssetsAndGoodwill",
        "ifrs-full_IntangibleAssetsOtherThanGoodwill",
    ],
}

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------


def _safe_float(val: Optional[str]) -> Optional[float]:
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _growth(cur: Optional[float], prev: Optional[float]) -> Optional[float]:
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / abs(prev) * 100


# ---------------------------------------------------------------------------
# 계정 추출
# ---------------------------------------------------------------------------


@dataclass
class AccountRow:
    """단일 계정과목 행의 금액 정보."""
    thstrm: Optional[float]
    frmtrm: Optional[float]


class AccountMap:
    """sj_div별 account_id → AccountRow 매핑."""

    def __init__(self, data: list[dict], sj_div: str):
        self._by_id: dict[str, AccountRow] = {}
        for item in data:
            if item["sj_div"] != sj_div or item["account_detail"] != "-":
                continue
            aid = item["account_id"]
            self._by_id[aid] = AccountRow(
                thstrm=_safe_float(item.get("thstrm_amount")),
                frmtrm=_safe_float(item.get("frmtrm_amount")),
            )

    def get(self, *account_ids: str) -> AccountRow:
        """여러 account_id 중 첫 번째로 존재하는 것을 반환."""
        for aid in account_ids:
            if aid in self._by_id:
                return self._by_id[aid]
        return AccountRow(thstrm=None, frmtrm=None)

    def val(self, *account_ids: str) -> Optional[float]:
        return self.get(*account_ids).thstrm

    def prev(self, *account_ids: str) -> Optional[float]:
        return self.get(*account_ids).frmtrm


# ---------------------------------------------------------------------------
# 비율 계산
# ---------------------------------------------------------------------------


def calc_ratios(data: list[dict]) -> dict[str, Optional[float]]:
    """raw JSON 배열 1개(= 1분기)에서 재무비율 딕셔너리를 계산한다."""

    bs = AccountMap(data, "BS")
    cis = AccountMap(data, "CIS")

    # -- BS 항목 --
    assets = bs.val("ifrs-full_Assets")
    assets_prev = bs.prev("ifrs-full_Assets")
    cur_assets = bs.val("ifrs-full_CurrentAssets")
    cur_assets_prev = bs.prev("ifrs-full_CurrentAssets")
    equity = bs.val("ifrs-full_Equity")
    cur_liab = bs.val("ifrs-full_CurrentLiabilities")
    inventory = bs.val("ifrs-full_Inventories")
    cash = bs.val("ifrs-full_CashAndCashEquivalents")
    ppe = bs.val("ifrs-full_PropertyPlantAndEquipment")
    intangible = bs.val(*_ACCOUNT_FALLBACKS["intangible"])
    trade_recv = bs.val(*_ACCOUNT_FALLBACKS["trade_recv"])

    short_borrow = bs.val("ifrs-full_ShorttermBorrowings") or 0
    cur_lt_borrow = bs.val("ifrs-full_CurrentPortionOfLongtermBorrowings") or 0
    lt_borrow = bs.val("ifrs-full_LongtermBorrowings") or 0

    retained = bs.val("ifrs-full_RetainedEarnings")
    issued_cap = bs.val("ifrs-full_IssuedCapital")

    # -- CIS 항목 --
    revenue = cis.val("ifrs-full_Revenue")
    revenue_prev = cis.prev("ifrs-full_Revenue")
    profit = cis.val("ifrs-full_ProfitLoss")
    profit_prev = cis.prev("ifrs-full_ProfitLoss")
    op_income = cis.val("dart_OperatingIncomeLoss")
    op_income_prev = cis.prev("dart_OperatingIncomeLoss")
    gross_profit = cis.val("ifrs-full_GrossProfit")
    cogs = cis.val("ifrs-full_CostOfSales")

    has_cis_prev = revenue_prev is not None

    r: dict[str, Optional[float]] = {}

    # -- 성장성 --
    r["총자산증가율"] = _growth(assets, assets_prev)
    r["유동자산증가율"] = _growth(cur_assets, cur_assets_prev)
    r["매출액증가율"] = _growth(revenue, revenue_prev) if has_cis_prev else None
    r["순이익증가율"] = _growth(profit, profit_prev) if has_cis_prev else None
    r["영업이익증가율"] = _growth(op_income, op_income_prev) if has_cis_prev else None

    # -- 수익성 --
    r["매출액순이익률"] = _pct(profit, revenue)
    r["매출총이익률"] = _pct(gross_profit, revenue)
    r["자기자본순이익률"] = _pct(profit, equity)
    r["매출원가율"] = _pct(cogs, revenue)

    # -- 활동성 (기말잔액 기준) --
    r["매출채권회전율"] = _safe_div(revenue, trade_recv)
    r["재고자산회전율"] = _safe_div(cogs, inventory)
    r["총자본회전율"] = _safe_div(revenue, assets)
    r["유형자산회전율"] = _safe_div(revenue, assets)  # NOTE: 기존 데이터와 일치 (확인 필요)

    # -- 안정성 --
    r["부채비율"] = _pct(assets, equity)  # 재무레버리지 비율
    r["유동비율"] = _pct(cur_assets, cur_liab)
    r["자기자본비율"] = _pct(equity, assets)
    r["당좌비율"] = (
        _pct((cur_assets or 0) - (inventory or 0), cur_liab)
        if cur_liab else None
    )
    r["비유동자산장기적합률"] = None
    r["순운전자본비율"] = (
        _pct((cur_assets or 0) - (cur_liab or 0), assets)
        if assets else None
    )
    total_borrow = short_borrow + cur_lt_borrow + lt_borrow
    r["차입금의존도"] = _pct(total_borrow, assets)
    r["현금비율"] = _pct(cash, cur_liab)

    # -- 원시값 --
    r["유형자산"] = ppe
    r["무형자산"] = intangible
    r["무형자산상각비"] = None
    r["유형자산상각비"] = None
    r["감가상각비"] = None

    # -- 자본효율 --
    r["총자본영업이익률"] = _pct(op_income, assets)
    r["총자본순이익률"] = _pct(profit, assets)
    r["유보액/납입자본비율"] = _pct(retained, issued_cap)
    r["총자본투자효율"] = _safe_div(profit, assets)  # 소수 (퍼센트 아님)

    return r


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    v = _safe_div(a, b)
    return v * 100 if v is not None else None


# ---------------------------------------------------------------------------
# CSV 생성
# ---------------------------------------------------------------------------


def convert_single(
    ticker: str,
    corp_name: str,
    year: int,
    label: int,
    raw_dir: str | Path,
    output_path: str | Path,
) -> int:
    """단일 기업/연도의 raw JSON → 재무비율 CSV 변환.

    Returns:
        생성된 행 수
    """
    raw_dir = Path(raw_dir)
    output_path = Path(output_path)
    rows: list[dict[str, str]] = []

    for quarter in QUARTER_ORDER:
        fpath = raw_dir / f"{ticker}_{year}_{quarter}.json"
        if not fpath.exists():
            continue

        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)

        ratios = calc_ratios(data)

        row = {
            "stock_code": ticker,
            "corp_name": corp_name,
            "year": str(year),
            "quarter": quarter,
            "label": str(label),
        }
        for col in RATIO_COLS:
            val = ratios.get(col)
            row[col] = str(val) if val is not None else ""
        rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_COLS)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def convert_batch(
    raw_base: str | Path,
    output_base: str | Path,
    company_map: dict[str, dict],
) -> list[str]:
    """raw_base 하위의 모든 JSON을 일괄 변환.

    Args:
        raw_base: data/raw/ 루트 (하위: {status}/{sector}/{ticker}_{year}_{quarter}.json)
        output_base: data/output/ 루트
        company_map: ticker → {"corp_name": str, "label": int, "sector": str}

    Returns:
        생성된 CSV 경로 리스트
    """
    raw_base = Path(raw_base)
    output_base = Path(output_base)
    generated = []

    # ticker_year 조합 수집
    seen: dict[tuple[str, str, str, str], Path] = {}  # (status, sector, ticker, year) → dir
    for json_path in sorted(raw_base.rglob("*.json")):
        parts = json_path.relative_to(raw_base).parts
        if len(parts) < 3:
            continue
        status, sector = parts[0], parts[1]
        fname = json_path.stem  # e.g. 024810_2024_ANNUAL
        tokens = fname.split("_")
        if len(tokens) < 3:
            continue
        ticker, year = tokens[0], tokens[1]
        key = (status, sector, ticker, year)
        if key not in seen:
            seen[key] = json_path.parent

    for (status, sector, ticker, year), raw_dir in sorted(seen.items()):
        info = company_map.get(ticker, {})
        corp_name = info.get("corp_name", "")
        label = info.get("label", 1 if status == "delisted" else 0)

        out_path = output_base / sector / f"{ticker}_{year}.csv"
        n = convert_single(ticker, corp_name, int(year), label, raw_dir, out_path)
        if n > 0:
            generated.append(str(out_path))
            print(f"  {out_path} ({n} rows)")

    return generated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DART raw JSON → 재무비율 CSV")
    sub = parser.add_subparsers(dest="cmd")

    # -- single --
    p_single = sub.add_parser("single", help="단일 기업 변환")
    p_single.add_argument("--raw-dir", required=True, help="raw JSON 디렉터리")
    p_single.add_argument("--ticker", required=True)
    p_single.add_argument("--year", type=int, required=True)
    p_single.add_argument("--corp-name", default="")
    p_single.add_argument("--label", type=int, default=0)
    p_single.add_argument("--output", required=True, help="출력 CSV 경로")

    # -- batch --
    p_batch = sub.add_parser("batch", help="일괄 변환")
    p_batch.add_argument("--raw-base", required=True, help="raw 루트 (e.g. data/raw)")
    p_batch.add_argument("--output-base", required=True, help="output 루트 (e.g. data/output)")
    p_batch.add_argument("--company-csv", default=None, help="기업 매핑 CSV (stock_code,corp_name)")

    args = parser.parse_args()

    if args.cmd == "single":
        n = convert_single(
            args.ticker, args.corp_name, args.year, args.label,
            args.raw_dir, args.output,
        )
        print(f"Generated {n} rows → {args.output}")

    elif args.cmd == "batch":
        company_map: dict[str, dict] = {}
        if args.company_csv and os.path.exists(args.company_csv):
            with open(args.company_csv, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    company_map[row["stock_code"]] = {
                        "corp_name": row.get("corp_name", ""),
                        "label": int(row.get("label", 0)),
                        "sector": row.get("gics_sector", ""),
                    }
        results = convert_batch(args.raw_base, args.output_base, company_map)
        print(f"\nTotal: {len(results)} files generated")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
