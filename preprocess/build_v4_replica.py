"""
build_v4_replica.py
─────────────────────────────────────────────────────────────────────────────
박민서 v4 보고서를 우리 환경에서 재현하기 위한 신규 전처리 파이프라인.

기존 build_master_dataset.py / build_fixed_datasets.py 는 절대 건드리지 않는다.
별도 출력 디렉터리 (preprocess/data/processed/v4_replica/) 를 사용한다.

특징:
  - data/raw/raw/{healthy,delisted}/<sector>/*.json 전체 사용 (Q1/H1/Q3/ANNUAL)
  - 30개 재무비율 (account_mapper + ratio_calculator 재사용) + 3개 YoY 증가율
  - 4개 보고기간 flag (period_Q1/H1/Q3/ANNUAL)
  - 각 재무비율에 _missing 결측 indicator
  - 6개 거시경제 변수 (macro_quarterly.csv)
  - 라벨링 2종:
      * all_delisted:  delisted 폴더의 모든 행을 label=1 (PDF v4 방식)
      * only_n1:       상폐연도 정확히 1년 전 분기 행만 label=1
                       (그 외 상폐기업 행은 제거 - 우리 기존 fixed_N1 방식과 동일)
  - Group Split: stock_code 기준 무작위 70/15/15 (PDF 비율과 동일)
  - 결측 처리: (sector, quarter, column) 중앙값 → 그래도 NaN이면 0

출력:
  preprocess/data/processed/v4_replica/all_delisted/{train,valid,test}.csv + meta.json
  preprocess/data/processed/v4_replica/only_n1/{train,valid,test}.csv     + meta.json

실행:
  .venv/bin/python preprocess/build_v4_replica.py --workers 8
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# 경로
# ─────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).resolve().parents[1]
PREP_DIR     = REPO_ROOT / "preprocess"
RAW_BASE     = REPO_ROOT / "data" / "raw" / "raw"
RAW_HEALTHY  = RAW_BASE / "healthy"
RAW_DELISTED = RAW_BASE / "delisted"
MACRO_CSV    = PREP_DIR / "data" / "macro" / "macro_quarterly.csv"
DELIST_XLSX  = PREP_DIR / "data" / "상장폐지현황.xlsx"
LISTED_XLSX  = PREP_DIR / "data" / "상장법인목록.xlsx"

OUT_BASE     = PREP_DIR / "data" / "processed" / "v4_replica"

# 파일명 패턴: 종목코드_연도_보고기간(_CFS|_OFS).json
PATTERN = re.compile(r"^(\d{6})_(\d{4})_(Q1|H1|Q3|ANNUAL)(?:_CFS|_OFS)?\.json$")

# build_master_dataset.py 와 동일한 제거 대상 (구조적 상폐 + 더미)
EXCLUDE_CODES = {
    "048260",  # 오스템임플란트 (자진상폐)
    "029960",  # 코엔텍 (완전자회사)
    "006580",  # 대양제지 (자진상폐)
    "115960",  # 연우 (완전자회사)
    "999999",  # 더미
}

QUARTERS  = ["Q1", "H1", "Q3", "ANNUAL"]

# 재무비율 컬럼 (ratio_calculator.RATIO_NAMES + YoY 3개)
RATIO_NAMES_BASE = [
    '총자산증가율', '유동자산증가율',
    '매출액순이익률', '매출총이익률', '자기자본순이익률',
    '매출채권회전율', '재고자산회전율', '총자본회전율',
    '유형자산회전율', '매출원가율',
    '부채비율', '유동비율', '자기자본비율', '당좌비율', '비유동자산장기적합률',
    '순운전자본비율', '차입금의존도', '현금비율',
    '유형자산', '무형자산', '무형자산상각비', '유형자산상각비', '감가상각비',
    '총자본영업이익률', '총자본순이익률', '유보액/납입자본비율', '총자본투자효율',
]
YOY_RATIOS = ['매출액증가율', '순이익증가율', '영업이익증가율']
RATIO_NAMES = RATIO_NAMES_BASE + YOY_RATIOS  # 30개

# YoY 계산용 원본 키
YOY_TARGETS = [
    ("매출액증가율",   "revenue"),
    ("순이익증가율",   "net_income"),
    ("영업이익증가율", "operating_income"),
]

MACRO_COLS = [
    'credit_spread', 'kosdaq_return', 'gdp_growth_yoy',
    'usdkrw_chg', 'vix_avg', 'cpi_yoy',
]

# Group Split 비율 (PDF: 1387/298/298 ≈ 0.70/0.15/0.15)
SPLIT_RATIO = (0.70, 0.15, 0.15)
SPLIT_SEED  = 42

# 분기 → 분기말 (month, day) (상장일 비교용)
QUARTER_END_MD = {"Q1": (3, 31), "H1": (6, 30), "Q3": (9, 30), "ANNUAL": (12, 31)}


# ═══════════════════════════════════════════════════════════════
# 1. 단일 JSON → record  (병렬 worker)
# ═══════════════════════════════════════════════════════════════
def process_file(args: tuple) -> dict | None:
    fp, label, base_dir = args

    sys.path.insert(0, str(base_dir / "src"))
    from account_mapper import extract_standard_items
    from ratio_calculator import compute_all_ratios

    m = PATTERN.match(fp.name)
    if not m:
        return None

    code    = m.group(1).zfill(6)
    year    = int(m.group(2))
    quarter = m.group(3)
    sector  = fp.parent.name

    if code in EXCLUDE_CODES:
        return None

    try:
        with open(fp, encoding="utf-8") as f:
            dart_items = json.load(f)
    except Exception:
        return None

    if not dart_items:
        return None

    std_items = extract_standard_items(dart_items)
    ratios    = compute_all_ratios(std_items)

    record = {
        "stock_code":  code,
        "year":        year,
        "quarter":     quarter,
        "gics_sector": sector,
        "_label_raw":  label,  # 폴더 기준 (healthy=0, delisted=1)
    }
    record.update(ratios)

    # YoY 계산용 원본 값
    for col, key in YOY_TARGETS:
        entry = std_items.get(key)
        record[f"_yoy_src_{key}"] = entry.get("thstrm") if entry else None

    return record


def process_folder(folder: Path, label: int, workers: int) -> pd.DataFrame:
    files = list(folder.rglob("*.json"))
    tag   = "healthy" if label == 0 else "delisted"
    print(f"  {tag}: {len(files):,}개 파일 (workers={workers})")

    args_list = [(fp, label, PREP_DIR) for fp in files]
    records: list[dict] = []

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_file, a): a for a in args_list}
        for fut in tqdm(as_completed(futures), total=len(files), desc=f"  {tag}", leave=False):
            r = fut.result()
            if r is not None:
                records.append(r)

    df = pd.DataFrame(records)
    print(f"  {tag}: {len(df):,}행 변환 완료")
    return df


# ═══════════════════════════════════════════════════════════════
# 2. (code, year, quarter) dedup  (sector 중복 / CFS·OFS 변형 정리)
# ═══════════════════════════════════════════════════════════════
def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    n_before = len(df)
    df = df.drop_duplicates(subset=["stock_code", "year", "quarter"], keep="first")
    df = df.reset_index(drop=True)
    print(f"  dedup: {n_before:,} → {len(df):,}")
    return df


# ═══════════════════════════════════════════════════════════════
# 3. YoY 증가율 계산  (같은 quarter, year-1 join)
# ═══════════════════════════════════════════════════════════════
def add_yoy_growth(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_prev_year"] = df["year"] - 1

    src_cols = [f"_yoy_src_{k}" for _, k in YOY_TARGETS]
    prev = df[["stock_code", "year", "quarter"] + src_cols].copy()

    df = df.merge(
        prev,
        left_on=["stock_code", "_prev_year", "quarter"],
        right_on=["stock_code", "year", "quarter"],
        suffixes=("", "_prev"),
        how="left",
    ).drop(columns=["year_prev", "quarter_prev"], errors="ignore")

    for col, key in YOY_TARGETS:
        cur  = df[f"_yoy_src_{key}"]
        prv  = df[f"_yoy_src_{key}_prev"]
        denom = prv.abs()
        val = (cur - prv) / denom * 100
        val[denom == 0] = np.nan
        df[col] = val

    drop_cols = ["_prev_year"] + src_cols + [f"{c}_prev" for c in src_cols]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return df


# ═══════════════════════════════════════════════════════════════
# 4. 상장일 필터 (재할당된 종목코드의 이전 회사 데이터 제거)
# ═══════════════════════════════════════════════════════════════
def filter_by_listing_date(df: pd.DataFrame, listed_xlsx: Path) -> pd.DataFrame:
    if not listed_xlsx.exists():
        print(f"  상장일 필터 skip ({listed_xlsx.name} 없음)")
        return df

    listed = pd.read_excel(listed_xlsx)
    code_col = next((c for c in listed.columns if "종목코드" in c), None)
    date_col = next((c for c in listed.columns if "상장일" in c), None)
    if code_col is None or date_col is None:
        print(f"  상장일 필터 skip (컬럼 추론 실패: {listed.columns.tolist()})")
        return df

    listed = listed[[code_col, date_col]].dropna()
    listed[code_col] = listed[code_col].astype(str).str.zfill(6)
    listed[date_col] = pd.to_datetime(listed[date_col], errors="coerce")
    listed = listed.dropna().drop_duplicates(subset=[code_col])
    listed_map = dict(zip(listed[code_col], listed[date_col]))

    n_before = len(df)
    keep = []
    for _, row in df.iterrows():
        code = row["stock_code"]
        listing_dt = listed_map.get(code)
        if listing_dt is None:
            keep.append(True)
            continue
        m, d = QUARTER_END_MD[row["quarter"]]
        report_dt = pd.Timestamp(year=int(row["year"]), month=m, day=d)
        keep.append(report_dt >= listing_dt)
    df = df[keep].reset_index(drop=True)
    print(f"  상장일 필터: {n_before:,} → {len(df):,} ({n_before - len(df):,}행 제거)")
    return df


# ═══════════════════════════════════════════════════════════════
# 5. 상폐일 → 라벨링 (only_n1 변형용)
# ═══════════════════════════════════════════════════════════════
def load_delist_map(xlsx: Path) -> dict[str, int]:
    """{stock_code: delist_year} 매핑."""
    if not xlsx.exists():
        raise FileNotFoundError(f"상장폐지현황 누락: {xlsx}")

    df = pd.read_excel(xlsx)
    code_col = next((c for c in df.columns if "종목코드" in c), None)
    date_col = next((c for c in df.columns if "폐지일자" in c), None)
    if code_col is None or date_col is None:
        raise ValueError(f"상폐 엑셀 컬럼 추론 실패: {df.columns.tolist()}")

    df[code_col] = df[code_col].astype(str).str.zfill(6)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).drop_duplicates(subset=[code_col], keep="first")
    return {row[code_col]: int(row[date_col].year) for _, row in df.iterrows()}


# ═══════════════════════════════════════════════════════════════
# 6. 거시경제 merge
# ═══════════════════════════════════════════════════════════════
def merge_macro(df: pd.DataFrame, macro_csv: Path) -> pd.DataFrame:
    if not macro_csv.exists():
        print(f"  거시경제 merge skip ({macro_csv.name} 없음)")
        for c in MACRO_COLS:
            df[c] = np.nan
        return df
    macro = pd.read_csv(macro_csv, encoding="utf-8-sig")
    macro["year"] = macro["year"].astype(int)
    df = df.merge(macro, on=["year", "quarter"], how="left")
    return df


# ═══════════════════════════════════════════════════════════════
# 7. Group Split (stock_code 기준 무작위)
# ═══════════════════════════════════════════════════════════════
def group_split(
    df: pd.DataFrame,
    ratio: tuple[float, float, float] = SPLIT_RATIO,
    seed: int = SPLIT_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    codes = sorted(df["stock_code"].unique())
    rng = np.random.default_rng(seed)
    rng.shuffle(codes)
    n = len(codes)
    n_tr = int(n * ratio[0])
    n_va = int(n * ratio[1])
    tr_codes = set(codes[:n_tr])
    va_codes = set(codes[n_tr:n_tr + n_va])
    te_codes = set(codes[n_tr + n_va:])

    tr = df[df["stock_code"].isin(tr_codes)].reset_index(drop=True)
    va = df[df["stock_code"].isin(va_codes)].reset_index(drop=True)
    te = df[df["stock_code"].isin(te_codes)].reset_index(drop=True)
    return tr, va, te


# ═══════════════════════════════════════════════════════════════
# 8. 결측 처리 (train fit → valid/test transform)
# ═══════════════════════════════════════════════════════════════
class MedianImputer:
    """(sector, quarter) 별 컬럼 중앙값 보간 + global fallback."""

    def __init__(self, cols: list[str]):
        self.cols = cols
        self.sector_quarter_med: dict[tuple, float] = {}
        self.quarter_med: dict[tuple, float] = {}
        self.global_med: dict[str, float] = {}

    def fit(self, df: pd.DataFrame) -> "MedianImputer":
        for col in self.cols:
            s = df[col]
            # sector × quarter 중앙값
            grp = df.groupby(["gics_sector", "quarter"])[col].median()
            for (sec, q), v in grp.items():
                if pd.notna(v):
                    self.sector_quarter_med[(sec, q, col)] = float(v)
            # quarter 중앙값 (fallback)
            grp2 = df.groupby("quarter")[col].median()
            for q, v in grp2.items():
                if pd.notna(v):
                    self.quarter_med[(q, col)] = float(v)
            # global 중앙값 (final fallback)
            gv = s.median()
            self.global_med[col] = float(gv) if pd.notna(gv) else 0.0
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in self.cols:
            mask = df[col].isna()
            if not mask.any():
                continue
            for idx in df[mask].index:
                sec = df.at[idx, "gics_sector"]
                q   = df.at[idx, "quarter"]
                v = self.sector_quarter_med.get((sec, q, col))
                if v is None:
                    v = self.quarter_med.get((q, col))
                if v is None:
                    v = self.global_med.get(col, 0.0)
                df.at[idx, col] = v
        return df


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        df[f"{c}_missing"] = df[c].isna().astype(int)
    return df


def add_period_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for q in QUARTERS:
        df[f"period_{q}"] = (df["quarter"] == q).astype(int)
    return df


# ═══════════════════════════════════════════════════════════════
# 9. 메인 빌드
# ═══════════════════════════════════════════════════════════════
def build_master(workers: int) -> pd.DataFrame:
    """raw → 전체 분기/연도 record DataFrame (전처리 전)."""
    print("[1/4] Raw JSON → record")
    df_h = process_folder(RAW_HEALTHY,  label=0, workers=workers)
    df_d = process_folder(RAW_DELISTED, label=1, workers=workers)
    df = pd.concat([df_h, df_d], ignore_index=True)
    print(f"  합계: {len(df):,}행")

    print("[2/4] dedup")
    df = dedupe(df)

    print("[3/4] YoY 증가율 계산")
    df = add_yoy_growth(df)

    print("[4/4] 상장일 필터")
    df = filter_by_listing_date(df, LISTED_XLSX)

    # 너무 옛날 연도 제거 (2015년 이전 → 학습 데이터로 무의미)
    n_before = len(df)
    df = df[df["year"] >= 2015].reset_index(drop=True)
    print(f"  2015년 이전 제거: {n_before:,} → {len(df):,}")

    return df


def make_variant(
    df_master: pd.DataFrame,
    variant: str,
    delist_map: dict[str, int],
) -> pd.DataFrame:
    """variant 별 라벨 적용 + 행 필터.

    variant:
      - 'all_delisted': 폴더 기준 라벨 그대로 (delisted=1, healthy=0)
      - 'only_n1':     상폐연도 정확히 N=1년 전 행만 label=1, 그 외 상폐기업 행 제거
    """
    df = df_master.copy()

    if variant == "all_delisted":
        df["label"] = df["_label_raw"].astype(int)
        return df.reset_index(drop=True)

    if variant == "only_n1":
        keep = []
        labels = []
        for _, row in df.iterrows():
            code = row["stock_code"]
            raw  = int(row["_label_raw"])
            if raw == 0:
                keep.append(True)
                labels.append(0)
                continue
            # delisted 폴더 → N=1년만 살리고 양성
            delist_year = delist_map.get(code)
            if delist_year is None:
                # 폴더는 delisted지만 엑셀에 없음 → 제외 (잡음 방지)
                keep.append(False)
                labels.append(0)
                continue
            if int(row["year"]) == delist_year - 1:
                keep.append(True)
                labels.append(1)
            else:
                keep.append(False)
                labels.append(0)
        df["label"] = labels
        df = df[keep].reset_index(drop=True)
        return df

    raise ValueError(f"unknown variant: {variant}")


def fit_and_transform(
    df: pd.DataFrame,
    macro_csv: Path,
    feature_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """variant DataFrame → train/valid/test 분할 + 결측 처리 + flag."""
    # macro merge (split 전, 동일 키 join이므로 leakage 없음)
    df = merge_macro(df, macro_csv)

    # group split
    tr, va, te = group_split(df)

    # 검증: 종목 중복 없음
    overlap_tv = set(tr["stock_code"]) & set(va["stock_code"])
    overlap_tt = set(tr["stock_code"]) & set(te["stock_code"])
    overlap_vt = set(va["stock_code"]) & set(te["stock_code"])
    assert not overlap_tv and not overlap_tt and not overlap_vt, "group split leakage!"

    # _missing flag (결측 처리 전에)
    tr = add_missing_flags(tr, feature_cols)
    va = add_missing_flags(va, feature_cols)
    te = add_missing_flags(te, feature_cols)

    # 결측 처리 (train fit)
    imputer = MedianImputer(feature_cols).fit(tr)
    tr = imputer.transform(tr)
    va = imputer.transform(va)
    te = imputer.transform(te)

    # period flag
    tr = add_period_flags(tr)
    va = add_period_flags(va)
    te = add_period_flags(te)

    # 남은 NaN(주로 macro) → 0
    for col in feature_cols + MACRO_COLS:
        for split in (tr, va, te):
            split[col] = split[col].fillna(0.0)

    meta = {
        "rows": {"train": len(tr), "valid": len(va), "test": len(te)},
        "pos":  {
            "train": int(tr["label"].sum()),
            "valid": int(va["label"].sum()),
            "test":  int(te["label"].sum()),
        },
        "neg":  {
            "train": int((tr["label"] == 0).sum()),
            "valid": int((va["label"] == 0).sum()),
            "test":  int((te["label"] == 0).sum()),
        },
        "unique_codes": {
            "train": int(tr["stock_code"].nunique()),
            "valid": int(va["stock_code"].nunique()),
            "test":  int(te["stock_code"].nunique()),
        },
        "overlap": {
            "train_valid": 0, "train_test": 0, "valid_test": 0,
        },
    }
    return tr, va, te, meta


# ═══════════════════════════════════════════════════════════════
# 10. CLI
# ═══════════════════════════════════════════════════════════════
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    OUT_BASE.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STEP A: master DataFrame 빌드 (raw → record)")
    print("=" * 70)
    df_master = build_master(args.workers)

    print("\nSTEP B: 상폐일 매핑 로드")
    delist_map = load_delist_map(DELIST_XLSX)
    print(f"  상폐 매핑: {len(delist_map):,}개 종목")

    # 학습에 쓸 30개 재무비율
    feature_cols = list(RATIO_NAMES)

    for variant in ["all_delisted", "only_n1"]:
        print(f"\n{'=' * 70}")
        print(f"STEP C: variant = {variant}")
        print("=" * 70)

        df_v = make_variant(df_master, variant, delist_map)
        print(f"  rows: {len(df_v):,}")
        print(f"  label=1: {int((df_v['label'] == 1).sum()):,}")
        print(f"  label=0: {int((df_v['label'] == 0).sum()):,}")
        print(f"  unique codes: {df_v['stock_code'].nunique():,}")

        tr, va, te, meta = fit_and_transform(df_v, MACRO_CSV, feature_cols)

        meta.update({
            "variant": variant,
            "feature_count_financial": len(feature_cols),
            "feature_count_missing_flag": len(feature_cols),
            "feature_count_period_flag": len(QUARTERS),
            "feature_count_macro": len(MACRO_COLS),
            "feature_count_total": len(feature_cols) * 2 + len(QUARTERS) + len(MACRO_COLS),
            "feature_cols_financial": feature_cols,
            "macro_cols": MACRO_COLS,
            "split_ratio": list(SPLIT_RATIO),
            "split_seed": SPLIT_SEED,
            "imbalance_ratio_train": (
                meta["neg"]["train"] / meta["pos"]["train"]
                if meta["pos"]["train"] > 0 else None
            ),
        })

        out_dir = OUT_BASE / variant
        out_dir.mkdir(parents=True, exist_ok=True)
        tr.to_csv(out_dir / "train.csv", index=False)
        va.to_csv(out_dir / "valid.csv", index=False)
        te.to_csv(out_dir / "test.csv",  index=False)
        with open(out_dir / "meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print(f"  → 저장: {out_dir}")
        print(f"  train pos/neg: {meta['pos']['train']:,} / {meta['neg']['train']:,}")
        print(f"  valid pos/neg: {meta['pos']['valid']:,} / {meta['neg']['valid']:,}")
        print(f"  test  pos/neg: {meta['pos']['test']:,} / {meta['neg']['test']:,}")
        if meta["imbalance_ratio_train"]:
            print(f"  imbalance_ratio (train neg/pos): {meta['imbalance_ratio_train']:.2f}")


if __name__ == "__main__":
    main()
