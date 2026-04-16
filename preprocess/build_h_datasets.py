"""
step2_label_and_preprocess.py
─────────────────────────────────────────────────────────────────────────────
전처리 파이프라인 Step 2

수행 작업:
  1. combined_raw.csv + 상폐일 → H값별 rolling 라벨링
  2. 2015년 이전 제거
  3. H별 연도 현황 출력 → split 기준 확인
  4. time split (train/valid/test)
  5. 전처리 fit/transform
     - ffill
     - CF=0
     - 섹터·분기 중앙값 보간
     - 이상치 클리핑
  6. H별 데이터셋 저장

출력 구조:
  data/processed/
    H6/  train.csv, valid.csv, test.csv, meta.json
    H8/  ...
    H12/ ...
    ...

실행:
  python step2_label_and_preprocess.py
"""

import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# 설정  ← 필요시 수정
# ─────────────────────────────────────────────────────────────
BASE_DIR      = Path(r"C:\kwu\KW0SS_PROJECT\kw0ss_project2\preprocess")
COMBINED_CSV  = BASE_DIR / "data" / "processed" / "combined_raw.csv"
DELISTED_XLSX = BASE_DIR / "data" / "상장폐지현황.xlsx"
MACRO_CSV     = BASE_DIR / "data" / "macro" / "macro_quarterly.csv"
OUT_BASE      = BASE_DIR / "data" / "processed"

# 실험할 H값 목록 (개월)
H_LIST = [6, 8, 10, 12, 14, 16, 18, 20, 22, 24]

# Time split 기준
TRAIN_YEARS = list(range(2015, 2023))   # 2015~2022
VALID_YEARS = [2023]
TEST_YEARS  = [2024]
# 2025는 H별로 train 포함 여부 자동 결정

# 상폐현황 파일 기준 날짜 (라벨 확정 가능 마지막 날)
REFERENCE_DATE = pd.Timestamp("2026-03-31")

# 재무비율 컬럼
RATIO_COLS = [
    '총자산증가율', '유동자산증가율', '매출액순이익률', '매출총이익률',
    '자기자본순이익률', '매출채권회전율', '재고자산회전율', '총자본회전율',
    '유형자산회전율', '매출원가율', '부채비율', '유동비율', '자기자본비율',
    '당좌비율', '비유동자산장기적합률', '순운전자본비율', '차입금의존도',
    '현금비율', '유형자산', '무형자산', '총자본영업이익률', '총자본순이익률',
    '유보액/납입자본비율', '총자본투자효율',
]
MACRO_COLS = [
    'credit_spread', 'kosdaq_return', 'gdp_growth_yoy',
    'usdkrw_chg', 'vix_avg', 'cpi_yoy',
]

# 도메인 룰 기반 이상치 범위
DOMAIN_RULES = {
    '부채비율':               (0,    2000),
    '유동비율':               (0,    2000),
    '자기자본비율':           (-500, 100),
    '당좌비율':               (0,    2000),
    '차입금의존도':           (0,    100),
    '현금비율':               (0,    2000),
    '매출원가율':             (0,    300),
    '매출총이익률':           (-300, 100),
    '매출액순이익률':         (-500, 100),
    '자기자본순이익률':       (-500, 100),
    '총자본영업이익률':       (-500, 100),
    '총자본순이익률':         (-500, 100),
    '총자본회전율':           (0,    50),
    '매출채권회전율':         (0,    200),
    '재고자산회전율':         (0,    200),
    '유형자산회전율':         (0,    200),
    '순운전자본비율':         (-500, 100),
    '비유동자산장기적합률':   (0,    2000),
    '총자본투자효율':         (-500, 100),
}

QUARTER_TO_MONTH = {"Q1": 3, "H1": 6, "Q3": 9, "ANNUAL": 12}


# ─────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────
def q2date(year: int, quarter: str) -> pd.Timestamp:
    month = QUARTER_TO_MONTH.get(quarter, 12)
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


# ─────────────────────────────────────────────────────────────
# 1. 상폐일 매핑 로드
# ─────────────────────────────────────────────────────────────
def load_delist_map(xlsx_path: Path) -> dict:
    df = pd.read_excel(xlsx_path)
    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
    df["폐지일자"] = pd.to_datetime(df["폐지일자"], errors="coerce")
    return dict(zip(df["종목코드"], df["폐지일자"]))


# ─────────────────────────────────────────────────────────────
# 2. H별 rolling 라벨링
# ─────────────────────────────────────────────────────────────
def build_rolling_labels(df: pd.DataFrame,
                          H: int,
                          delist_map: dict) -> pd.DataFrame:
    """
    각 (기업, 시점 T)에 대해 T+H개월 내 상폐 여부로 라벨 계산.

    rolling_label:
        1  = H개월 내 재무/회계 기반 상폐
        0  = 정상
       -1  = 상폐현황 미매칭 (수동 확인 필요)
       -2  = 이미 상폐된 이후 시점 (학습 불가)
    """
    df = df.copy()
    df["period_date"] = df.apply(
        lambda r: q2date(int(r["year"]), r["quarter"]), axis=1
    )

    # T + H개월 이후는 라벨 미확정 → 제외 기준
    cutoff = REFERENCE_DATE - pd.DateOffset(months=H)

    def assign(row):
        # 라벨 미확정 구간
        if row["period_date"] > cutoff:
            return -3   # 미확정 → 나중에 제거

        # 정상 기업
        if row["label"] == 0:
            return 0

        # 상폐 기업
        code = row["stock_code"]
        t    = row["period_date"]

        if code not in delist_map or pd.isna(delist_map[code]):
            return -1   # 미매칭

        dd = delist_map[code]
        if dd <= t:
            return -2   # 이미 상폐 이후
        if t < dd <= t + pd.DateOffset(months=H):
            return 1    # 양성
        return 0        # H개월 초과 → 아직 정상

    df["rolling_label"] = df.apply(assign, axis=1)
    return df


# ─────────────────────────────────────────────────────────────
# 3. 거시경제 병합
# ─────────────────────────────────────────────────────────────
def merge_macro(df: pd.DataFrame, macro_path: Path) -> pd.DataFrame:
    if not macro_path.exists():
        print("  [경고] macro_quarterly.csv 없음 → 거시경제 컬럼 제외")
        return df
    macro = pd.read_csv(macro_path)
    macro["quarter"] = macro["quarter"].str.strip()
    df = df.merge(macro[["year", "quarter"] + MACRO_COLS],
                  on=["year", "quarter"], how="left")
    return df


# ─────────────────────────────────────────────────────────────
# 4. Time split
# ─────────────────────────────────────────────────────────────
def time_split(df: pd.DataFrame, H: int):
    """
    라벨 확정 가능한 행만 사용 (rolling_label >= 0).
    2025년은 H에 따라 자동으로 train에 포함되거나 제외됨.
    """
    valid_df = df[df["rolling_label"].isin([0, 1])].copy()
    # 원본 label 컬럼 제거 후 rolling_label을 label로 대체
    valid_df = valid_df.drop(columns=["label", "period_date", "data_source"], errors="ignore")
    valid_df = valid_df.rename(columns={"rolling_label": "label"})

    # 2015년 이전 제거
    valid_df = valid_df[valid_df["year"] >= 2015]

    train = valid_df[valid_df["year"].isin(TRAIN_YEARS +
            [y for y in valid_df["year"].unique()
             if y not in TRAIN_YEARS + VALID_YEARS + TEST_YEARS
             and y >= 2015 and y not in [2023, 2024]])]
    valid = valid_df[valid_df["year"].isin(VALID_YEARS)]
    test  = valid_df[valid_df["year"].isin(TEST_YEARS)]

    return train, valid, test


# ─────────────────────────────────────────────────────────────
# 5. 전처리 클래스
# ─────────────────────────────────────────────────────────────
class Preprocessor:
    def __init__(self):
        self.sector_quarter_medians = {}
        self.global_medians         = {}
        self.clip_bounds            = {}

    def fit(self, train: pd.DataFrame) -> "Preprocessor":
        ratio_cols_present = [c for c in RATIO_COLS if c in train.columns]

        # 섹터·분기별 중앙값
        for col in ratio_cols_present:
            grp = train.groupby(["gics_sector", "quarter"])[col].median()
            for (sec, qtr), val in grp.items():
                self.sector_quarter_medians[(sec, qtr, col)] = val
            self.global_medians[col] = train[col].median()

        # 클리핑 범위
        for col in ratio_cols_present:
            if col in DOMAIN_RULES:
                lo, hi = DOMAIN_RULES[col]
            else:
                q1  = train[col].quantile(0.25)
                q3  = train[col].quantile(0.75)
                iqr = q3 - q1
                lo  = q1 - 3 * iqr
                hi  = q3 + 3 * iqr
            self.clip_bounds[col] = (float(lo), float(hi))

        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ratio_cols_present = [c for c in RATIO_COLS if c in df.columns]

        # 1. ffill (기업 내 이전 분기값)
        df = df.sort_values(["stock_code", "year", "quarter"])
        df[ratio_cols_present] = (
            df.groupby("stock_code")[ratio_cols_present].ffill()
        )

        # 2. CF=0 (감가상각비 계열)
        cf_cols = [c for c in df.columns
                   if "상각비" in c or "감가" in c]
        for col in cf_cols:
            df[col] = df[col].fillna(0)

        # 3. 섹터·분기 중앙값 보간
        for col in ratio_cols_present:
            if df[col].isna().any():
                def _fill(row, col=col):
                    key = (row["gics_sector"], row["quarter"], col)
                    return self.sector_quarter_medians.get(key, np.nan)

                mask = df[col].isna()
                df.loc[mask, col] = df[mask].apply(_fill, axis=1)

            # 전체 중앙값 fallback
            still_na = df[col].isna()
            if still_na.any():
                df.loc[still_na, col] = self.global_medians.get(col, 0)

        # 4. 이상치 클리핑
        for col, (lo, hi) in self.clip_bounds.items():
            if col in df.columns:
                df[col] = df[col].clip(lo, hi)

        return df

    def to_dict(self) -> dict:
        return {
            "sector_quarter_medians": {
                str(k): v for k, v in self.sector_quarter_medians.items()
            },
            "global_medians": self.global_medians,
            "clip_bounds":    self.clip_bounds,
        }


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  Step 2: H별 라벨링 + Split + 전처리")
    print("=" * 65)

    # 데이터 로드
    print("\n데이터 로드 중...")
    df = pd.read_csv(COMBINED_CSV)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    print(f"  combined_raw: {len(df):,}행")

    # 상폐일 매핑
    delist_map = load_delist_map(DELISTED_XLSX)
    print(f"  상폐 매핑: {len(delist_map)}개 종목")

    # 거시경제 데이터
    df = merge_macro(df, MACRO_CSV)

    # H별 처리
    print(f"\nH값 목록: {H_LIST}\n")

    summary = []

    for H in H_LIST:
        print(f"\n{'─'*55}")
        print(f"  H = {H}개월")
        print(f"{'─'*55}")

        out_dir = OUT_BASE / f"H{H}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # 라벨링
        labeled = build_rolling_labels(df, H, delist_map)

        # 유효 행 통계
        valid_rows   = labeled[labeled["rolling_label"].isin([0, 1])]
        invalid_rows = labeled[~labeled["rolling_label"].isin([0, 1])]
        print(f"  유효 행: {len(valid_rows):,} "
              f"(양성 {int(valid_rows['rolling_label'].sum())})")
        print(f"  제외 행: {len(invalid_rows):,} "
              f"(-3 미확정: {(labeled['rolling_label']==-3).sum()}, "
              f"-2 상폐후: {(labeled['rolling_label']==-2).sum()}, "
              f"-1 미매칭: {(labeled['rolling_label']==-1).sum()})")

        # Time split
        train, valid, test = time_split(labeled, H)
        print(f"  train: {len(train):,}행 | 양성 {int(train['label'].sum())}")
        print(f"  valid: {len(valid):,}행 | 양성 {int(valid['label'].sum())}")
        print(f"  test : {len(test):,}행  | 양성 {int(test['label'].sum())}")

        # valid/test 양성 부족 경고
        if int(valid["label"].sum()) < 20:
            print(f"  [경고] valid 양성 샘플 부족 ({int(valid['label'].sum())}개)")
        if int(test["label"].sum()) < 20:
            print(f"  [경고] test 양성 샘플 부족 ({int(test['label'].sum())}개)")

        # 전처리 fit (train 기준)
        prep = Preprocessor()
        prep.fit(train)

        train = prep.transform(train)
        valid = prep.transform(valid)
        test  = prep.transform(test)

        # 저장
        train.to_csv(out_dir / "train.csv", index=False)
        valid.to_csv(out_dir / "valid.csv", index=False)
        test.to_csv(out_dir  / "test.csv",  index=False)

        meta = {
            "H_months":      H,
            "train_years":   TRAIN_YEARS,
            "valid_years":   VALID_YEARS,
            "test_years":    TEST_YEARS,
            "train_rows":    len(train),
            "valid_rows":    len(valid),
            "test_rows":     len(test),
            "train_pos":     int(train["label"].sum()),
            "valid_pos":     int(valid["label"].sum()),
            "test_pos":      int(test["label"].sum()),
            "imbalance_ratio": round(
                (len(train) - int(train["label"].sum()))
                / max(int(train["label"].sum()), 1), 1
            ),
            "preprocessor":  prep.to_dict(),
        }
        with open(out_dir / "meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print(f"  저장 완료 → {out_dir}")

        summary.append({
            "H":          H,
            "train_pos":  int(train["label"].sum()),
            "valid_pos":  int(valid["label"].sum()),
            "test_pos":   int(test["label"].sum()),
            "imbalance":  meta["imbalance_ratio"],
        })

    # 전체 요약
    print("\n" + "=" * 65)
    print("  H별 데이터셋 요약")
    print("=" * 65)
    print(f"\n  {'H':>4} | {'train 양성':>10} | {'valid 양성':>10} "
          f"| {'test 양성':>9} | {'불균형':>7}")
    print("  " + "-" * 50)
    for s in summary:
        flag = " ←" if s["valid_pos"] < 20 or s["test_pos"] < 20 else ""
        print(f"  {s['H']:>4} | {s['train_pos']:>10} | {s['valid_pos']:>10} "
              f"| {s['test_pos']:>9} | {s['imbalance']:>6.1f}:1{flag}")

    print("\n  ← 표시: valid/test 양성 20개 미만 — 평가 불안정")


if __name__ == "__main__":
    main()