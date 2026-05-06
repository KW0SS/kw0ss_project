"""
build_fixed_datasets.py
─────────────────────────────────────────────────────────────────────────────
전처리 파이프라인 Step 2 (고정 N년 전 라벨링 버전)

build_h_datasets.py 와의 차이:
  - H 방식: T 시점 기준으로 이후 H개월 내 상폐 여부 → 여러 분기가 양성
  - N 방식: 상폐 연도 기준으로 정확히 N년 전 연도의 행만 양성
    예) 상폐연도=2022, N=1 → 2021년 행(Q1/H1/Q3/ANNUAL 모두) label=1

수행 작업:
  1. combined_raw.csv + 상폐일 → N값별 고정 라벨링
  2. 2015년 이전 제거
  3. N별 연도 현황 출력 → split 기준 확인
  4. time split (train/valid/test)  ← 기준 동일: 2015~2022 / 2023 / 2024
  5. 전처리 fit/transform            ← Preprocessor 동일
     - ffill
     - CF=0
     - 섹터·분기 중앙값 보간
     - 이상치 클리핑
     - (옵션) Winsorize
     - (옵션) RobustScaler
  6. N별 데이터셋 저장

출력 구조:
  data/processed/
    fixed_N1/  baseline/  train.csv, valid.csv, test.csv, meta.json
               exp-A/     ...
               exp-B/     ...
               exp-C/     ...
    fixed_N2/  ...
    fixed_N3/  ...

실행:
  python build_fixed_datasets.py
"""

import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats.mstats import winsorize as scipy_winsorize
from sklearn.preprocessing import RobustScaler

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# 설정  ← 필요시 수정
# ─────────────────────────────────────────────────────────────
BASE_DIR      = Path(r"C:\kwu\KW0SS_PROJECT\kw0ss_project2\preprocess")
COMBINED_CSV  = BASE_DIR / "data" / "combined_raw.csv"
DELISTED_XLSX = BASE_DIR / "data" / "상장폐지현황.xlsx"
MACRO_CSV     = BASE_DIR / "data" / "macro" / "macro_quarterly.csv"
OUT_BASE      = BASE_DIR / "data" / "processed"

# 실험할 N값 목록 (년)
# N=1: 상폐 1년 전 연도만 양성 (가장 엄격 — 박민서 버전 3에 대응)
# N=2: 상폐 2년 전 연도만 양성
# N=3: 상폐 3년 전 연도만 양성
N_LIST = [1, 2, 3]

# Time split 기준 (build_h_datasets.py 와 동일)
TRAIN_YEARS = list(range(2015, 2023))   # 2015~2022
VALID_YEARS = [2023]
TEST_YEARS  = [2024]

# 재무비율 컬럼 (build_h_datasets.py 와 동일)
RATIO_COLS = [
    '총자산증가율', '유동자산증가율', '매출액증가율', '순이익증가율', '영업이익증가율',
    '매출액순이익률', '매출총이익률', '자기자본순이익률',
    '매출채권회전율', '재고자산회전율', '총자본회전율',
    '유형자산회전율', '매출원가율', '부채비율', '유동비율', '자기자본비율',
    '당좌비율', '비유동자산장기적합률', '순운전자본비율', '차입금의존도',
    '현금비율', '유형자산', '무형자산', '총자본영업이익률', '총자본순이익률',
    '유보액/납입자본비율', '총자본투자효율',
]
MACRO_COLS = [
    'credit_spread', 'kosdaq_return', 'gdp_growth_yoy',
    'usdkrw_chg', 'vix_avg', 'cpi_yoy',
]

# 도메인 룰 기반 이상치 범위 (build_h_datasets.py 와 동일)
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

WINSORIZE_COLS = [
    '매출액순이익률',
    '자기자본순이익률',
    '총자본영업이익률',
    '총자본순이익률',
    '매출총이익률',
]
WINSORIZE_LIMITS = (0.01, 0.01)

QUARTER_TO_MONTH = {"Q1": 3, "H1": 6, "Q3": 9, "ANNUAL": 12}


# ─────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────
def q2date(year: int, quarter: str) -> pd.Timestamp:
    month = QUARTER_TO_MONTH.get(quarter, 12)
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


# ─────────────────────────────────────────────────────────────
# 1. 상폐일 매핑 로드 (build_h_datasets.py 와 동일)
# ─────────────────────────────────────────────────────────────
def load_delist_map(xlsx_path: Path) -> dict:
    df = pd.read_excel(xlsx_path)
    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
    df["폐지일자"] = pd.to_datetime(df["폐지일자"], errors="coerce")
    return dict(zip(df["종목코드"], df["폐지일자"]))


# ─────────────────────────────────────────────────────────────
# 2. 고정 N년 전 라벨링  ← 핵심 변경 부분
# ─────────────────────────────────────────────────────────────
def build_fixed_labels(df: pd.DataFrame,
                        N: int,
                        delist_map: dict) -> pd.DataFrame:
    """
    각 (기업, 시점)에 대해 상폐 연도 기준 정확히 N년 전 연도 여부로 라벨 계산.

    fixed_label:
         1  = 상폐연도 기준 정확히 N년 전 연도의 행
         0  = 정상기업 행 또는 상폐기업의 N년 전이 아닌 행
        -1  = 상폐현황 미매칭 (수동 확인 필요)
        -2  = 이미 상폐된 이후 시점 (학습 불가)

    H 방식과의 차이:
        H 방식은 기준 시점 T에서 앞을 바라보며(forward-looking) H개월 내 상폐 여부를 봄.
        N 방식은 상폐 연도에서 뒤를 바라보며(backward-looking) N년 전 연도를 고정으로 찍음.
        → H 방식은 같은 상폐기업의 여러 분기가 양성이 될 수 있지만,
          N 방식은 정확히 N년 전 연도의 분기들(Q1/H1/Q3/ANNUAL)만 양성.
    """
    df = df.copy()
    df["period_date"] = df.apply(
        lambda r: q2date(int(r["year"]), r["quarter"]), axis=1
    )

    def assign(row):
        # 정상기업
        if row["label"] == 0:
            return 0

        code = row["stock_code"]
        if code not in delist_map or pd.isna(delist_map[code]):
            return -1

        dd = delist_map[code]

        # 이미 상폐된 이후 시점
        if row["period_date"] >= dd:
            return -2

        # 상폐 연도에서 정확히 N년 전 연도인지 확인
        delist_year = dd.year
        target_year = delist_year - N

        if row["year"] == target_year:
            return 1

        # 상폐기업이지만 N년 전이 아닌 시점 → 음성
        return 0

    df["fixed_label"] = df.apply(assign, axis=1)
    return df


# ─────────────────────────────────────────────────────────────
# 3. 거시경제 병합 (build_h_datasets.py 와 동일)
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
def time_split(df: pd.DataFrame):
    """
    라벨 확정 가능한 행만 사용 (fixed_label >= 0).
    split 기준은 build_h_datasets.py 와 동일.
    """
    valid_df = df[df["fixed_label"].isin([0, 1])].copy()
    valid_df = valid_df.drop(
        columns=["label", "period_date", "data_source"], errors="ignore"
    )
    valid_df = valid_df.rename(columns={"fixed_label": "label"})
    valid_df = valid_df[valid_df["year"] >= 2015]

    train = valid_df[valid_df["year"].isin(TRAIN_YEARS +
            [y for y in valid_df["year"].unique()
             if y not in TRAIN_YEARS + VALID_YEARS + TEST_YEARS
             and y >= 2015 and y not in [2023, 2024]])]
    valid = valid_df[valid_df["year"].isin(VALID_YEARS)]
    test  = valid_df[valid_df["year"].isin(TEST_YEARS)]

    return train, valid, test


# ─────────────────────────────────────────────────────────────
# 5. 전처리 클래스 (build_h_datasets.py 와 동일)
# ─────────────────────────────────────────────────────────────
class Preprocessor:
    def __init__(self, winsorize: bool = False, robust_scale: bool = False):
        self.winsorize    = winsorize
        self.robust_scale = robust_scale

        self.sector_quarter_medians = {}
        self.global_medians         = {}
        self.clip_bounds            = {}
        self.winsorize_bounds: dict[str, tuple[float, float]] = {}
        self._robust_scaler: RobustScaler | None = None
        self._robust_cols:   list[str]           = []

    def fit(self, train: pd.DataFrame) -> "Preprocessor":
        ratio_cols_present = [c for c in RATIO_COLS if c in train.columns]

        for col in ratio_cols_present:
            grp = train.groupby(["gics_sector", "quarter"])[col].median()
            for (sec, qtr), val in grp.items():
                self.sector_quarter_medians[(sec, qtr, col)] = val
            self.global_medians[col] = train[col].median()

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

        if self.winsorize:
            win_cols = [c for c in WINSORIZE_COLS if c in train.columns]
            for col in win_cols:
                s = train[col].dropna()
                lo = float(s.quantile(WINSORIZE_LIMITS[0]))
                hi = float(s.quantile(1 - WINSORIZE_LIMITS[1]))
                self.winsorize_bounds[col] = (lo, hi)

        if self.robust_scale:
            self._robust_cols = ratio_cols_present
            self._robust_scaler = RobustScaler()
            fit_data = train[self._robust_cols].fillna(0)
            self._robust_scaler.fit(fit_data)

        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ratio_cols_present = [c for c in RATIO_COLS if c in df.columns]

        # 1. ffill
        df = df.sort_values(["stock_code", "year", "quarter"])
        df[ratio_cols_present] = (
            df.groupby("stock_code")[ratio_cols_present].ffill()
        )

        # 2. CF=0
        cf_cols = [c for c in df.columns if "상각비" in c or "감가" in c]
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

            still_na = df[col].isna()
            if still_na.any():
                df.loc[still_na, col] = self.global_medians.get(col, 0)

        # 4. 이상치 클리핑
        for col, (lo, hi) in self.clip_bounds.items():
            if col in df.columns:
                df[col] = df[col].clip(lo, hi)

        # 5. Winsorize (옵션)
        if self.winsorize:
            for col, (lo, hi) in self.winsorize_bounds.items():
                if col in df.columns:
                    df[col] = df[col].clip(lo, hi)

        # 6. RobustScaler (옵션)
        if self.robust_scale and self._robust_scaler is not None:
            cols = [c for c in self._robust_cols if c in df.columns]
            df[cols] = self._robust_scaler.transform(df[cols].fillna(0))

        return df

    def to_dict(self) -> dict:
        return {
            "winsorize":    self.winsorize,
            "robust_scale": self.robust_scale,
            "sector_quarter_medians": {
                str(k): v for k, v in self.sector_quarter_medians.items()
            },
            "global_medians":    self.global_medians,
            "clip_bounds":       self.clip_bounds,
            "winsorize_bounds":  self.winsorize_bounds,
            "robust_scale_center": (
                self._robust_scaler.center_.tolist()
                if self._robust_scaler else []
            ),
            "robust_scale_scale": (
                self._robust_scaler.scale_.tolist()
                if self._robust_scaler else []
            ),
        }


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  고정 N년 전 라벨링 데이터셋 생성")
    print("  (build_h_datasets.py의 rolling 방식과 비교용)")
    print("=" * 65)

    print("\n데이터 로드 중...")
    df = pd.read_csv(COMBINED_CSV)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    print(f"  combined_raw (전체): {len(df):,}행")

    # ── 분기 전체 사용 ────────────────────────────────────────────
    # N년 전 연도에 해당하는 Q1/H1/Q3/ANNUAL 행을 모두 양성으로 처리.
    # H 방식도 H개월 내 여러 분기가 양성이 되므로 비교 조건이 대칭.
    # ANNUAL만 쓰면 time split 구조상 valid/test 양성이 20개 미만으로
    # 평가가 불안정해지는 문제가 있어 전 분기를 사용함.
    print(f"  고유 기업 수:         {df['stock_code'].nunique():,}개")
    print(f"  연도 범위:            {df['year'].min()} ~ {df['year'].max()}")

    delist_map = load_delist_map(DELISTED_XLSX)
    print(f"  상폐 매핑:            {len(delist_map)}개 종목")

    df = merge_macro(df, MACRO_CSV)

    EXPERIMENTS = [
        ("baseline", False, False),
        ("exp-A",    True,  False),
        ("exp-B",    False, True),
        ("exp-C",    True,  True),
    ]

    print(f"\nN값 목록: {N_LIST}")
    print(f"실험 설정: {[e[0] for e in EXPERIMENTS]}\n")

    summary: list[dict] = []

    for N in N_LIST:
        print(f"\n{'─'*55}")
        print(f"  N = {N}년 전  (상폐연도 - {N} 연도의 행만 양성)")
        print(f"{'─'*55}")

        labeled = build_fixed_labels(df, N, delist_map)

        valid_rows   = labeled[labeled["fixed_label"].isin([0, 1])]
        invalid_rows = labeled[~labeled["fixed_label"].isin([0, 1])]
        print(f"  유효 행: {len(valid_rows):,} "
              f"(양성 {int((valid_rows['fixed_label'] == 1).sum())})")
        print(f"  제외 행: {len(invalid_rows):,} "
              f"(-2 상폐후: {(labeled['fixed_label']==-2).sum()}, "
              f"-1 미매칭: {(labeled['fixed_label']==-1).sum()})")

        # H 방식과의 양성 수 차이 안내
        pos_count = int((valid_rows["fixed_label"] == 1).sum())
        print(f"  ※ H 방식 대비: 같은 상폐기업이라도 {N}년 전 연도 행만 양성 "
              f"(Q1+H1+Q3+ANNUAL 최대 4개 행/기업)")

        train_raw, valid_raw, test_raw = time_split(labeled)
        print(f"  train: {len(train_raw):,}행 | 양성 {int(train_raw['label'].sum())}")
        print(f"  valid: {len(valid_raw):,}행 | 양성 {int(valid_raw['label'].sum())}")
        print(f"  test : {len(test_raw):,}행  | 양성 {int(test_raw['label'].sum())}")

        if int(valid_raw["label"].sum()) < 20:
            print(f"  [경고] valid 양성 샘플 부족 ({int(valid_raw['label'].sum())}개) "
                  f"— N값 조정 또는 해석 주의")
        if int(test_raw["label"].sum()) < 20:
            print(f"  [경고] test 양성 샘플 부족 ({int(test_raw['label'].sum())}개) "
                  f"— N값 조정 또는 해석 주의")

        for exp_name, do_winsorize, do_robust in EXPERIMENTS:
            out_dir = OUT_BASE / f"fixed_N{N}" / exp_name
            out_dir.mkdir(parents=True, exist_ok=True)

            prep = Preprocessor(winsorize=do_winsorize, robust_scale=do_robust)
            prep.fit(train_raw)

            train = prep.transform(train_raw)
            valid = prep.transform(valid_raw)
            test  = prep.transform(test_raw)

            train.to_csv(out_dir / "train.csv", index=False)
            valid.to_csv(out_dir / "valid.csv", index=False)
            test.to_csv(out_dir  / "test.csv",  index=False)

            # ── 메타데이터 ─────────────────────────────────────────
            # H 방식(build_h_datasets.py)과 비교 가능하도록
            # 데이터 생성 과정 전반을 기록.
            train_pos = int(train["label"].sum())
            valid_pos = int(valid["label"].sum())
            test_pos  = int(test["label"].sum())

            meta = {
                # 라벨링 방식
                "label_type":   "fixed",
                "N_years":      N,
                "quarter_filter": "all quarters (Q1/H1/Q3/ANNUAL)",
                "labeling_rule": (
                    f"상폐연도 기준 정확히 {N}년 전 연도의 전 분기(Q1/H1/Q3/ANNUAL) label=1. "
                    f"H 방식이 H개월 내 여러 분기를 양성으로 보는 것과 대칭 구조. "
                    f"차이는 forward-looking(H) vs backward-looking(N) 라벨링 방식."
                ),

                # 실험 설정
                "experiment":   exp_name,
                "winsorize":    do_winsorize,
                "robust_scale": do_robust,

                # split 기준
                "train_years":  TRAIN_YEARS,
                "valid_years":  VALID_YEARS,
                "test_years":   TEST_YEARS,

                # 라벨링 단계 집계 (split 전)
                "total_annual_rows":      len(labeled),
                "total_valid_rows":       int(labeled["fixed_label"].isin([0, 1]).sum()),
                "total_pos_before_split": int((labeled["fixed_label"] == 1).sum()),
                "excluded_after_delist":  int((labeled["fixed_label"] == -2).sum()),
                "excluded_no_match":      int((labeled["fixed_label"] == -1).sum()),
                "unique_companies":       int(labeled["stock_code"].nunique()),
                "unique_delisted_pos":    int(
                    labeled[labeled["fixed_label"] == 1]["stock_code"].nunique()
                ),
                "year_range": {
                    "min": int(labeled["year"].min()),
                    "max": int(labeled["year"].max()),
                },

                # split 후 집계
                "train_rows":      len(train),
                "valid_rows":      len(valid),
                "test_rows":       len(test),
                "train_pos":       train_pos,
                "valid_pos":       valid_pos,
                "test_pos":        test_pos,
                "train_neg":       len(train) - train_pos,
                "valid_neg":       len(valid) - valid_pos,
                "test_neg":        len(test)  - test_pos,
                "imbalance_ratio": round(
                    (len(train) - train_pos) / max(train_pos, 1), 1
                ),

                # 전처리 파라미터 (재현용)
                "preprocessor": prep.to_dict(),
            }
            with open(out_dir / "meta.json", "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)

            print(f"  [{exp_name}] 저장 완료 → {out_dir}")

            summary.append({
                "N":         N,
                "exp":       exp_name,
                "train_pos": int(train["label"].sum()),
                "valid_pos": int(valid["label"].sum()),
                "test_pos":  int(test["label"].sum()),
                "imbalance": meta["imbalance_ratio"],
            })

    # 전체 요약
    print("\n" + "=" * 65)
    print("  N별 데이터셋 요약 (baseline 기준)")
    print("=" * 65)
    print(f"\n  {'N':>4} | {'train 양성':>10} | {'valid 양성':>10} "
          f"| {'test 양성':>9} | {'불균형':>7}")
    print("  " + "-" * 50)
    for s in summary:
        if s["exp"] != "baseline":
            continue
        flag = " ←" if s["valid_pos"] < 20 or s["test_pos"] < 20 else ""
        print(f"  {s['N']:>4} | {s['train_pos']:>10} | {s['valid_pos']:>10} "
              f"| {s['test_pos']:>9} | {s['imbalance']:>6.1f}:1{flag}")

    print("\n  ← 표시: valid/test 양성 20개 미만 — 평가 불안정")
    print(f"\n  저장 구조: data/processed/fixed_N{{N}}/{{실험명}}/train|valid|test.csv")
    print(f"\n  H 방식과 비교 시 동일 combined_raw + 동일 전처리 설정 사용.")
    print(f"  라벨링 방식 차이만 분리해서 확인 가능.")


if __name__ == "__main__":
    main()