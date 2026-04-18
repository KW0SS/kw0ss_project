"""
step1_build_combined.py
─────────────────────────────────────────────────────────────────────────────
전처리 파이프라인 Step 1

수행 작업:
  1. raw/healthy, raw/delisted 의 JSON → 재무비율 계산 (전처리 없이 원본 그대로)
  2. 구조적 상폐 / 더미 종목 제거
  3. YoY 증가율 계산 (매출액/순이익/영업이익)
  4. 연도별 현황 출력 (Step 2의 split 기준 결정용)
  5. combined_raw.csv 저장

NOTE:
  - clean_data.csv 사용 안 함 (raw 폴더에 전체 원본 JSON이 있으므로)
  - 전처리(결측치 보간, 이상치 클리핑)는 Step 2에서 split 이후에 수행
  - 매출액/순이익/영업이익 증가율은 IS frmtrm 결측 문제로 YoY 방식 사용
    (ratio_calculator.py에서 제거, 여기서 전담)

수정 이력
─────────
[2025-04-17] YoY 증가율 계산 통합.
  - DART 분기/반기 보고서의 IS frmtrm 미기재로 frmtrm 기반 증가율
    Q1/H1/Q3 결측률 92%+ 확인 → 전년 동기(YoY) 조인 방식으로 교체.
  - ratio_calculator.py에서 매출액/순이익/영업이익 증가율 함수 제거.
  - process_folder()에서 YoY 계산용 원본값(revenue, net_income,
    operating_income) 임시 저장 후 _add_yoy_growth_cols()에서 전년
    동기 조인으로 증가율 계산. 임시 컬럼은 저장 전 제거됨.
  - 개선 결과: 결측률 64~74% → 6~18% (valid/test 기준 74% → 6~7%)

출력:
  data/processed/combined_raw.csv   ← Step 2 입력 (전처리 전 원본)

실행:
  python build_master_dataset.py
"""

import json
import warnings
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# 경로 설정  ← 필요시 수정
# ─────────────────────────────────────────────────────────────
BASE_DIR     = Path(r"C:\kwu\KW0SS_PROJECT\kw0ss_project2\preprocess")
RAW_HEALTHY  = BASE_DIR / "data" / "raw" / "healthy"
RAW_DELISTED = BASE_DIR / "data" / "raw" / "delisted"
OUT_DIR      = BASE_DIR / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 제거 대상: 구조적 상폐 4개 + 더미
EXCLUDE_CODES = {
    "048260",  # 오스템임플란트 (자진상폐)
    "029960",  # 코엔텍 (완전자회사)
    "006580",  # 대양제지 (자진상폐)
    "115960",  # 연우 (완전자회사)
    "999999",  # 더미
}

PATTERN = re.compile(r"^(\d{6})_(\d{4})_(Q1|Q3|H1|ANNUAL)\.json$")

# [2025-04-17] YoY 증가율 계산 대상.
# (컬럼명, 원본 피처명) 쌍.
# ratio_calculator.py에서 제거된 IS 증가율 3개를 여기서 전담.
YOY_TARGETS: list[tuple[str, str]] = [
    ("매출액증가율",   "revenue"),
    ("순이익증가율",   "net_income"),
    ("영업이익증가율", "operating_income"),
]


# ─────────────────────────────────────────────────────────────
# 1. JSON → 재무비율
# ─────────────────────────────────────────────────────────────
def process_folder(folder: Path, label: int) -> pd.DataFrame:
    """
    폴더 안의 JSON 파일을 읽어 재무비율 DataFrame 반환.
    하위 폴더(섹터) 구조 지원.
    label: 0=healthy, 1=delisted
    """
    sys.path.insert(0, str(BASE_DIR / "src"))
    from account_mapper import extract_standard_items
    from ratio_calculator import compute_all_ratios

    files = list(folder.rglob("*.json"))
    print(f"  {'healthy' if label==0 else 'delisted'}: {len(files):,}개 파일")

    records = []
    fail_count = 0

    for i, fp in enumerate(files):
        if i % 2000 == 0 and i > 0:
            print(f"    {i:,} / {len(files):,} 처리 중...")

        m = PATTERN.match(fp.name)
        if not m:
            continue

        code    = m.group(1).zfill(6)
        year    = int(m.group(2))
        quarter = m.group(3)
        sector  = fp.parent.name   # 하위 폴더명 = 섹터

        if code in EXCLUDE_CODES:
            continue

        try:
            with open(fp, encoding="utf-8") as f:
                dart_items = json.load(f)
        except Exception:
            fail_count += 1
            continue

        if not dart_items:
            fail_count += 1
            continue

        # 표준 키 추출 → 재무비율 계산
        std_items = extract_standard_items(dart_items)
        ratios    = compute_all_ratios(std_items)

        record = {
            "stock_code":  code,
            "year":        year,
            "quarter":     quarter,
            "label":       label,
            "gics_sector": sector,
        }
        record.update(ratios)

        # [2025-04-17] YoY 증가율 계산용 원본값 임시 저장.
        # _add_yoy_growth_cols()에서 전년 동기 조인에 사용 후 제거됨.
        # 컬럼명 앞에 '_yoy_src_' prefix를 붙여 최종 출력 컬럼과 구분.
        for _, feat in YOY_TARGETS:
            entry = std_items.get(feat)
            record[f"_yoy_src_{feat}"] = entry.get("thstrm") if entry else None

        records.append(record)

    if fail_count:
        print(f"    파싱 실패: {fail_count}개 파일")

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────
# 2. YoY 증가율 계산
# ─────────────────────────────────────────────────────────────
def _add_yoy_growth_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    전년 동기(YoY) 조인으로 IS 증가율 3개 계산 후 임시 컬럼 제거.

    처리 흐름:
      1. (stock_code, year-1, quarter) 키로 전년 동기 값 조인
      2. (당기 - 전기) / |전기| * 100 계산
         - 분모 abs(): 전기 음수 시 부호 왜곡 방지
         - 전기 0: NaN 처리
      3. _yoy_src_* 임시 컬럼 제거
    """
    df = df.copy()
    df["_prev_year"] = df["year"] - 1

    src_cols = [f"_yoy_src_{feat}" for _, feat in YOY_TARGETS]

    # 전년 동기 값 조인
    prev_df = df[["stock_code", "year", "quarter"] + src_cols].copy()
    prev_df = prev_df.rename(columns={c: f"{c}_prev" for c in src_cols})

    df = df.merge(
        prev_df,
        left_on=["stock_code", "_prev_year", "quarter"],
        right_on=["stock_code", "year", "quarter"],
        suffixes=("", "_prev_key"),
        how="left",
    )

    # merge로 생긴 중복 key 컬럼 제거
    for col in ["year_prev_key", "quarter_prev_key"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # YoY 증가율 계산
    for col_name, feat in YOY_TARGETS:
        src      = f"_yoy_src_{feat}"
        src_prev = f"_yoy_src_{feat}_prev"

        curr  = df[src]
        prev  = df[src_prev]
        denom = prev.abs()

        growth = (curr - prev) / denom * 100
        growth[denom == 0] = np.nan   # 전기 0이면 NaN
        df[col_name] = growth

    # 임시 컬럼 제거 (_yoy_src_* 및 조인용 _prev_year)
    drop_cols = (
        ["_prev_year"]
        + src_cols
        + [f"{c}_prev" for c in src_cols]
    )
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    return df


# ─────────────────────────────────────────────────────────────
# 3. 연도별 현황 출력
# ─────────────────────────────────────────────────────────────
def print_yearly_stats(df: pd.DataFrame):
    print("\n" + "=" * 65)
    print("  연도별 현황 — Step 2 split 기준 결정에 활용하세요")
    print("=" * 65)
    print(f"\n  {'연도':<6} {'전체행':>8} {'기업수':>7} {'상폐기업수':>10} {'상폐행수':>9}")
    print("  " + "-" * 44)

    by_year = df.groupby("year")
    for year, grp in by_year:
        total         = len(grp)
        companies     = grp["stock_code"].nunique()
        pos_companies = grp[grp["label"] == 1]["stock_code"].nunique()
        pos_rows      = int(grp["label"].sum())
        flag = "  ← 양성 10개 미만" if pos_rows < 10 else ""
        print(f"  {int(year):<6} {total:>8,} {companies:>7,} "
              f"{pos_companies:>10} {pos_rows:>9}{flag}")

    total_pos = int(df["label"].sum())
    total_neg = int((df["label"] == 0).sum())
    print(f"\n  전체: {len(df):,}행 | "
          f"정상 {total_neg:,} : 상폐 {total_pos} "
          f"= {total_neg // max(total_pos, 1)}:1")

    print("\n  [참고] H별 라벨링 시 양성 샘플 수가 달라집니다.")
    print("  Step 2에서 H값에 따라 split 연도를 결정하세요.")
    print("  일반적으로: valid/test 각각 양성 20개 이상 확보 권장")


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  Step 1: JSON → 재무비율 계산 (raw 폴더 전체)")
    print("  ※ clean_data.csv 미사용 — 전처리 없이 원본 그대로 저장")
    print("=" * 65)

    # JSON → 재무비율 (healthy + delisted 전체)
    print("\n[1/3] raw 데이터 변환 중...")
    healthy  = process_folder(RAW_HEALTHY,  label=0)
    delisted = process_folder(RAW_DELISTED, label=1)
    combined = pd.concat([healthy, delisted], ignore_index=True)
    combined = combined.sort_values(
        ["stock_code", "year", "quarter"]
    ).reset_index(drop=True)
    print(f"  변환 완료: {len(combined):,}행 "
          f"(healthy {len(healthy):,} + delisted {len(delisted):,})")
    print(f"  기업 수: {combined['stock_code'].nunique():,}")

    # YoY 증가율 계산
    print("\n[2/3] YoY 증가율 계산 중...")
    combined = _add_yoy_growth_cols(combined)
    for col_name, _ in YOY_TARGETS:
        null_rate = combined[col_name].isna().mean()
        print(f"  {col_name} 결측률: {null_rate:.1%}")

    # 저장
    out_path = OUT_DIR / "combined_raw.csv"
    combined.to_csv(out_path, index=False)
    print(f"\n  저장 완료 → {out_path}")

    # 연도별 현황 출력
    print("\n[3/3] 연도별 현황 확인...")
    print_yearly_stats(combined)

    print("\n" + "=" * 65)
    print("  Step 1 완료")
    print("  다음: 연도별 현황 확인 후 H값과 split 기준 결정")
    print("        → step2_label_and_preprocess.py 실행")
    print("=" * 65)


if __name__ == "__main__":
    main()