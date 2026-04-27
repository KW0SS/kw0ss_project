"""
build_master_dataset.py
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
[2025-04-17] _CFS, _OFS suffix 대응 패턴 추가.
[2025-04-17] YoY 증가율 계산 추가.
  - DART 분기/반기 보고서는 IS frmtrm을 비워 공시하는 경우가 많아
    기존 frmtrm 기반 증가율 계산 시 Q1/H1/Q3 결측률 92%+.
  - 전년 동기(YoY) 방식으로 교체: 결측률 74% → 14%로 개선.
  - process_file()에서 revenue/net_income/operating_income thstrm 값 보존.
  - _add_yoy_growth_cols()에서 전년 동기 조인 후 증가율 계산.
[2025-04-27] process_folder 병렬처리 추가 (ProcessPoolExecutor).
  - 단일 프로세스 대비 workers 수만큼 처리 속도 향상.
  - workers 기본값: CPU 코어 수 - 1.
  - 실행: python build_master_dataset.py --workers 8

출력:
  data/processed/combined_raw.csv   ← Step 2 입력 (전처리 전 원본)

실행:
  python build_master_dataset.py
  python build_master_dataset.py --workers 8  # workers 수 직접 지정
"""

import json
import warnings
import re
import sys
import argparse
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
from tqdm import tqdm

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

# [2025-04-17] _CFS, _OFS suffix 대응 추가
# 수집 스크립트 버전에 따라 파일명에 _CFS가 붙는 경우가 있음
PATTERN = re.compile(r"^(\d{6})_(\d{4})_(Q1|Q3|H1|ANNUAL)(?:_CFS|_OFS)?\.json$")

# [2025-04-17] YoY 증가율 계산용 원본 항목 목록
# thstrm(당기) 값만 저장. 전년 동기 조인은 _add_yoy_growth_cols()에서 수행.
YOY_TARGETS: list[tuple[str, str]] = [
    ("매출액증가율",   "revenue"),
    ("순이익증가율",   "net_income"),
    ("영업이익증가율", "operating_income"),
]
YOY_SOURCE_KEYS = [feat for _, feat in YOY_TARGETS]


# ─────────────────────────────────────────────────────────────
# 1. 단일 파일 처리 (병렬 worker용)
# ─────────────────────────────────────────────────────────────
def process_file(args: tuple) -> dict | None:
    """
    단일 JSON 파일을 읽어 재무비율 record 반환.
    ProcessPoolExecutor worker로 실행되므로 독립적으로 동작해야 함.
    """
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
        "label":       label,
        "gics_sector": sector,
    }
    record.update(ratios)

    # [2025-04-17] YoY 증가율 계산용 원본값 저장
    # DART 분기/반기 보고서는 frmtrm을 비워 공시해 기존 방식 결측률 92%+.
    # 전년 동기 조인을 위해 thstrm 값 보존.
    for key in YOY_SOURCE_KEYS:
        entry = std_items.get(key)
        record[f"_yoy_src_{key}"] = entry.get("thstrm") if entry else None

    return record


# ─────────────────────────────────────────────────────────────
# 2. 폴더 병렬 처리
# ─────────────────────────────────────────────────────────────
def process_folder(folder: Path, label: int, workers: int) -> pd.DataFrame:
    """
    폴더 안의 JSON 파일을 병렬로 읽어 재무비율 DataFrame 반환.

    Parameters
    ----------
    folder  : raw/healthy 또는 raw/delisted 경로
    label   : 0=healthy, 1=delisted
    workers : 병렬 프로세스 수
    """
    files  = list(folder.rglob("*.json"))
    split  = "healthy" if label == 0 else "delisted"
    print(f"  {split}: {len(files):,}개 파일 (workers={workers})")

    args_list = [(fp, label, BASE_DIR) for fp in files]
    records   = []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, a): a for a in args_list}
        for future in tqdm(
            as_completed(futures),
            total=len(files),
            desc=f"  {split}",
            leave=False,
        ):
            result = future.result()
            if result is not None:
                records.append(result)

    df = pd.DataFrame(records)
    print(f"  {split}: {len(df):,}행 변환 완료")
    return df


# ─────────────────────────────────────────────────────────────
# 3. YoY 증가율 계산
# ─────────────────────────────────────────────────────────────
def _add_yoy_growth_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    전년 동기 조인 방식으로 IS 증가율 3개 계산.

    [2025-04-17] frmtrm 기반 계산 교체 이유:
    DART 분기/반기 보고서는 IS frmtrm을 비워 공시하는 경우가 많아
    Q1/H1/Q3 결측률이 92%+로 사실상 사용 불가.
    전년 동기(YoY) 방식: 결측률 74% → 14%로 개선.

    분모에 abs() 사용 이유:
    전기가 음수일 때 단순 나눗셈은 부호가 뒤집혀 해석이 왜곡됨.
    """
    df = df.copy()
    df["_prev_year"] = df["year"] - 1

    src_cols = [f"_yoy_src_{feat}" for _, feat in YOY_TARGETS]

    # 전년 동기 조인: (stock_code, year-1, quarter) 기준
    prev_df = df[["stock_code", "year", "quarter"] + src_cols].copy()
    df = df.merge(
        prev_df,
        left_on=["stock_code", "_prev_year", "quarter"],
        right_on=["stock_code", "year", "quarter"],
        suffixes=("", "_prev"),
        how="left",
    )
    df = df.drop(columns=["year_prev", "quarter_prev"], errors="ignore")

    # YoY 증가율 계산
    for col_name, feat in YOY_TARGETS:
        src      = f"_yoy_src_{feat}"
        src_prev = f"_yoy_src_{feat}_prev"
        if src not in df.columns or src_prev not in df.columns:
            continue
        denom = df[src_prev].abs()
        result = (df[src] - df[src_prev]) / denom * 100
        result[denom == 0] = np.nan
        df[col_name] = result

    # 임시 컬럼 제거
    drop_cols = (
        ["_prev_year"]
        + src_cols
        + [f"{c}_prev" for c in src_cols]
    )
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    return df


# ─────────────────────────────────────────────────────────────
# 4. 연도별 현황 출력
# ─────────────────────────────────────────────────────────────
def print_yearly_stats(df: pd.DataFrame):
    print("\n" + "=" * 65)
    print("  연도별 현황 — Step 2 split 기준 결정에 활용하세요")
    print("=" * 65)
    print(f"\n  {'연도':<6} {'전체행':>8} {'기업수':>7} {'상폐기업수':>10} {'상폐행수':>9}")
    print("  " + "-" * 44)

    for year, grp in df.groupby("year"):
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
    parser = argparse.ArgumentParser(
        description="Step 1: JSON → 재무비율 계산 + YoY 증가율"
    )
    parser.add_argument(
        "--workers", type=int,
        default=max(1, os.cpu_count() - 1),
        help="병렬 프로세스 수 (기본: CPU 코어 수 - 1)"
    )
    args = parser.parse_args()

    print("=" * 65)
    print("  Step 1: JSON → 재무비율 계산 (raw 폴더 전체)")
    print(f"  workers: {args.workers} / CPU: {os.cpu_count()}")
    print("=" * 65)

    # [1/3] JSON → 재무비율 (병렬)
    print("\n[1/3] raw 데이터 병렬 변환 중...")
    healthy  = process_folder(RAW_HEALTHY,  label=0, workers=args.workers)
    delisted = process_folder(RAW_DELISTED, label=1, workers=args.workers)

    combined = pd.concat([healthy, delisted], ignore_index=True)
    combined = combined.sort_values(
        ["stock_code", "year", "quarter"]
    ).reset_index(drop=True)
    print(f"\n  변환 완료: {len(combined):,}행 "
          f"(healthy {len(healthy):,} + delisted {len(delisted):,})")
    print(f"  기업 수: {combined['stock_code'].nunique():,}")

    # [2/3] YoY 증가율 계산
    print("\n[2/3] YoY 증가율 계산 중...")
    combined = _add_yoy_growth_cols(combined)
    for col_name, _ in YOY_TARGETS:
        null_rate = combined[col_name].isna().mean()
        print(f"  {col_name} 결측률: {null_rate:.1%}")

    # 저장
    out_path = OUT_DIR / "combined_raw.csv"
    combined.to_csv(out_path, index=False)
    print(f"  저장 완료 → {out_path}")

    # [3/3] 연도별 현황 출력
    print("\n[3/3] 연도별 현황 확인...")
    print_yearly_stats(combined)

    print("\n" + "=" * 65)
    print("  Step 1 완료")
    print("  다음: 연도별 현황 확인 후 H값과 split 기준 결정")
    print("        → build_h_datasets.py 실행")
    print("=" * 65)


if __name__ == "__main__":
    main()