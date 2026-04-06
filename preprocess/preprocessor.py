"""데이터 통합 및 정제 파이프라인.

실행 순서
─────────
1. S3 다운로드   → s3_downloader.py 사용
2. raw JSON 변환 → account_mapper + ratio_calculator → financial_raw.csv
3. macro 병합    → macro_quarterly.csv → financial_with_macro.csv
4. 정제          → 결측치 · 이상치 처리 → clean_data.csv

label / gics_sector 추론 방식
──────────────────────────────
기업 메타 CSV 없이 S3 경로 구조에서 직접 추론합니다.

  data/raw/{status}/{gics_sector}/{stock_code}_{year}_{quarter}.json
           ↑              ↑
    healthy → label=0    경로 그대로 gics_sector 사용
    delisted → label=1

사용 예시
─────────
# S3 다운로드 (s3_downloader.py)
python s3/s3_downloader.py download --workers 10

# 전체 실행
python preprocess.py

# JSON->CSV 변환 스킵 (financial_raw.csv 이미 있을 때)
python preprocess.py --skip-convert
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── 경로 설정 ─────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent
DATA_DIR   = ROOT / "data"
RAW_DIR    = DATA_DIR / "raw"
OUTPUT_DIR = DATA_DIR / "output"
META_DIR   = DATA_DIR / "meta"
MACRO_PATH = DATA_DIR / "macro/macro_quarterly.csv"

RAW_CSV    = OUTPUT_DIR / "financial_raw.csv"
MACRO_CSV  = OUTPUT_DIR / "financial_with_macro.csv"
CLEAN_CSV  = OUTPUT_DIR / "clean_data.csv"

sys.path.insert(0, str(ROOT / "src"))
from account_mapper import extract_standard_items
from ratio_calculator import compute_all_ratios, RATIO_NAMES

# ── 로거 ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("preprocess")

# ── 컬럼 목록 ─────────────────────────────────────────────────
META_COLS  = ["stock_code", "year", "quarter", "label", "gics_sector"]
MACRO_COLS = ["credit_spread", "kosdaq_return", "gdp_growth_yoy",
              "usdkrw_chg", "vix_avg", "cpi_yoy"]
RATIO_COLS = list(RATIO_NAMES)

# S3 경로 status → label
STATUS_TO_LABEL = {"healthy": "0", "delisted": "1"}

# ── Winsorizing 상·하한 ───────────────────────────────────────
WINSOR_LIMITS: dict[str, tuple[float, float]] = {
    "부채비율":             (0,    1000),
    "유동비율":             (0,    2000),
    "당좌비율":             (0,    2000),
    "자기자본비율":         (-200, 100),
    "차입금의존도":         (0,    200),
    "현금비율":             (0,    500),
    "비유동자산장기적합률": (0,    50),
    "순운전자본비율":       (-200, 200),
    "매출채권회전율":       (0,    200),
    "재고자산회전율":       (0,    200),
    "총자본회전율":         (0,    20),
    "유형자산회전율":       (0,    50),
    "매출원가율":           (0,    300),
    "총자산증가율":         (-100, 500),
    "유동자산증가율":       (-100, 500),
    "매출액증가율":         (-100, 1000),
    "순이익증가율":         (-500, 1000),
    "영업이익증가율":       (-500, 1000),
    "매출액순이익률":       (-500, 100),
    "매출총이익률":         (-200, 100),
    "자기자본순이익률":     (-500, 200),
    "총자본영업이익률":     (-200, 200),
    "총자본순이익률":       (-200, 200),
    "총자본투자효율":       (-10,  10),
    "유보액/납입자본비율":  (-500, 5000),
}

COL_MISSING_THRESHOLD = 0.50
ROW_MISSING_THRESHOLD = 0.70


# ══════════════════════════════════════════════════════════════
# 1. raw JSON → financial_raw.csv
# ══════════════════════════════════════════════════════════════

def _parse_filepath(jf: Path, raw_dir: Path) -> dict[str, str] | None:
    """
    파일 경로에서 모든 메타 정보를 추출.

    경로 구조: {raw_dir}/{status}/{gics_sector}/{stock_code}_{year}_{quarter}.json

    Returns:
        {stock_code, year, quarter, label, gics_sector} 또는 None (파싱 실패)
    """
    try:
        rel_parts = jf.relative_to(raw_dir).parts
        # parts[0] = status, parts[1] = gics_sector, parts[2] = 파일명
        if len(rel_parts) < 3:
            return None

        status      = rel_parts[0]   # "healthy" or "delisted"
        gics_sector = rel_parts[1]   # "Information Technology" 등

    except Exception:
        return None

    # 파일명 파싱: {stock_code}_{year}_{quarter}
    stem  = jf.stem
    parts = stem.split("_")
    if len(parts) < 3:
        return None

    label = STATUS_TO_LABEL.get(status)
    if label is None:
        log.warning(f"알 수 없는 status 폴더: {status} ({jf.name})")
        return None

    return {
        "stock_code":  parts[0],
        "year":        parts[1],
        "quarter":     parts[2],
        "label":       label,
        "gics_sector": gics_sector,
    }


def json_to_financial_raw(
    raw_dir: Path = RAW_DIR,
    output_path: Path = RAW_CSV,
) -> tuple[pd.DataFrame, int]:
    """
    data/raw/{status}/{sector}/*.json → financial_raw.csv

    label과 gics_sector는 S3 경로 구조에서 자동 추론.
    외부 메타 파일(A_companies_final.csv 등) 불필요.
    """
    json_files = sorted(raw_dir.rglob("*.json"))
    if not json_files:
        raise FileNotFoundError(
            f"JSON 파일 없음: {raw_dir}\n"
            "먼저 python -m s3.s3_downloader download 를 실행하세요."
        )
    log.info(f"JSON 파일 {len(json_files)}개 변환 시작")

    rows    = []
    skipped = 0

    for i, jf in enumerate(json_files, 1):
        meta = _parse_filepath(jf, raw_dir)
        if not meta:
            log.warning(f"경로/파일명 파싱 실패: {jf}")
            skipped += 1
            continue

        try:
            dart_items: list[dict] = json.loads(jf.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"JSON 읽기 실패 {jf.name}: {e}")
            skipped += 1
            continue

        if not dart_items:
            skipped += 1
            continue

        # ── 핵심 변환 ──────────────────────────────────────────
        std_items = extract_standard_items(dart_items)
        ratios    = compute_all_ratios(std_items)

        row = {
            "stock_code":  meta["stock_code"],
            "year":        int(meta["year"]),
            "quarter":     meta["quarter"],
            "label":       meta["label"],
            "gics_sector": meta["gics_sector"],
        }
        row.update(ratios)
        rows.append(row)

        if i % 500 == 0:
            log.info(f"  변환 진행: {i}/{len(json_files)} (스킵: {skipped})")

    if not rows:
        raise ValueError("변환된 행이 0개입니다. 경로 구조를 확인하세요.")

    df = pd.DataFrame(rows, columns=META_COLS + RATIO_COLS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    log.info(f"financial_raw.csv 저장: {output_path}")
    log.info(f"  변환 성공: {len(df)}행 / 스킵: {skipped}개")
    log.info(f"  label 분포:\n{df['label'].value_counts().to_string()}")
    log.info(f"  섹터 분포:\n{df['gics_sector'].value_counts().to_string()}")

    null_counts = df[RATIO_COLS].isna().sum()
    high_null   = null_counts[null_counts > len(df) * 0.3]
    if not high_null.empty:
        log.warning(
            f"결측률 30% 초과 비율 컬럼 ({len(high_null)}개):\n"
            f"{high_null.to_string()}"
        )

    return df, skipped


# ══════════════════════════════════════════════════════════════
# 2. macro 병합
# ══════════════════════════════════════════════════════════════

def merge_macro(
    df: pd.DataFrame,
    macro_path: Path = MACRO_PATH,
    output_path: Path = MACRO_CSV,
) -> pd.DataFrame:
    """year + quarter 키로 거시경제 데이터 left join."""
    if not macro_path.exists():
        raise FileNotFoundError(f"macro 파일 없음: {macro_path}")

    df_macro         = pd.read_csv(macro_path)
    df_macro["year"] = df_macro["year"].astype(int)
    df               = df.merge(df_macro, on=["year", "quarter"], how="left")

    unmatched = df[MACRO_COLS].isna().all(axis=1).sum()
    if unmatched:
        log.warning(f"거시경제 데이터 미매칭 행: {unmatched}개 (macro 범위: 2015~2025)")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    log.info(f"financial_with_macro.csv 저장: {output_path} ({len(df)}행)")
    return df


# ══════════════════════════════════════════════════════════════
# 3. 정제
# ══════════════════════════════════════════════════════════════

def _analyze_missing(df: pd.DataFrame) -> None:
    valid  = [c for c in RATIO_COLS if c in df.columns]
    miss   = (df[valid].isna().sum() / len(df)).sort_values(ascending=False)
    report = pd.DataFrame({"결측률": miss.round(3), "결측수": df[valid].isna().sum()})
    log.info(f"\n[결측률 리포트 (상위 15개)]\n{report.head(15).to_string()}")


def _drop_high_missing_cols(
    df: pd.DataFrame, threshold: float
) -> tuple[pd.DataFrame, list[str]]:
    valid     = [c for c in RATIO_COLS if c in df.columns]
    miss_rate = df[valid].isna().mean()
    drop_cols = miss_rate[miss_rate > threshold].index.tolist()
    if drop_cols:
        log.info(f"컬럼 제거 (결측률 >{threshold:.0%}): {drop_cols}")
        df = df.drop(columns=drop_cols)
    return df, drop_cols


def _drop_high_missing_rows(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    valid    = [c for c in RATIO_COLS if c in df.columns]
    row_miss = df[valid].isna().mean(axis=1)
    before   = len(df)
    df       = df[row_miss <= threshold].copy()
    log.info(f"행 제거 (결측률 >{threshold:.0%}): {before - len(df)}행 제거, {len(df)}행 남음")
    return df


def _impute(df: pd.DataFrame) -> pd.DataFrame:
    """
    결측치 대체 (순서대로):
    1. ffill      — 같은 기업 내 이전 분기 값
    2. CF=0       — 감가상각비 계열은 없으면 0
    3. 섹터중앙값 — 같은 섹터·분기 내 중앙값
    4. 전체중앙값 — fallback
    """
    valid = [c for c in RATIO_COLS if c in df.columns]

    quarter_order = {"Q1": 1, "H1": 2, "Q3": 3, "ANNUAL": 4}
    df["_q_order"] = df["quarter"].map(quarter_order)
    df = df.sort_values(["stock_code", "year", "_q_order"]).drop(columns=["_q_order"])

    df[valid] = df.groupby("stock_code")[valid].transform(lambda x: x.ffill())

    cf_cols = [c for c in ["감가상각비", "유형자산상각비", "무형자산상각비"] if c in df.columns]
    df[cf_cols] = df[cf_cols].fillna(0)

    remaining = [c for c in valid if c not in cf_cols]
    df[remaining] = df.groupby(["gics_sector", "quarter"])[remaining].transform(
        lambda x: x.fillna(x.median())
    )
    df[remaining] = df[remaining].fillna(df[remaining].median())

    log.info("결측치 대체 완료 (ffill → CF=0 → 섹터중앙값 → 전체중앙값)")
    return df


def _winsorize(df: pd.DataFrame) -> pd.DataFrame:
    valid         = [c for c in RATIO_COLS if c in df.columns]
    total_clipped = 0

    for col in valid:
        if col in WINSOR_LIMITS:
            lo, hi = WINSOR_LIMITS[col]
        else:
            q1     = df[col].quantile(0.25)
            q3     = df[col].quantile(0.75)
            iqr    = q3 - q1
            lo, hi = q1 - 3 * iqr, q3 + 3 * iqr

        clipped        = df[col].clip(lower=lo, upper=hi)
        total_clipped += (df[col] != clipped).sum()
        df[col]        = clipped

    log.info(f"Winsorizing 완료 (총 {total_clipped}개 값 클리핑)")
    return df


def clean(
    df: pd.DataFrame,
    output_path: Path = CLEAN_CSV,
) -> pd.DataFrame:
    rows_before = len(df)
    
    log.info(f"\n{'='*50}\n정제 시작: {len(df)}행 × {len(df.columns)}컬럼\n{'='*50}")

    _analyze_missing(df)
    df, dropped_cols = _drop_high_missing_cols(df, COL_MISSING_THRESHOLD)
    df = _drop_high_missing_rows(df, ROW_MISSING_THRESHOLD)
    df = _impute(df)
    df = _winsorize(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    valid = [c for c in RATIO_COLS if c in df.columns]
    log.info(f"\n[정제 완료]")
    log.info(f"  최종 행 수:        {len(df)}")
    log.info(f"  최종 컬럼 수:      {len(df.columns)}")
    log.info(f"  제거된 비율 컬럼:  {dropped_cols if dropped_cols else '없음'}")
    log.info(f"  잔여 결측값:       {df[valid].isna().sum().sum()}개")
    log.info(f"  clean_data.csv:    {output_path}")

    _save_clean_report(
        df=df,
        dropped_cols=dropped_cols,
        rows_before=rows_before,
        output_path=META_DIR / "clean_report.json",
    )

    return df

def _save_clean_report(
    df: pd.DataFrame,
    dropped_cols: list[str],
    rows_before: int,
    output_path: Path,
) -> None:
    valid = [c for c in RATIO_COLS if c in df.columns]
    report = {
        "cleaned_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows_before":      rows_before,
        "rows_after":       len(df),
        "rows_dropped":     rows_before - len(df),
        "cols_before":      rows_before,  
        "cols_after":       len(df.columns),
        "dropped_cols":     dropped_cols,
        "remaining_nulls":  int(df[valid].isna().sum().sum()),
        "col_missing_threshold": COL_MISSING_THRESHOLD,
        "row_missing_threshold": ROW_MISSING_THRESHOLD,
        "label_distribution":   df["label"].value_counts().to_dict(),
        "sector_distribution":  df["gics_sector"].value_counts().to_dict(),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info(f"정제 리포트 저장: {output_path}")

# 메타데이터 생성
def _save_conversion_meta(
    df: pd.DataFrame,
    skipped: int,
    unmatched_macro: int,
    output_path: Path,
) -> None:
    valid = [c for c in RATIO_COLS if c in df.columns]
    null_counts = df[valid].isna().sum()
    high_null = null_counts[null_counts > len(df) * 0.3]

    meta = {
        "converted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_rows": len(df),
        "skipped": skipped,
        "label_distribution": df["label"].value_counts().to_dict(),
        "sector_distribution": df["gics_sector"].value_counts().to_dict(),
        "high_missing_columns": {
            "threshold": 0.3,
            "columns": high_null.to_dict(),
        },
        "macro_unmatched_rows": unmatched_macro,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info(f"변환 메타데이터 저장: {output_path}")

# ══════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="데이터 통합 및 정제 파이프라인")
    parser.add_argument("--skip-convert", action="store_true")
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    skipped = 0

    # 1) raw JSON → financial_raw.csv
    if not args.skip_convert:
        df, skipped = json_to_financial_raw(raw_dir=raw_dir, output_path=RAW_CSV)
    else:
        log.info(f"financial_raw.csv 로드: {RAW_CSV}")
        df = pd.read_csv(RAW_CSV, dtype={"stock_code": str, "label": str})

    # 2) macro 병합
    df = merge_macro(df, macro_path=MACRO_PATH, output_path=MACRO_CSV)

    # 3) 메타데이터 저장
    unmatched_macro = int(df[MACRO_COLS].isna().all(axis=1).sum())
    _save_conversion_meta(
        df=df,
        skipped=skipped,
        unmatched_macro=unmatched_macro,
        output_path=DATA_DIR / "meta" / "conversion_meta.json",
    )

    # 4) 정제
    clean(df, output_path=CLEAN_CSV)

if __name__ == "__main__":
    main()