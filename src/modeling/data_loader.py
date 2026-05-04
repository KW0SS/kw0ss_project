"""
H-horizon별 전처리된 데이터셋 로더.

preprocess/data/processed/H{n}/ 의 train/valid/test.csv를 로드하고
모델 학습에 필요한 X/y 분리, 피처 선택, 결측 처리를 수행한다.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer

# ---------------------------------------------------------------------------
# 컬럼 분류 상수
# ---------------------------------------------------------------------------

META_COLUMNS = {"stock_code", "year", "quarter", "gics_sector"}
TARGET_COLUMN = "label"

RAW_VALUE_COLUMNS = {"유형자산", "무형자산", "무형자산상각비", "유형자산상각비", "감가상각비"}

MACRO_COLUMNS = {
    "credit_spread", "kosdaq_return", "gdp_growth_yoy",
    "usdkrw_chg", "vix_avg", "cpi_yoy",
}

# YoY 증가율 컬럼 — 결측률이 ~66%로 높아 기본적으로 제외
HIGH_MISSING_COLUMNS = {"매출액증가율", "순이익증가율", "영업이익증가율"}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "preprocess" / "data" / "processed"
DEFAULT_VARIANT = "baseline"

AVAILABLE_HORIZONS = sorted(
    [int(p.name[1:]) for p in DATA_DIR.iterdir() if p.is_dir() and p.name.startswith("H")],
)


def _resolve_horizon_dir(horizon: int, variant: str, data_dir: Path | str | None) -> Path:
    """H{horizon}/{variant}/ 또는 (구조 호환) H{horizon}/ 경로를 반환."""
    base = Path(data_dir) if data_dir else DATA_DIR
    h_dir = base / f"H{horizon}"
    variant_dir = h_dir / variant
    if variant_dir.exists():
        return variant_dir
    if (h_dir / "train.csv").exists():
        return h_dir
    raise FileNotFoundError(f"Horizon 디렉터리 없음: {variant_dir} (또는 {h_dir})")


# ---------------------------------------------------------------------------
# 데이터 로드
# ---------------------------------------------------------------------------


def load_horizon(
    horizon: int,
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict[str, pd.DataFrame]:
    """H{horizon}/{variant} 디렉터리에서 train/valid/test CSV를 로드한다.

    Returns:
        {"train": df_train, "valid": df_valid, "test": df_test}
    """
    h_dir = _resolve_horizon_dir(horizon, variant, data_dir)

    splits = {}
    for split_name in ("train", "valid", "test"):
        csv_path = h_dir / f"{split_name}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"파일 없음: {csv_path}")
        df = pd.read_csv(csv_path, dtype={"stock_code": str})
        splits[split_name] = df

    return splits


def load_meta(
    horizon: int,
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict:
    """H{horizon}/{variant}/meta.json을 로드한다."""
    h_dir = _resolve_horizon_dir(horizon, variant, data_dir)
    with open(h_dir / "meta.json", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 피처 선택 및 X/y 분리
# ---------------------------------------------------------------------------


def get_feature_columns(
    df: pd.DataFrame,
    include_macro: bool = True,
    include_raw_value: bool = True,
    drop_high_missing: bool = True,
) -> list[str]:
    """모델 학습에 사용할 수치형 피처 컬럼 목록을 반환한다.

    Args:
        include_macro: 매크로 변수 포함 여부
        include_raw_value: 원시값(유형자산 등) 포함 여부
        drop_high_missing: 결측률 높은 YoY 증가율 컬럼 제외 여부
    """
    exclude = META_COLUMNS | {TARGET_COLUMN}

    if not include_macro:
        exclude |= MACRO_COLUMNS
    if not include_raw_value:
        exclude |= RAW_VALUE_COLUMNS
    if drop_high_missing:
        exclude |= HIGH_MISSING_COLUMNS

    return [c for c in df.columns if c not in exclude]


def prepare_xy(
    df: pd.DataFrame,
    feature_cols: list[str] | None = None,
    impute_strategy: str = "median",
    imputer: SimpleImputer | None = None,
) -> tuple[pd.DataFrame, pd.Series, SimpleImputer]:
    """DataFrame에서 X, y를 분리하고 결측을 impute한다.

    Args:
        df: 원본 DataFrame (meta + features + label)
        feature_cols: 사용할 피처 컬럼. None이면 자동 선택
        impute_strategy: SimpleImputer 전략 ("median", "mean" 등)
        imputer: 이미 fit된 imputer. None이면 새로 fit

    Returns:
        (X, y, fitted_imputer)
    """
    if feature_cols is None:
        feature_cols = get_feature_columns(df)

    X = df[feature_cols].copy()
    y = df[TARGET_COLUMN].copy()

    if imputer is None:
        imputer = SimpleImputer(strategy=impute_strategy)
        X_imputed = imputer.fit_transform(X)
    else:
        X_imputed = imputer.transform(X)

    X = pd.DataFrame(X_imputed, columns=feature_cols, index=df.index)

    return X, y, imputer


# ---------------------------------------------------------------------------
# 편의 함수: 한 번에 로드 + 분리
# ---------------------------------------------------------------------------


def load_and_prepare(
    horizon: int,
    include_macro: bool = True,
    include_raw_value: bool = True,
    drop_high_missing: bool = True,
    impute_strategy: str = "median",
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict:
    """H{horizon}/{variant} 데이터를 로드하고 X/y 분리까지 수행한다.

    train으로 fit한 imputer를 valid/test에 동일 적용한다.

    Returns:
        {
            "X_train", "y_train",
            "X_valid", "y_valid",
            "X_test",  "y_test",
            "feature_cols": list[str],
            "imputer": SimpleImputer,
            "meta": dict,
        }
    """
    splits = load_horizon(horizon, data_dir, variant=variant)
    meta = load_meta(horizon, data_dir, variant=variant)

    feature_cols = get_feature_columns(
        splits["train"],
        include_macro=include_macro,
        include_raw_value=include_raw_value,
        drop_high_missing=drop_high_missing,
    )

    X_train, y_train, imputer = prepare_xy(
        splits["train"], feature_cols, impute_strategy,
    )
    X_valid, y_valid, _ = prepare_xy(
        splits["valid"], feature_cols, imputer=imputer,
    )
    X_test, y_test, _ = prepare_xy(
        splits["test"], feature_cols, imputer=imputer,
    )

    return {
        "X_train": X_train, "y_train": y_train,
        "X_valid": X_valid, "y_valid": y_valid,
        "X_test": X_test, "y_test": y_test,
        "feature_cols": feature_cols,
        "imputer": imputer,
        "meta": meta,
    }


# ---------------------------------------------------------------------------
# 진단 출력
# ---------------------------------------------------------------------------


def print_data_summary(horizon: int, data: dict) -> None:
    """로드된 데이터의 요약 정보를 출력한다."""
    meta = data["meta"]
    print(f"=== H{horizon} Dataset Summary ===")
    print(f"  Features: {len(data['feature_cols'])}개")
    print(f"  Train: {len(data['X_train']):,}행  (pos={int(data['y_train'].sum())})")
    print(f"  Valid: {len(data['X_valid']):,}행  (pos={int(data['y_valid'].sum())})")
    print(f"  Test:  {len(data['X_test']):,}행  (pos={int(data['y_test'].sum())})")
    print(f"  Imbalance ratio: {meta.get('imbalance_ratio', 'N/A')}")
    print(f"  Train years: {meta.get('train_years', [])}")
    print(f"  Valid years: {meta.get('valid_years', [])}")
    print(f"  Test  years: {meta.get('test_years', [])}")

    remaining_nan = data["X_train"].isnull().sum().sum()
    print(f"  Remaining NaN after impute: {remaining_nan}")
    print()
