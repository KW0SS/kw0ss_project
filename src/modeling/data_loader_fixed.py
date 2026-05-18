"""fixed_N{n} 전처리 데이터셋 로더.

preprocess/data/processed/fixed_N{n}/{variant}/ 의 train/valid/test.csv 를 로드한다.
H-horizon 로더(data_loader.py)와 컬럼/임퓨트 정책은 공유하지만, 경로 규칙과
라벨 의미(backward-looking N years vs forward-looking H months)가 다르다.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sklearn.impute import SimpleImputer

from src.modeling.data_loader import (
    HIGH_MISSING_COLUMNS,
    MACRO_COLUMNS,
    META_COLUMNS,
    RAW_VALUE_COLUMNS,
    TARGET_COLUMN,
    get_feature_columns,
    prepare_xy,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "preprocess" / "data" / "processed"
DEFAULT_VARIANT = "baseline"
COLLINEAR_DROP_COLUMNS = {"유동비율", "유형자산상각비"}

AVAILABLE_N_YEARS = sorted(
    int(p.name[len("fixed_N"):])
    for p in DATA_DIR.iterdir()
    if p.is_dir() and p.name.startswith("fixed_N") and p.name[len("fixed_N"):].isdigit()
)


def _resolve_fixed_dir(n_years: int, variant: str, data_dir: Path | str | None) -> Path:
    """fixed_N{n}/{variant}/ 경로를 반환한다."""
    base = Path(data_dir) if data_dir else DATA_DIR
    n_dir = base / f"fixed_N{n_years}"
    variant_dir = n_dir / variant
    if variant_dir.exists():
        return variant_dir
    raise FileNotFoundError(f"fixed_N 디렉터리 없음: {variant_dir}")


def load_fixed_n(
    n_years: int,
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict[str, pd.DataFrame]:
    """fixed_N{n}/{variant} 의 train/valid/test CSV 를 로드한다."""
    n_dir = _resolve_fixed_dir(n_years, variant, data_dir)
    splits = {}
    for split_name in ("train", "valid", "test"):
        csv_path = n_dir / f"{split_name}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"파일 없음: {csv_path}")
        splits[split_name] = pd.read_csv(csv_path, dtype={"stock_code": str})
    return splits


def load_meta(
    n_years: int,
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict:
    n_dir = _resolve_fixed_dir(n_years, variant, data_dir)
    with open(n_dir / "meta.json", encoding="utf-8") as f:
        return json.load(f)


def load_and_prepare_fixed(
    n_years: int,
    include_macro: bool = True,
    include_raw_value: bool = True,
    drop_high_missing: bool = True,
    impute_strategy: str = "median",
    data_dir: Path | str | None = None,
    variant: str = DEFAULT_VARIANT,
) -> dict:
    """fixed_N{n}/{variant} 데이터를 로드하고 X/y 분리 + impute 까지 수행."""
    splits = load_fixed_n(n_years, data_dir, variant=variant)
    meta = load_meta(n_years, data_dir, variant=variant)

    feature_cols = get_feature_columns(
        splits["train"],
        include_macro=include_macro,
        include_raw_value=include_raw_value,
        drop_high_missing=drop_high_missing,
    )
    excluded_collinear = [c for c in feature_cols if c in COLLINEAR_DROP_COLUMNS]
    feature_cols = [c for c in feature_cols if c not in COLLINEAR_DROP_COLUMNS]

    X_train, y_train, imputer = prepare_xy(splits["train"], feature_cols, impute_strategy)
    X_valid, y_valid, _ = prepare_xy(splits["valid"], feature_cols, imputer=imputer)
    X_test, y_test, _ = prepare_xy(splits["test"], feature_cols, imputer=imputer)

    return {
        "X_train": X_train, "y_train": y_train,
        "X_valid": X_valid, "y_valid": y_valid,
        "X_test": X_test, "y_test": y_test,
        "feature_cols": feature_cols,
        "excluded_collinear": excluded_collinear,
        "imputer": imputer,
        "meta": meta,
    }


def print_data_summary_fixed(n_years: int, variant: str, data: dict) -> None:
    meta = data["meta"]
    print(f"=== fixed_N{n_years} / {variant} Dataset Summary ===")
    print(f"  Features: {len(data['feature_cols'])}개")
    print(f"  Excluded collinear: {data.get('excluded_collinear', [])}")
    print(f"  Train: {len(data['X_train']):,}행  (pos={int(data['y_train'].sum())})")
    print(f"  Valid: {len(data['X_valid']):,}행  (pos={int(data['y_valid'].sum())})")
    print(f"  Test:  {len(data['X_test']):,}행  (pos={int(data['y_test'].sum())})")
    print(f"  Imbalance ratio: {meta.get('imbalance_ratio', 'N/A')}")
    print(f"  Label rule: backward-looking, 상폐연도 - {n_years}년")
    print(f"  Train years: {meta.get('train_years', [])}")
    print(f"  Valid years: {meta.get('valid_years', [])}")
    print(f"  Test  years: {meta.get('test_years', [])}")

    remaining_nan = data["X_train"].isnull().sum().sum()
    print(f"  Remaining NaN after impute: {remaining_nan}")
    print()
