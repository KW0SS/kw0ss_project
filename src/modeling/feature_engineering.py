"""fixed_N 데이터셋용 피처 엔지니어링 변환.

각 변환은 (meta + features + label) 형태의 split DataFrame을 입력받아
변환된 DataFrame을 반환한다. 모두 행 단위(stateless) 변환이므로
train/valid/test에 동일 함수를 그대로 적용하면 되고 누수가 없다.
(impute median fit은 기존대로 data_loader_fixed의 prepare_xy에서 train만 사용)

지원 모드
---------
- drop_collinear     : EDA 다중공선성 후보 2개(`유동비율`, `유형자산상각비`) 제거
- ratio_total_assets : 절대값 KRW 항목 5개를 총자산 대비 비율로 치환
- signed_log1p       : 모든 피처에 sign(x)*log1p(|x|) 적용

총자산 컬럼 부재 처리
---------------------
processed fixed_N CSV에는 총자산(자산총계) 컬럼이 없다. 다만 동일 기간의
두 회전율이 매출액을 공유하므로 총자산을 정확히 복원할 수 있다.

    유형자산회전율 = 매출액 / 유형자산
    총자본회전율   = 매출액 / 총자산
    => 총자산 = 유형자산 * 유형자산회전율 / 총자본회전율

이 항등식은 raw-scale variant(baseline, exp-A)에서만 성립한다.
robust_scale variant(exp-B, exp-C)는 컬럼별 RobustScaler가 곱셈 항등식을
깨므로 ratio_total_assets 적용 대상에서 제외한다(run_all_fixed에서 스킵).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.modeling.data_loader import META_COLUMNS, TARGET_COLUMN

# 다중공선성 제거 후보 (exp_005와 동일 기준)
COLLINEAR_DROP_COLUMNS = {"유동비율", "유형자산상각비"}

# 절대값(KRW) 항목 -> 총자산 대비 비율 컬럼명
RATIO_SOURCE_TO_TARGET = {
    "유형자산": "유형자산_총자산비율",
    "무형자산": "무형자산_총자산비율",
    "무형자산상각비": "무형자산상각비_총자산비율",
    "유형자산상각비": "유형자산상각비_총자산비율",
    "감가상각비": "감가상각비_총자산비율",
}

# robust_scale variant는 총자산 복원 불가 → ratio FE 대상에서 제외
RATIO_INCOMPATIBLE_VARIANTS = {"exp-B", "exp-C"}

FE_MODES = ("drop_collinear", "ratio_total_assets", "signed_log1p")


def _reconstruct_total_assets(df: pd.DataFrame) -> pd.Series:
    """총자산 = 유형자산 * 유형자산회전율 / 총자본회전율.

    복원 불가능한 행(분모 0/음수, 유형자산 비양수, 비유한)은 NaN.
    후속 impute(median, train fit)에서 채워진다.
    """
    required = ("유형자산", "유형자산회전율", "총자본회전율")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"총자산 복원에 필요한 컬럼 없음: {missing}")

    ta = df["유형자산"] * df["유형자산회전율"] / df["총자본회전율"]
    valid = (
        np.isfinite(ta)
        & (ta > 0)
        & (df["총자본회전율"] > 0)
        & (df["유형자산"] > 0)
    )
    return ta.where(valid)


def apply_drop_collinear(df: pd.DataFrame) -> pd.DataFrame:
    """다중공선성 후보 2개 컬럼을 제거한다."""
    return df.drop(
        columns=[c for c in COLLINEAR_DROP_COLUMNS if c in df.columns],
        errors="ignore",
    )


def apply_ratio_total_assets(df: pd.DataFrame) -> pd.DataFrame:
    """절대값 항목 5개를 총자산 대비 비율로 치환한다.

    원본 절대값 컬럼은 제거하고 `*_총자산비율` 컬럼을 추가한다.
    """
    df = df.copy()
    total_assets = _reconstruct_total_assets(df)
    for src, target in RATIO_SOURCE_TO_TARGET.items():
        if src in df.columns:
            df[target] = df[src] / total_assets
    df = df.drop(
        columns=[c for c in RATIO_SOURCE_TO_TARGET if c in df.columns],
        errors="ignore",
    )
    return df


def apply_signed_log1p(df: pd.DataFrame) -> pd.DataFrame:
    """모든 피처 컬럼에 sign(x)*log1p(|x|)를 적용한다.

    meta/label 컬럼은 변환하지 않는다. RobustScaler가 적용된 variant라도
    부호 보존 단조 변환이므로 유효하다.
    """
    df = df.copy()
    skip = META_COLUMNS | {TARGET_COLUMN}
    feature_cols = [
        c for c in df.columns
        if c not in skip and pd.api.types.is_numeric_dtype(df[c])
    ]
    for c in feature_cols:
        col = df[c]
        df[c] = np.sign(col) * np.log1p(col.abs())
    return df


_DISPATCH = {
    "drop_collinear": apply_drop_collinear,
    "ratio_total_assets": apply_ratio_total_assets,
    "signed_log1p": apply_signed_log1p,
}


def apply_feature_engineering(df: pd.DataFrame, fe_mode: str) -> pd.DataFrame:
    """fe_mode에 해당하는 변환을 split DataFrame에 적용한다."""
    if fe_mode not in _DISPATCH:
        raise ValueError(
            f"알 수 없는 fe_mode: {fe_mode!r} (지원: {sorted(_DISPATCH)})"
        )
    return _DISPATCH[fe_mode](df)
