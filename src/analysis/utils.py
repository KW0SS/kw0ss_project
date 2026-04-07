"""
재사용 분석 유틸리티 함수 모듈.

Phase 2: EDA 노트북과 이후 분석에서 반복 사용할 함수들.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# ---------------------------------------------------------------------------
# 2-1. 데이터 로드 및 컬럼 분류
# ---------------------------------------------------------------------------

META_COLUMNS = {"stock_code", "corp_name", "year", "quarter", "label", "gics_sector"}

RAW_VALUE_COLUMNS = {"유형자산", "무형자산"}

MACRO_COLUMNS = {
    "credit_spread", "kosdaq_return", "gdp_growth_yoy",
    "usdkrw_chg", "vix_avg", "cpi_yoy",
}

_META_DTYPES = {
    "stock_code": str,
    "corp_name": str,
    "year": int,
    "quarter": str,
    "label": int,
    "gics_sector": str,
}


def load_csv(path: str | Path) -> pd.DataFrame:
    """재무비율 CSV를 로드하고 dtype을 교정한다.

    - stock_code: zero-padded 6자리 str 유지
    - year, label: int
    - 재무비율 컬럼: float (빈 문자열 → NaN)
    """
    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
        dtype={"stock_code": str},
    )
    for col, dtype in _META_DTYPES.items():
        if col in df.columns:
            if dtype == int:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == str:
                df[col] = df[col].astype(str)

    ratio_cols = [c for c in df.columns if c not in META_COLUMNS]
    for col in ratio_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def classify_columns(df: pd.DataFrame) -> dict[str, list[str]]:
    """DataFrame 컬럼을 meta / ratio / raw_value / macro로 분류한다.

    Returns:
        {"meta": [...], "ratio": [...], "raw_value": [...], "macro": [...]}
    """
    meta = [c for c in df.columns if c in META_COLUMNS]
    raw_value = [c for c in df.columns if c in RAW_VALUE_COLUMNS]
    macro = [c for c in df.columns if c in MACRO_COLUMNS]
    non_ratio = META_COLUMNS | RAW_VALUE_COLUMNS | MACRO_COLUMNS
    ratio = [c for c in df.columns if c not in non_ratio]
    return {"meta": meta, "ratio": ratio, "raw_value": raw_value, "macro": macro}


def summarize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame 전체 요약 통계를 반환한다.

    Returns:
        컬럼별 dtype, non-null 수, 결측률, unique 수, 샘플값을 담은 DataFrame
    """
    summary = pd.DataFrame({
        "dtype": df.dtypes,
        "non_null": df.count(),
        "null_count": df.isnull().sum(),
        "null_pct": (df.isnull().sum() / len(df) * 100).round(2),
        "unique": df.nunique(),
        "sample": df.iloc[0] if len(df) > 0 else None,
    })
    return summary


# ---------------------------------------------------------------------------
# 2-2. 결측 분석
# ---------------------------------------------------------------------------


def missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼별 결측 수, 결측률, dtype을 반환한다."""
    return pd.DataFrame({
        "dtype": df.dtypes,
        "null_count": df.isnull().sum(),
        "null_pct": (df.isnull().sum() / len(df) * 100).round(2),
    }).sort_values("null_pct", ascending=False)


def high_missing_columns(df: pd.DataFrame, threshold: float = 0.5) -> list[str]:
    """결측률이 threshold 이상인 컬럼 목록을 반환한다."""
    pct = df.isnull().sum() / len(df)
    return pct[pct >= threshold].sort_values(ascending=False).index.tolist()


def plot_missing_heatmap(df: pd.DataFrame, figsize: tuple = (14, 6)) -> None:
    """결측 패턴 히트맵을 그린다. (흰색=결측, 검정=존재)"""
    cols = classify_columns(df)
    target_cols = cols["ratio"] + cols["raw_value"]
    subset = df[target_cols] if target_cols else df

    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        subset.isnull().T,
        cbar=False,
        yticklabels=True,
        cmap=["#2d2d2d", "#e74c3c"],
        ax=ax,
    )
    ax.set_title("결측 패턴 (빨간색 = 결측)")
    ax.set_xlabel("행 인덱스")
    plt.tight_layout()
    plt.show()


# ---------------------------------------------------------------------------
# 2-3. 단변량 분석
# ---------------------------------------------------------------------------


def plot_histogram(
    df: pd.DataFrame,
    feature: str,
    bins: int = 50,
    figsize: tuple = (8, 4),
) -> None:
    """단일 피처의 히스토그램을 그린다."""
    fig, ax = plt.subplots(figsize=figsize)
    df[feature].dropna().hist(bins=bins, ax=ax, edgecolor="white", alpha=0.8)
    ax.set_title(f"{feature} 분포")
    ax.set_xlabel(feature)
    ax.set_ylabel("빈도")
    plt.tight_layout()
    plt.show()


def plot_boxplot_by_label(
    df: pd.DataFrame,
    feature: str,
    figsize: tuple = (8, 4),
) -> None:
    """label(0/1)별 boxplot을 그린다."""
    fig, ax = plt.subplots(figsize=figsize)
    sns.boxplot(data=df, x="label", y=feature, ax=ax)
    ax.set_title(f"{feature} — label별 비교")
    ax.set_xticklabels(["healthy (0)", "delisted (1)"])
    plt.tight_layout()
    plt.show()


def analyze_single_feature(df: pd.DataFrame, feature: str) -> dict:
    """단일 피처의 요약 통계를 딕셔너리로 반환한다."""
    s = df[feature].dropna()
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    return {
        "count": int(s.count()),
        "mean": s.mean(),
        "median": s.median(),
        "std": s.std(),
        "min": s.min(),
        "max": s.max(),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "skewness": s.skew(),
        "kurtosis": s.kurtosis(),
    }


def compare_group_stats_by_label(df: pd.DataFrame, feature: str) -> pd.DataFrame:
    """label=0 vs label=1 그룹 비교 통계를 반환한다."""
    grouped = df.groupby("label")[feature]
    stats = grouped.agg(["count", "mean", "median", "std", "min", "max"])
    stats["null_pct"] = df.groupby("label")[feature].apply(
        lambda x: x.isnull().sum() / len(x) * 100
    )
    return stats


# ---------------------------------------------------------------------------
# 2-4. 상관관계
# ---------------------------------------------------------------------------


def plot_correlation_heatmap(
    df: pd.DataFrame,
    method: str = "pearson",
    figsize: tuple = (14, 12),
) -> None:
    """수치형 컬럼 간 상관계수 히트맵을 그린다."""
    cols = classify_columns(df)
    numeric_cols = cols["ratio"]
    corr = df[numeric_cols].corr(method=method)

    mask = np.triu(np.ones_like(corr, dtype=bool))
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        corr,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        center=0,
        vmin=-1,
        vmax=1,
        ax=ax,
        annot_kws={"size": 7},
    )
    ax.set_title(f"상관계수 히트맵 ({method})")
    plt.tight_layout()
    plt.show()


def get_high_corr_pairs(
    df: pd.DataFrame,
    threshold: float = 0.9,
    method: str = "pearson",
) -> list[tuple[str, str, float]]:
    """절대 상관계수가 threshold 이상인 (col_a, col_b, corr) 쌍을 반환한다."""
    cols = classify_columns(df)
    corr = df[cols["ratio"]].corr(method=method)
    pairs = []
    seen = set()
    for i, col_a in enumerate(corr.columns):
        for j, col_b in enumerate(corr.columns):
            if i >= j:
                continue
            val = corr.iloc[i, j]
            if abs(val) >= threshold and (col_a, col_b) not in seen:
                pairs.append((col_a, col_b, round(val, 4)))
                seen.add((col_a, col_b))
    return sorted(pairs, key=lambda x: abs(x[2]), reverse=True)


# ---------------------------------------------------------------------------
# 2-5. 이상치 탐지
# ---------------------------------------------------------------------------


def detect_outliers_iqr(
    df: pd.DataFrame,
    feature: str,
    factor: float = 1.5,
) -> pd.Series:
    """IQR 기반 이상치 탐지. True = 이상치인 행."""
    s = df[feature]
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return (s < lower) | (s > upper)
