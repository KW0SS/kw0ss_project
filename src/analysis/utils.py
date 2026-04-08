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


# ---------------------------------------------------------------------------
# 2-6. 이상치 판정표
# ---------------------------------------------------------------------------


def _skew_signal(mean: float, median: float, iqr: float) -> str:
    """abs(mean - median) / iqr 기반 왜도 신호를 분류한다."""
    if iqr == 0:
        return "iqr_zero"
    ratio = abs(mean - median) / iqr
    if ratio < 0.3:
        return "low"
    if ratio < 0.7:
        return "moderate"
    return "high"


def _distribution_note(mean: float, median: float, iqr: float) -> str:
    """mean vs median 비교로 분포 방향성을 판단한다."""
    if iqr == 0:
        return "iqr is zero"
    diff = mean - median
    threshold = iqr * 0.05
    if abs(diff) < threshold:
        return "symmetric or near-symmetric"
    return "right-tailed" if diff > 0 else "left-tailed"


def build_outlier_decision_table(
    df: pd.DataFrame,
    numeric_cols: list[str] | None = None,
) -> pd.DataFrame:
    """수치형 컬럼별 IQR 기반 이상치 판정표를 생성한다.

    Args:
        df: 분석 대상 DataFrame
        numeric_cols: 분석할 컬럼 목록. None이면 숫자형 컬럼 전체 자동 선택

    Returns:
        컬럼별 IQR 경계, 이상치 개수/비율, 해석용 컬럼이 포함된 DataFrame
    """
    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        numeric_cols = [c for c in numeric_cols if c not in META_COLUMNS]

    if not numeric_cols:
        return pd.DataFrame()

    rows = []
    for col in numeric_cols:
        s = df[col].dropna()
        cnt = len(s)

        if cnt == 0:
            rows.append({"column": col, "count": 0})
            continue

        q1 = s.quantile(0.25)
        med = s.quantile(0.50)
        q3 = s.quantile(0.75)
        iqr = q3 - q1

        lower_1_5 = q1 - 1.5 * iqr
        upper_1_5 = q3 + 1.5 * iqr
        lower_3_0 = q1 - 3.0 * iqr
        upper_3_0 = q3 + 3.0 * iqr

        outlier_mask = (s < lower_1_5) | (s > upper_1_5)
        extreme_mask = (s < lower_3_0) | (s > upper_3_0)
        outlier_cnt = int(outlier_mask.sum())
        extreme_cnt = int(extreme_mask.sum())

        mean_val = s.mean()
        std_val = s.std()

        rows.append({
            "column": col,
            "count": cnt,
            "min": round(s.min(), 4),
            "q1": round(q1, 4),
            "median": round(med, 4),
            "q3": round(q3, 4),
            "max": round(s.max(), 4),
            "mean": round(mean_val, 4),
            "std": round(std_val, 4),
            "iqr": round(iqr, 4),
            "lower_1_5": round(lower_1_5, 4),
            "upper_1_5": round(upper_1_5, 4),
            "lower_3_0": round(lower_3_0, 4),
            "upper_3_0": round(upper_3_0, 4),
            "outlier_cnt_1_5": outlier_cnt,
            "outlier_ratio_1_5": round(outlier_cnt / cnt, 4) if cnt > 0 else 0.0,
            "extreme_cnt_3_0": extreme_cnt,
            "extreme_ratio_3_0": round(extreme_cnt / cnt, 4) if cnt > 0 else 0.0,
            "skew_signal": _skew_signal(mean_val, med, iqr),
            "distribution_note": _distribution_note(mean_val, med, iqr),
        })

    result = pd.DataFrame(rows)
    if "outlier_ratio_1_5" in result.columns:
        result = result.sort_values("outlier_ratio_1_5", ascending=False)
    return result.reset_index(drop=True)


def render_outlier_html(
    table: pd.DataFrame,
    top_n: int = 10,
) -> str:
    """이상치 판정표를 HTML 문자열로 렌더링한다.

    Args:
        table: build_outlier_decision_table() 결과
        top_n: 요약 문장에 표시할 상위 컬럼 수

    Returns:
        HTML 문자열 (Jupyter display(HTML(...))용)
    """
    if table.empty:
        return "<p>분석 가능한 수치형 컬럼이 없습니다.</p>"

    # --- 요약 문장 ---
    top_outlier = table.nlargest(top_n, "outlier_ratio_1_5")
    outlier_items = [
        f"{row['column']}({row['outlier_ratio_1_5']:.1%})"
        for _, row in top_outlier.iterrows()
        if row["outlier_ratio_1_5"] > 0
    ]
    top_extreme = table.nlargest(top_n, "extreme_ratio_3_0")
    extreme_items = [
        f"{row['column']}({row['extreme_ratio_3_0']:.1%})"
        for _, row in top_extreme.iterrows()
        if row["extreme_ratio_3_0"] > 0
    ]

    summary_parts = []
    if outlier_items:
        summary_parts.append(
            f"<b>이상치 비율(1.5×IQR) 상위:</b> {', '.join(outlier_items)}"
        )
    if extreme_items:
        summary_parts.append(
            f"<b>극단 이상치 비율(3×IQR) 상위:</b> {', '.join(extreme_items)}"
        )

    # --- 테이블 포매팅 ---
    display_cols = [
        "column", "count", "median", "iqr", "min", "max",
        "outlier_cnt_1_5", "outlier_ratio_1_5",
        "extreme_cnt_3_0", "extreme_ratio_3_0",
        "skew_signal", "distribution_note",
    ]
    display_df = table[[c for c in display_cols if c in table.columns]].head(top_n).copy()
    display_df["outlier_ratio_1_5"] = display_df["outlier_ratio_1_5"].apply(
        lambda x: f"{x:.1%}"
    )
    display_df["extreme_ratio_3_0"] = display_df["extreme_ratio_3_0"].apply(
        lambda x: f"{x:.1%}"
    )

    def _format_value(value: float) -> str:
        if pd.isna(value):
            return ""
        abs_value = abs(value)
        if abs_value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:.2f}T"
        if abs_value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        if abs_value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if abs_value >= 1_000:
            return f"{value / 1_000:.2f}K"
        return f"{value:,.2f}"

    float_cols = ["median", "iqr", "min", "max"]
    for col in float_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(_format_value)

    display_df = display_df.rename(columns={
        "column": "컬럼",
        "count": "행 수",
        "median": "중앙값",
        "iqr": "IQR",
        "min": "최솟값",
        "max": "최댓값",
        "outlier_cnt_1_5": "이상치 수",
        "outlier_ratio_1_5": "이상치 비율",
        "extreme_cnt_3_0": "극단 수",
        "extreme_ratio_3_0": "극단 비율",
        "skew_signal": "왜도 신호",
        "distribution_note": "분포 방향",
    })

    table_html = display_df.to_html(
        index=False,
        escape=False,
        border=0,
        classes="outlier-table",
    )

    # --- 스타일 ---
    styled_html = f"""
<style>
.outlier-section h2 {{ margin-top: 1.5em; }}
.outlier-section p {{ margin: 0.5em 0; line-height: 1.6; }}
.outlier-section .summary {{ background: #f8f9fa; padding: 12px 16px;
  border-left: 4px solid #4a90d9; margin: 1em 0; border-radius: 4px; }}
.outlier-section .table-wrap {{
  max-width: 100%; overflow-x: auto; margin: 1em 0;
}}
.outlier-section table.outlier-table {{
  border-collapse: collapse; width: 100%; min-width: 820px; font-size: 0.9em;
}}
.outlier-section table.outlier-table th {{
  background: #4a90d9; color: white; padding: 8px 10px;
  text-align: right; white-space: nowrap;
}}
.outlier-section table.outlier-table th:first-child {{ text-align: left; }}
.outlier-section table.outlier-table td {{
  padding: 6px 10px; border-bottom: 1px solid #e0e0e0;
  text-align: right; white-space: nowrap;
}}
.outlier-section table.outlier-table td:first-child {{
  text-align: left; font-weight: 600; position: sticky; left: 0;
  background: white; box-shadow: 1px 0 0 #e0e0e0;
}}
.outlier-section table.outlier-table tr:hover td {{ background: #f0f4ff; }}
.outlier-section .note {{
  color: #666; font-size: 0.9em; margin-top: 0.25em;
}}
</style>

<div class="outlier-section">
<h2>이상치 판정표</h2>
<p>이 표는 각 수치형 변수에 대해 IQR(사분위 범위) 기반 이상치 후보 및
극단 이상치 범위를 계산한 결과입니다.</p>
<p>1.5×IQR 초과 값은 <b>이상치 후보</b>, 3×IQR 초과 값은
<b>극단 이상치</b>로 간주했습니다.
(outlier_ratio_1_5 내림차순 정렬)</p>

<div class="summary">
{"<br>".join(summary_parts) if summary_parts else "이상치가 감지된 컬럼이 없습니다."}
</div>

<p class="note">아래 표는 이상치 비율이 높은 상위 {top_n}개 컬럼만 표시합니다.
큰 숫자는 K/M/B/T 단위로 축약했습니다.
전체 판정표는 노트북 변수 <code>outlier_table</code>에서 확인할 수 있습니다.</p>
<div class="table-wrap">
{table_html}
</div>
</div>
"""
    return styled_html
