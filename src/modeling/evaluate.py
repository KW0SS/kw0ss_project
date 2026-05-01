"""
모델 평가 프레임워크.

극심한 클래스 불균형(~158:1) 환경에 맞춘 평가 지표 계산,
threshold 최적화, 결과 저장/비교 기능을 제공한다.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = PROJECT_ROOT / "results"


# ---------------------------------------------------------------------------
# 핵심 평가 함수
# ---------------------------------------------------------------------------


def compute_metrics(
    y_true: np.ndarray | pd.Series,
    y_pred: np.ndarray,
    y_proba: np.ndarray,
) -> dict[str, float]:
    """분류 모델의 주요 지표를 계산한다.

    Args:
        y_true: 실제 라벨
        y_pred: 예측 라벨 (0/1)
        y_proba: positive class 확률

    Returns:
        {"f1", "precision", "recall", "roc_auc", "pr_auc"}
    """
    return {
        "f1": round(f1_score(y_true, y_pred, zero_division=0), 4),
        "precision": round(precision_score(y_true, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_true, y_pred, zero_division=0), 4),
        "roc_auc": round(roc_auc_score(y_true, y_proba), 4),
        "pr_auc": round(average_precision_score(y_true, y_proba), 4),
    }


# ---------------------------------------------------------------------------
# Threshold 최적화
# ---------------------------------------------------------------------------


def find_best_threshold(
    y_true: np.ndarray | pd.Series,
    y_proba: np.ndarray,
    metric: str = "f1",
) -> tuple[float, float]:
    """PR curve 기반으로 최적 threshold를 탐색한다.

    Args:
        metric: 최적화 대상 ("f1" 또는 "recall_at_precision")

    Returns:
        (best_threshold, best_score)
    """
    precisions, recalls, thresholds = precision_recall_curve(y_true, y_proba)

    # precision_recall_curve는 thresholds가 len-1 이므로 마지막 제거
    precisions = precisions[:-1]
    recalls = recalls[:-1]

    if metric == "f1":
        with np.errstate(divide="ignore", invalid="ignore"):
            denom = precisions + recalls
            f1_scores = np.where(denom > 0, 2 * precisions * recalls / denom, 0.0)
        best_idx = np.argmax(f1_scores)
        return float(thresholds[best_idx]), float(f1_scores[best_idx])

    if metric == "recall_at_precision":
        # precision >= 0.1 조건에서 recall 최대화
        mask = precisions >= 0.1
        if mask.any():
            valid_idx = np.where(mask)[0]
            best_in_valid = valid_idx[np.argmax(recalls[valid_idx])]
            return float(thresholds[best_in_valid]), float(recalls[best_in_valid])

    # fallback: F1 기준
    with np.errstate(divide="ignore", invalid="ignore"):
        denom = precisions + recalls
        f1_scores = np.where(denom > 0, 2 * precisions * recalls / denom, 0.0)
    best_idx = np.argmax(f1_scores)
    return float(thresholds[best_idx]), float(f1_scores[best_idx])


# ---------------------------------------------------------------------------
# 모델 평가 통합 함수
# ---------------------------------------------------------------------------


def evaluate_model(
    model,
    X: pd.DataFrame | np.ndarray,
    y: pd.Series | np.ndarray,
    threshold: float | None = None,
    scaler=None,
) -> dict:
    """모델을 평가하고 결과 dict를 반환한다.

    Args:
        model: predict_proba를 지원하는 sklearn-compatible 모델
        X: 피처 행렬
        y: 타깃 벡터
        threshold: 커스텀 threshold. None이면 0.5
        scaler: 적용할 scaler (LogisticRegression 등)

    Returns:
        {"metrics": {...}, "threshold": float, "y_pred": array, "y_proba": array}
    """
    X_input = scaler.transform(X) if scaler is not None else X

    y_proba = model.predict_proba(X_input)[:, 1]

    if threshold is None:
        threshold = 0.5
    y_pred = (y_proba >= threshold).astype(int)

    metrics = compute_metrics(y, y_pred, y_proba)

    return {
        "metrics": metrics,
        "threshold": threshold,
        "y_pred": y_pred,
        "y_proba": y_proba,
    }


# ---------------------------------------------------------------------------
# 결과 저장 / 로드
# ---------------------------------------------------------------------------


def save_result(
    model_name: str,
    horizon: int,
    valid_metrics: dict[str, float],
    test_metrics: dict[str, float],
    params: dict | None = None,
    threshold: float = 0.5,
    notes: str = "",
    results_dir: Path | str | None = None,
) -> Path:
    """실험 결과를 JSON 파일로 저장한다.

    Returns:
        저장된 파일 경로
    """
    out_dir = Path(results_dir) if results_dir else RESULTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "model_name": model_name,
        "horizon": horizon,
        "threshold": threshold,
        "valid": valid_metrics,
        "test": test_metrics,
        "params": params or {},
        "notes": notes,
        "timestamp": datetime.now().isoformat(),
    }

    filename = f"{model_name}_H{horizon}.json"
    filepath = out_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return filepath


def load_results(
    results_dir: Path | str | None = None,
) -> list[dict]:
    """results/ 디렉터리의 모든 JSON 결과를 로드한다."""
    out_dir = Path(results_dir) if results_dir else RESULTS_DIR
    results = []
    for json_path in sorted(out_dir.glob("*.json")):
        with open(json_path, encoding="utf-8") as f:
            results.append(json.load(f))
    return results


# ---------------------------------------------------------------------------
# 비교 테이블
# ---------------------------------------------------------------------------

METRIC_NAMES = ["f1", "precision", "recall", "roc_auc", "pr_auc"]


def build_comparison_table(
    results: list[dict],
    split: str = "test",
) -> pd.DataFrame:
    """여러 실험 결과를 비교 테이블로 만든다.

    Args:
        results: load_results()의 반환값 또는 동일 형식의 list
        split: "valid" 또는 "test"

    Returns:
        모델별 metric 비교 DataFrame
    """
    rows = []
    for r in results:
        row = {
            "model": r["model_name"],
            "horizon": r["horizon"],
            "threshold": r.get("threshold", 0.5),
        }
        metrics = r.get(split, {})
        for m in METRIC_NAMES:
            row[m] = metrics.get(m, None)
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("pr_auc", ascending=False).reset_index(drop=True)
    return df


def print_comparison(
    results: list[dict],
    split: str = "test",
) -> None:
    """모델별 metric 비교 테이블을 출력한다."""
    df = build_comparison_table(results, split)
    if df.empty:
        print("결과 없음")
        return

    print(f"\n=== Model Comparison ({split} set) ===")
    header = f"{'Model':<25} {'H':>3} {'Thr':>5}"
    header += "".join(f"{m:>12}" for m in METRIC_NAMES)
    print(header)
    print("-" * len(header))

    for _, row in df.iterrows():
        line = f"{row['model']:<25} {row['horizon']:>3} {row['threshold']:>5.2f}"
        for m in METRIC_NAMES:
            val = row.get(m)
            line += f"{val:>12.4f}" if val is not None else f"{'N/A':>12}"
        print(line)
    print()
