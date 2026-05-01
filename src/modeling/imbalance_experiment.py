"""
클래스 불균형 대응 전략 비교 실험.

RandomForest를 기준 모델로, 다양한 불균형 처리 전략의
valid set PR-AUC를 비교하여 최적 전략을 확정한다.

사용법:
    python -m src.modeling.imbalance_experiment
    python -m src.modeling.imbalance_experiment --horizon 10 12
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from imblearn.combine import SMOTETomek
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from sklearn.ensemble import RandomForestClassifier
from sklearn.utils.class_weight import compute_sample_weight

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.data_loader import load_and_prepare, print_data_summary
from src.modeling.evaluate import (
    compute_metrics,
    find_best_threshold,
)

warnings.filterwarnings("ignore", category=FutureWarning)


# ---------------------------------------------------------------------------
# RF 공통 파라미터
# ---------------------------------------------------------------------------

RF_BASE_PARAMS = dict(
    n_estimators=300,
    max_depth=10,
    min_samples_leaf=5,
    random_state=42,
    n_jobs=-1,
)


# ---------------------------------------------------------------------------
# 전략 정의
# ---------------------------------------------------------------------------


def _train_and_eval(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    rf_kwargs: dict | None = None,
    sample_weight: np.ndarray | None = None,
) -> dict:
    """RF를 학습하고 valid 평가 결과를 반환한다."""
    params = {**RF_BASE_PARAMS}
    if rf_kwargs:
        params.update(rf_kwargs)

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train, sample_weight=sample_weight)

    y_proba = model.predict_proba(X_valid)[:, 1]

    # default threshold
    y_pred_default = (y_proba >= 0.5).astype(int)
    metrics_default = compute_metrics(y_valid, y_pred_default, y_proba)

    # optimized threshold
    best_thr, _ = find_best_threshold(y_valid, y_proba, metric="f1")
    y_pred_opt = (y_proba >= best_thr).astype(int)
    metrics_opt = compute_metrics(y_valid, y_pred_opt, y_proba)

    return {
        "metrics_default": metrics_default,
        "metrics_optimized": metrics_opt,
        "threshold": best_thr,
        "train_shape": X_train.shape,
    }


def strategy_baseline(X_train, y_train, X_valid, y_valid):
    """전략 1: 아무 처리 없이 RF (baseline)."""
    return _train_and_eval(X_train, y_train, X_valid, y_valid)


def strategy_class_weight(X_train, y_train, X_valid, y_valid):
    """전략 2: class_weight='balanced'."""
    return _train_and_eval(
        X_train, y_train, X_valid, y_valid,
        rf_kwargs={"class_weight": "balanced"},
    )


def strategy_sample_weight(X_train, y_train, X_valid, y_valid):
    """전략 3: compute_sample_weight로 수동 가중치."""
    sw = compute_sample_weight("balanced", y_train)
    return _train_and_eval(
        X_train, y_train, X_valid, y_valid,
        sample_weight=sw,
    )


def strategy_smote(X_train, y_train, X_valid, y_valid):
    """전략 4: SMOTE 오버샘플링."""
    # k_neighbors를 소수 클래스 수에 맞게 조정
    n_pos = int(y_train.sum())
    k = min(5, n_pos - 1) if n_pos > 1 else 1
    smote = SMOTE(random_state=42, k_neighbors=k)
    X_res, y_res = smote.fit_resample(X_train, y_train)
    return _train_and_eval(X_res, y_res, X_valid, y_valid)


def strategy_smote_tomek(X_train, y_train, X_valid, y_valid):
    """전략 5: SMOTE + Tomek Links."""
    n_pos = int(y_train.sum())
    k = min(5, n_pos - 1) if n_pos > 1 else 1
    smote_tomek = SMOTETomek(
        smote=SMOTE(random_state=42, k_neighbors=k),
        random_state=42,
    )
    X_res, y_res = smote_tomek.fit_resample(X_train, y_train)
    return _train_and_eval(X_res, y_res, X_valid, y_valid)


def strategy_undersample(X_train, y_train, X_valid, y_valid):
    """전략 6: Random UnderSampling (다수 클래스 10:1로 축소)."""
    n_pos = int(y_train.sum())
    target_neg = n_pos * 10
    rus = RandomUnderSampler(
        sampling_strategy={0: target_neg, 1: n_pos},
        random_state=42,
    )
    X_res, y_res = rus.fit_resample(X_train, y_train)
    return _train_and_eval(X_res, y_res, X_valid, y_valid)


def strategy_undersample_cw(X_train, y_train, X_valid, y_valid):
    """전략 7: UnderSampling(10:1) + class_weight='balanced'."""
    n_pos = int(y_train.sum())
    target_neg = n_pos * 10
    rus = RandomUnderSampler(
        sampling_strategy={0: target_neg, 1: n_pos},
        random_state=42,
    )
    X_res, y_res = rus.fit_resample(X_train, y_train)
    return _train_and_eval(
        X_res, y_res, X_valid, y_valid,
        rf_kwargs={"class_weight": "balanced"},
    )


# 전략 레지스트리
STRATEGIES = {
    "baseline":         strategy_baseline,
    "class_weight":     strategy_class_weight,
    "sample_weight":    strategy_sample_weight,
    "smote":            strategy_smote,
    "smote_tomek":      strategy_smote_tomek,
    "undersample_10:1": strategy_undersample,
    "under+cw":         strategy_undersample_cw,
}


# ---------------------------------------------------------------------------
# 실험 실행
# ---------------------------------------------------------------------------


def run_imbalance_experiment(horizon: int) -> pd.DataFrame:
    """단일 horizon에 대해 전체 전략 비교를 실행한다."""
    data = load_and_prepare(horizon)
    print_data_summary(horizon, data)

    X_train = data["X_train"].values
    y_train = data["y_train"].values
    X_valid = data["X_valid"].values
    y_valid = data["y_valid"].values

    rows = []
    for name, fn in STRATEGIES.items():
        print(f"  Running: {name} ...", end=" ", flush=True)
        try:
            result = fn(X_train, y_train, X_valid, y_valid)
            m = result["metrics_optimized"]
            rows.append({
                "strategy": name,
                "train_rows": result["train_shape"][0],
                "threshold": round(result["threshold"], 4),
                "f1": m["f1"],
                "precision": m["precision"],
                "recall": m["recall"],
                "roc_auc": m["roc_auc"],
                "pr_auc": m["pr_auc"],
            })
            print(f"PR-AUC={m['pr_auc']:.4f}  F1={m['f1']:.4f}")
        except Exception as e:
            print(f"ERROR: {e}")
            rows.append({"strategy": name, "pr_auc": None})

    df = pd.DataFrame(rows)
    df = df.sort_values("pr_auc", ascending=False).reset_index(drop=True)
    return df


def print_result_table(df: pd.DataFrame, horizon: int) -> None:
    """비교 결과를 포맷팅하여 출력한다."""
    print(f"\n{'='*80}")
    print(f"  Imbalance Strategy Comparison — H{horizon} (valid set, optimized threshold)")
    print(f"{'='*80}")

    cols = ["strategy", "train_rows", "threshold", "pr_auc", "f1", "precision", "recall", "roc_auc"]
    display_cols = [c for c in cols if c in df.columns]

    header = f"{'Strategy':<20} {'Rows':>8} {'Thr':>6}"
    header += f"{'PR-AUC':>10} {'F1':>8} {'Prec':>8} {'Recall':>8} {'ROC-AUC':>10}"
    print(header)
    print("-" * len(header))

    for _, row in df.iterrows():
        if row.get("pr_auc") is None:
            print(f"{row['strategy']:<20}  ERROR")
            continue
        line = f"{row['strategy']:<20} {int(row['train_rows']):>8,} {row['threshold']:>6.3f}"
        line += f"{row['pr_auc']:>10.4f} {row['f1']:>8.4f} {row['precision']:>8.4f}"
        line += f" {row['recall']:>8.4f} {row['roc_auc']:>10.4f}"
        print(line)

    print()
    best = df.iloc[0]
    print(f"  ** Best strategy: {best['strategy']}  (PR-AUC={best['pr_auc']:.4f})")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="클래스 불균형 대응 전략 비교 실험")
    parser.add_argument(
        "--horizon", nargs="+", type=int, default=[10],
        help="실험할 horizon (기본: 10)",
    )
    args = parser.parse_args()

    for h in args.horizon:
        df = run_imbalance_experiment(h)
        print_result_table(df, h)


if __name__ == "__main__":
    main()
