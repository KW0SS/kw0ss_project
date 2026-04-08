"""
Baseline 분류 모델 파이프라인.

Phase 4: clean_data가 도착하면 바로 실행할 수 있는 baseline 뼈대.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier

from src.analysis.utils import classify_columns, load_csv


# ---------------------------------------------------------------------------
# 4-1. 전처리
# ---------------------------------------------------------------------------


def prepare_features(
    df: pd.DataFrame,
    target: str = "label",
) -> tuple[pd.DataFrame, pd.Series]:
    """수치형 컬럼만 선택하고 결측을 median으로 대체한 뒤 X/y를 분리한다."""
    cols = classify_columns(df)
    feature_cols = cols["ratio"] + cols["raw_value"]

    X = df[feature_cols].copy()
    y = df[target].copy()

    imputer = SimpleImputer(strategy="median")
    X = pd.DataFrame(imputer.fit_transform(X), columns=feature_cols, index=df.index)

    return X, y


def time_split(
    df: pd.DataFrame,
    train_end: int,
    val_end: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """year 기준으로 train / val / test를 분할한다.

    - train: year <= train_end
    - val:   train_end < year <= val_end
    - test:  year > val_end
    """
    train = df[df["year"] <= train_end]
    val = df[(df["year"] > train_end) & (df["year"] <= val_end)]
    test = df[df["year"] > val_end]
    return train, val, test


def random_split(
    df: pd.DataFrame,
    test_size: float = 0.2,
    val_size: float = 0.1,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """랜덤으로 train / val / test를 분할한다."""
    rng = np.random.RandomState(seed)
    indices = rng.permutation(len(df))

    n_test = int(len(df) * test_size)
    n_val = int(len(df) * val_size)

    test = df.iloc[indices[:n_test]]
    val = df.iloc[indices[n_test : n_test + n_val]]
    train = df.iloc[indices[n_test + n_val :]]
    return train, val, test


# ---------------------------------------------------------------------------
# 4-2. 모델 학습
# ---------------------------------------------------------------------------


def train_logistic(
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> LogisticRegression:
    """Logistic Regression을 학습한다."""
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)

    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_scaled, y_train)
    model._scaler = scaler  # 예측 시 동일 스케일링 적용용
    return model


def train_decision_tree(
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> DecisionTreeClassifier:
    """Decision Tree를 학습한다."""
    model = DecisionTreeClassifier(
        max_depth=5,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


# ---------------------------------------------------------------------------
# 4-3. 평가
# ---------------------------------------------------------------------------


def _predict(model, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """모델에 맞는 predict / predict_proba를 수행한다."""
    if hasattr(model, "_scaler"):
        X_input = model._scaler.transform(X)
    else:
        X_input = X

    y_pred = model.predict(X_input)
    y_proba = model.predict_proba(X_input)[:, 1]
    return y_pred, y_proba


def evaluate(
    model,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> dict[str, float]:
    """모델 성능 지표를 딕셔너리로 반환한다."""
    y_pred, y_proba = _predict(model, X_test)

    precision_curve, recall_curve, _ = precision_recall_curve(y_test, y_proba)
    pr_auc = np.trapz(precision_curve, recall_curve)

    return {
        "f1": round(f1_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "roc_auc": round(roc_auc_score(y_test, y_proba), 4),
        "pr_auc": round(abs(pr_auc), 4),
    }


def print_report(results: dict[str, dict[str, float]]) -> None:
    """모델별 지표 비교 테이블을 출력한다."""
    metrics = ["f1", "precision", "recall", "roc_auc", "pr_auc"]
    header = f"{'Model':<25}" + "".join(f"{m:>12}" for m in metrics)
    print(header)
    print("-" * len(header))
    for name, scores in results.items():
        row = f"{name:<25}" + "".join(f"{scores.get(m, 0):>12.4f}" for m in metrics)
        print(row)


# ---------------------------------------------------------------------------
# 4-4. 메인 실행
# ---------------------------------------------------------------------------


def run_baseline(
    csv_path: str,
    split: str = "time",
    train_end: int = 2021,
    val_end: int = 2022,
) -> dict[str, dict[str, float]]:
    """전체 baseline 파이프라인: 로드 → 전처리 → 분할 → 학습 → 평가."""
    df = load_csv(csv_path)

    # 분할
    if split == "time":
        train_df, val_df, test_df = time_split(df, train_end, val_end)
    else:
        train_df, val_df, test_df = random_split(df)

    print(f"Split: {split}")
    print(f"  train: {len(train_df):,}행, val: {len(val_df):,}행, test: {len(test_df):,}행")
    print(f"  label 분포 (train): {dict(train_df['label'].value_counts())}")
    print()

    # 전처리
    X_train, y_train = prepare_features(train_df)
    X_val, y_val = prepare_features(val_df)
    X_test, y_test = prepare_features(test_df)

    # 학습
    models = {
        "LogisticRegression": train_logistic(X_train, y_train),
        "DecisionTree(depth=5)": train_decision_tree(X_train, y_train),
    }

    # 평가 (val)
    print("=== Validation Set ===")
    val_results = {name: evaluate(m, X_val, y_val) for name, m in models.items()}
    print_report(val_results)
    print()

    # 평가 (test)
    print("=== Test Set ===")
    test_results = {name: evaluate(m, X_test, y_test) for name, m in models.items()}
    print_report(test_results)

    return test_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baseline 분류 모델 실행")
    parser.add_argument(
        "--data",
        default="preprocess/data/output/clean_data_no_macro.csv",
        help="CSV 파일 경로",
    )
    parser.add_argument(
        "--split",
        choices=["time", "random"],
        default="time",
        help="분할 방식 (default: time)",
    )
    parser.add_argument("--train-end", type=int, default=2021, help="train 마지막 연도")
    parser.add_argument("--val-end", type=int, default=2022, help="val 마지막 연도")
    args = parser.parse_args()

    run_baseline(args.data, args.split, args.train_end, args.val_end)
