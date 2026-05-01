"""LightGBM 학습 모듈."""

from __future__ import annotations

import numpy as np
import pandas as pd
import lightgbm as lgb


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame | None = None,
    y_valid: pd.Series | None = None,
) -> tuple[lgb.LGBMClassifier, dict]:
    """LightGBM을 학습하고 (model, params) 튜플을 반환한다.

    Phase 2 결론에 따라 is_unbalance=False (baseline) + threshold 최적화.
    early stopping + callback으로 과적합 방지 및 학습 시간 절감.
    """
    params = dict(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=20,
        is_unbalance=False,
        metric="average_precision",
        random_state=42,
        n_jobs=2,
        verbose=-1,
    )

    callbacks = [
        lgb.early_stopping(stopping_rounds=30, verbose=False),
        lgb.log_evaluation(period=0),
    ]

    fit_params = {"callbacks": callbacks}
    if X_valid is not None and y_valid is not None:
        fit_params["eval_set"] = [(X_valid, y_valid)]

    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train, **fit_params)

    actual_params = {**params}
    actual_params["best_iteration"] = getattr(model, "best_iteration_", params["n_estimators"])

    return model, actual_params
