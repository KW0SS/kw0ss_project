"""XGBoost 학습 모듈."""

from __future__ import annotations

import numpy as np
import pandas as pd
import xgboost as xgb


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame | None = None,
    y_valid: pd.Series | None = None,
) -> tuple[xgb.XGBClassifier, dict]:
    """XGBoost를 학습하고 (model, params) 튜플을 반환한다.

    Phase 2 결론: scale_pos_weight 없이 baseline + threshold 최적화가 유리하지만,
    XGBoost 자체 내장 불균형 처리도 비교를 위해 scale_pos_weight는 1로 설정.
    early stopping으로 과적합 방지 및 학습 시간 절감.
    """
    params = dict(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        scale_pos_weight=1,
        eval_metric="aucpr",
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=2,
        tree_method="hist",
        verbosity=0,
    )

    fit_params = {}
    if X_valid is not None and y_valid is not None:
        fit_params["eval_set"] = [(X_valid, y_valid)]
        fit_params["verbose"] = False

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train, **fit_params)

    # 실제 사용된 트리 수 기록
    actual_params = {**params}
    actual_params["best_iteration"] = getattr(model, "best_iteration", params["n_estimators"])

    return model, actual_params
