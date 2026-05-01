"""Gradient Boosting (sklearn) 학습 모듈."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame | None = None,
    y_valid: pd.Series | None = None,
) -> tuple[GradientBoostingClassifier, dict]:
    """sklearn GBM을 학습하고 (model, params) 튜플을 반환한다.

    sklearn GBM은 class_weight를 지원하지 않으므로
    Phase 2 결론에 따라 가중치 없이 학습한다.
    n_estimators를 낮추고 subsample로 속도를 확보한다.
    """
    params = dict(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        min_samples_leaf=10,
        random_state=42,
    )

    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    return model, params
