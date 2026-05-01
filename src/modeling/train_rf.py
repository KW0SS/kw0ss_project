"""Random Forest 학습 모듈."""

from __future__ import annotations

import pandas as pd
from sklearn.ensemble import RandomForestClassifier


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame | None = None,
    y_valid: pd.Series | None = None,
) -> tuple[RandomForestClassifier, dict]:
    """RF를 학습하고 (model, params) 튜플을 반환한다."""
    params = dict(
        n_estimators=200,
        max_depth=10,
        min_samples_leaf=5,
        max_features="sqrt",
        random_state=42,
        n_jobs=2,
    )

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    return model, params
