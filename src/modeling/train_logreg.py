"""Logistic Regression 학습 모듈.

StandardScaler + LogisticRegression 을 Pipeline 으로 묶어
다른 tree 기반 모델과 동일한 인터페이스(predict_proba)로 동작하게 한다.
극심한 불균형을 고려해 class_weight='balanced' 기본 적용.
"""

from __future__ import annotations

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_valid: pd.DataFrame | None = None,
    y_valid: pd.Series | None = None,
) -> tuple[Pipeline, dict]:
    params = dict(
        C=1.0,
        penalty="l2",
        solver="lbfgs",
        class_weight="balanced",
        max_iter=2000,
        random_state=42,
        n_jobs=2,
    )

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(**params)),
    ])
    pipeline.fit(X_train, y_train)

    return pipeline, params
