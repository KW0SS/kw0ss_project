"""
train_xgb_v4_replica.py
─────────────────────────────────────────────────────────────────────────────
박민서 v4 보고서를 우리 환경에서 재현하는 XGBoost 학습 스크립트.

기존 src/modeling/train_xgboost.py 는 건드리지 않는다.

사용 데이터:
  preprocess/data/processed/v4_replica/{variant}/{train,valid,test}.csv

변형:
  - all_delisted : delisted 폴더 전체 = label 1
  - only_n1      : 상폐 정확히 1년 전 분기만 = label 1

PDF 보고서 grid1_02 best setting을 그대로 사용:
  max_depth=3, learning_rate=0.03, n_estimators=400,
  subsample=0.8, colsample_bytree=0.8,
  scale_pos_weight = neg/pos (train 기준)

threshold 최적화:
  validation 에서 [0.05, 0.95] (step 0.01) 중 F1 최대 → test에 그대로 적용
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

REPO_ROOT  = Path(__file__).resolve().parents[2]
DATA_BASE  = REPO_ROOT / "preprocess" / "data" / "processed" / "v4_replica"
RESULT_DIR = REPO_ROOT / "results" / "exp_010_v4_replica"

VARIANTS = ["all_delisted", "only_n1"]

NON_FEATURE_COLS = {"stock_code", "year", "quarter", "gics_sector", "label", "_label_raw"}


# ═══════════════════════════════════════════════════════════════
def load_split(variant: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    base = DATA_BASE / variant
    tr = pd.read_csv(base / "train.csv")
    va = pd.read_csv(base / "valid.csv")
    te = pd.read_csv(base / "test.csv")
    feat_cols = [c for c in tr.columns if c not in NON_FEATURE_COLS]
    return tr, va, te, feat_cols


def best_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> tuple[float, float]:
    """validation F1 최대화 threshold."""
    grid = np.arange(0.05, 0.96, 0.01)
    best_t, best_f1 = 0.5, -1.0
    for t in grid:
        pred = (y_prob >= t).astype(int)
        if pred.sum() == 0:
            continue
        f1 = f1_score(y_true, pred, zero_division=0)
        if f1 > best_f1:
            best_f1, best_t = float(f1), float(t)
    return best_t, best_f1


def eval_at_threshold(y_true: np.ndarray, y_prob: np.ndarray, t: float) -> dict:
    pred = (y_prob >= t).astype(int)
    return {
        "threshold": float(t),
        "precision": float(precision_score(y_true, pred, zero_division=0)),
        "recall":    float(recall_score(y_true, pred, zero_division=0)),
        "f1":        float(f1_score(y_true, pred, zero_division=0)),
        "roc_auc":   float(roc_auc_score(y_true, y_prob)),
        "pr_auc":    float(average_precision_score(y_true, y_prob)),
        "pos_pred":  int(pred.sum()),
    }


# ═══════════════════════════════════════════════════════════════
def train_one(variant: str, seed: int = 42) -> dict:
    tr, va, te, feat_cols = load_split(variant)

    X_tr, y_tr = tr[feat_cols].values, tr["label"].astype(int).values
    X_va, y_va = va[feat_cols].values, va["label"].astype(int).values
    X_te, y_te = te[feat_cols].values, te["label"].astype(int).values

    pos = int((y_tr == 1).sum())
    neg = int((y_tr == 0).sum())
    spw = neg / pos if pos > 0 else 1.0

    params = {
        "n_estimators":     400,
        "max_depth":        3,
        "learning_rate":    0.03,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": spw,
        "objective":        "binary:logistic",
        "eval_metric":      "aucpr",
        "tree_method":      "hist",
        "random_state":     seed,
        "n_jobs":           4,
        "verbosity":        0,
    }

    model = xgb.XGBClassifier(**params)
    model.fit(X_tr, y_tr, eval_set=[(X_va, y_va)], verbose=False)

    p_va = model.predict_proba(X_va)[:, 1]
    p_te = model.predict_proba(X_te)[:, 1]

    best_t, _ = best_threshold(y_va, p_va)
    valid_metrics = eval_at_threshold(y_va, p_va, best_t)
    test_metrics  = eval_at_threshold(y_te, p_te, best_t)

    # feature importance (top 15)
    fi = model.feature_importances_
    fi_pairs = sorted(zip(feat_cols, fi.tolist()), key=lambda x: x[1], reverse=True)[:15]

    return {
        "variant":  variant,
        "data": {
            "train_rows": len(tr), "valid_rows": len(va), "test_rows": len(te),
            "train_pos":  pos,     "train_neg":  neg,
            "valid_pos":  int(y_va.sum()), "valid_neg": int((y_va == 0).sum()),
            "test_pos":   int(y_te.sum()), "test_neg":  int((y_te == 0).sum()),
            "imbalance_train": neg / pos if pos > 0 else None,
            "n_features": len(feat_cols),
        },
        "params":   {k: (float(v) if isinstance(v, (np.floating,)) else v) for k, v in params.items()},
        "valid":    valid_metrics,
        "test":     test_metrics,
        "feature_importance_top15": [
            {"feature": f, "importance": float(v)} for f, v in fi_pairs
        ],
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
    }


# ═══════════════════════════════════════════════════════════════
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", choices=VARIANTS + ["all"], default="all")
    ap.add_argument("--seed",    type=int, default=42)
    args = ap.parse_args()

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    targets = VARIANTS if args.variant == "all" else [args.variant]

    summary = {}
    for v in targets:
        print(f"[{v}] 학습 시작 ...")
        res = train_one(v, seed=args.seed)
        out_path = RESULT_DIR / f"{v}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        print(f"  → {out_path}")
        print(f"  threshold={res['valid']['threshold']:.3f}  "
              f"valid F1={res['valid']['f1']:.4f}  "
              f"test F1={res['test']['f1']:.4f}  "
              f"test PR-AUC={res['test']['pr_auc']:.4f}")
        summary[v] = res

    print("\n=== 요약 ===")
    print(f"{'variant':<14} {'thr':>6} {'val_F1':>8} {'tst_F1':>8} {'tst_P':>8} {'tst_R':>8} {'tst_PR':>8}")
    for v, r in summary.items():
        print(f"{v:<14} {r['valid']['threshold']:>6.3f} {r['valid']['f1']:>8.4f} "
              f"{r['test']['f1']:>8.4f} {r['test']['precision']:>8.4f} "
              f"{r['test']['recall']:>8.4f} {r['test']['pr_auc']:>8.4f}")


if __name__ == "__main__":
    main()
