# Experiment Comparison Report

## Executive Summary

- Baseline:
- Candidates:
- Best overall setup:
- Best operational setup:
- Main trade-off:
- Recommendation:

## 1. Comparison Objective

- Comparison question:
- Baseline experiment:
- Candidate experiments:
- Decision criterion:
- Operational context:

## 2. Controlled and Changed Variables

### Controlled Variables

- Dataset source:
- Target definition:
- Train/validation/test split:
- Preprocessing settings:
- Model families:
- Evaluation metrics:

### Changed Variables

- Horizon:
- Preprocessing variant:
- Feature set:
- Hyperparameters:
- Threshold policy:

## 3. Dataset Distribution Comparison

| Experiment | Horizon | Samples | Positives | Negatives | Positive Rate |
|---|---:|---:|---:|---:|---:|
| Baseline | Not available | Not available | Not available | Not available | Not available |

## 4. Overall Performance Comparison

| Experiment | Best Model | ROC-AUC | PR-AUC | Precision | Recall | F1 | Delta vs Baseline |
|---|---|---:|---:|---:|---:|---:|---:|
| Baseline | Not available | Not available | Not available | Not available | Not available | Not available | 0.000 |

## 5. Model-Level Comparison

| Model | Baseline Metric | Candidate Metric | Delta | Interpretation |
|---|---:|---:|---:|---|
| RF | Not available | Not available | Not available | Not available |
| XGBoost | Not available | Not available | Not available | Not available |
| LightGBM | Not available | Not available | Not available | Not available |

## 6. Horizon Trade-Off Analysis

- Short-horizon strengths:
- Short-horizon weaknesses:
- Long-horizon strengths:
- Long-horizon weaknesses:
- Positive-count effect:
- Practical lead-time trade-off:
- Sweet-spot candidate:

## 7. Threshold Comparison

| Experiment | Threshold Strategy | Precision | Recall | F1 | Operational Meaning |
|---|---|---:|---:|---:|---|
| Baseline | Not available | Not available | Not available | Not available | Not available |

## 8. Stability Analysis

- Metric volatility:
- Seed sensitivity:
- Split sensitivity:
- Outlier experiment:
- Consistency of model ranking:

## 9. Feature and Interpretation Comparison

- Common important features:
- Horizon-specific features:
- Preprocessing-sensitive features:
- Financial interpretation consistency:
- Leakage or proxy-risk concerns:

## 10. Case-Level Comparison

- Cases improved by candidate experiments:
- Cases worsened by candidate experiments:
- False-negative changes:
- False-positive changes:
- Early-warning examples:

## 11. Recommendation

- Recommended horizon:
- Recommended model:
- Recommended threshold policy:
- Recommended preprocessing:
- Deployment caveats:

## 12. Follow-Up Experiments

1. Tune the selected horizon with Optuna.
2. Fix a cost-sensitive threshold policy.
3. Compare preprocessing variants under the selected horizon.
4. Add sector and lag/diff features.
5. Evaluate ensemble or stacking approaches.
6. Add SHAP-based interpretation for the selected model.
