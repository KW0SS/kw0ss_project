# ML Training Strategy

## Executive Summary

- Current baseline:
- Main objective:
- Primary metric:
- Operational constraint:
- Recommended next run:
- Main risk:

## 1. Current State

- Baseline experiment:
- Dataset version:
- Prediction horizon:
- Preprocessing:
- Model families:
- Best model:
- Best metric:
- Known limitation:

## 2. Decision Target

Choose the primary target for the next stage.

- Improve ranking quality:
- Improve recall:
- Improve precision:
- Improve early-warning lead time:
- Improve threshold stability:
- Improve interpretability:

## 3. High-Priority Experiments

### 3.1 Horizon Sweep

- Runs:
- Controlled variables:
- Changed variable:
- Success criteria:
- Expected output:
- Stop/go rule:

### 3.2 Hyperparameter Tuning

- Target models:
- Search method:
- Search space:
- Validation metric:
- Early stopping:
- Success criteria:
- Expected output:

### 3.3 Threshold Policy

- Candidate policies:
- Cost assumptions:
- Recall target:
- Precision target:
- Success criteria:
- Expected output:

## 4. Medium-Priority Experiments

### 4.1 Preprocessing Variants

| Run Name | Clipping | Winsorization | Scaling | Notes |
|---|---|---|---|---|
| baseline | Yes | No | No | Existing setup |
| exp_winsor | Yes | Yes | No | Outlier-control variant |
| exp_robust | Yes | No | RobustScaler | Scale-robust variant |
| exp_winsor_robust | Yes | Yes | RobustScaler | Combined variant |

### 4.2 Feature Engineering

- Ratio interactions:
- Sector encoding:
- Lag features:
- Difference features:
- Rolling features:
- Leakage checks:

### 4.3 Ensemble Strategy

- Candidate models:
- Blending method:
- Stacking method:
- Validation requirement:
- Success criteria:

## 5. Low-Priority Experiments

### 5.1 Model Interpretation

- SHAP global importance:
- SHAP case studies:
- False-positive review:
- False-negative review:

### 5.2 Additional Model Families

- CatBoost:
- Logistic regression baseline:
- Calibration model:

## 6. Recommended Run Queue

| Priority | Run Name | Purpose | Success Metric | Output |
|---:|---|---|---|---|
| 1 | Not available | Not available | Not available | Not available |

## 7. Reporting Plan

- Single experiment report location:
- Comparison report location:
- Required tables:
- Required plots:
- Required case studies:

## 8. Risks and Controls

- Data leakage risk:
- Class imbalance risk:
- Threshold overfitting risk:
- Horizon selection bias:
- Sector distribution shift:

## 9. Final Recommendation

- Next command or pipeline:
- Expected decision after completion:
- Criteria for advancing to deployment-style evaluation:
