---
name: ml-training-strategy
description: Design English machine-learning training strategies for financial-risk or delisting-prediction experiments, including horizon planning, preprocessing variants, threshold policy, tuning, and validation order.
---

Use this skill when the user asks how to plan ML experiments, choose next training runs, compare horizons, design preprocessing variants, tune models, or create a staged training roadmap.

Default language: English.
If the user asks for Korean, write the strategy in Korean while preserving the same structure.

## Required Workflow

1. Identify the current baseline experiment.
2. Identify the decision target: better AUC, better recall, better precision, earlier warning horizon, or operational thresholding.
3. Separate experiments into high, medium, and low priority.
4. Keep one main variable changed per experiment group when possible.
5. Define success criteria before proposing runs.
6. Include expected trade-offs and required artifacts.

## Strategy Principles

- Start from a reproducible baseline.
- Expand horizons before expensive tuning if horizon choice is still uncertain.
- Tune only the most promising horizon/model combinations.
- Evaluate threshold policies after ranking quality is acceptable.
- Compare preprocessing variants under the same data split.
- Use feature engineering only after baseline and horizon behavior are understood.
- Treat SHAP and case studies as interpretation steps after model selection.

## Recommended Priority Order

1. Baseline validation
2. Horizon sweep
3. Preprocessing comparison
4. Hyperparameter tuning
5. Threshold policy
6. Feature engineering
7. Ensemble or stacking
8. Model interpretation

## Output Style

- Be practical and experiment-oriented.
- Use clear run names.
- Include expected outputs.
- Include stop/go criteria.
- Avoid broad research suggestions that do not map to executable runs.
