# exp_003: processed_v3 exp-A (winsorize) — 실험 리포트

> 실험일: 2026-05-06
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 비교 대상: [`../exp_002_v3_baseline/summary.md`](../exp_002_v3_baseline/summary.md) (H10), [`../exp_002_v3_baseline/summary_H12.md`](../exp_002_v3_baseline/summary_H12.md) (H12)

---

## 0. 실험 배경

baseline에 **winsorize=True**를 적용한 변형(exp-A)에서 모델 성능 변화를 측정. 재무비율 분포의 극단 outlier(상·하위 1%)를 클리핑하면 트리 모델이 더 안정적인 split을 학습하는지가 핵심 질문.

| Variant | winsorize | robust_scale |
|---|---|---|
| baseline | False | False |
| **exp-A** (이번 실험) | **True** | False |
| exp-B | False | True |
| exp-C | True | True |

실행 명령:

```bash
python3 -m src.modeling.run_all --exp exp_003_v3_winsor --variant exp-A --horizon 10 12
```

---

## 1. 데이터 요약

H10/H12 모두 row 수, positive 수, imbalance ratio는 baseline과 동일 (winsorize는 분포만 변경, 행 수 불변).

| 항목 | H10 | H12 |
|---|---|---|
| Train | 31,360 (pos=210) | 31,360 (pos=250) |
| Valid | 5,050 (pos=21) | 5,050 (pos=27) |
| Test | 5,311 (pos=36) | 5,311 (pos=51) |
| Imbalance ratio | 148.3 | 124.4 |

---

## 2. 모델 결과

### 2-1. H10 — Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.1762** | **0.3103** | **0.4091** | **0.2500** | 0.8664 | 0.267 |
| XGBoost | 0.1317 | 0.0476 | 0.1667 | 0.0278 | 0.8789 | 0.327 |
| GBM | 0.1069 | 0.1639 | 0.2000 | 0.1389 | 0.8776 | 0.271 |
| LightGBM | 0.0919 | 0.0000 | 0.0000 | 0.0000 | 0.8846 | 0.478 |

### 2-2. H12 — Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2524** | 0.3415 | 0.4516 | 0.2745 | 0.8656 | 0.234 |
| LightGBM | 0.2136 | 0.3150 | 0.2632 | 0.3922 | 0.8885 | 0.055 |
| GBM | 0.2049 | 0.1379 | 0.5714 | 0.0784 | 0.8820 | 0.554 |
| XGBoost | 0.1951 | 0.0690 | 0.2857 | 0.0392 | 0.8891 | 0.333 |

---

## 3. baseline 대비 효과 (RF)

| Horizon | Metric | baseline | **exp-A** | Δ |
|---|---|---|---|---|
| H10 | Test PR-AUC | 0.1735 | **0.1762** | **+0.0027** |
| H10 | Test F1 | 0.3158 | 0.3103 | -0.0055 |
| H10 | Test Precision | 0.4286 | 0.4091 | -0.020 |
| H12 | Test PR-AUC | 0.2358 | **0.2524** | **+0.0166** |
| H12 | Test F1 | 0.3544 | 0.3415 | -0.0129 |
| H12 | Test Precision | 0.5000 | 0.4516 | -0.048 |

**관찰:**
- **H12에서 RF PR-AUC +1.66%p 개선** — winsorize가 H12 데이터에서 outlier 노이즈 억제로 효과를 봄.
- F1/Precision은 미세 하락 — winsorize가 극단 양성 샘플의 결정적 시그널을 같이 깎아 cut-off 부근의 예측 정확도가 약간 저하.
- H10은 거의 영향 없음 — H10의 positive 분포가 이미 안정적이라 winsorize 추가 효과 작음.

---

## 4. 분석

**RF 1위 유지, 부스팅의 LightGBM H12에서 각성:**
- LightGBM H12 PR-AUC 0.2136, F1 **0.3150** — H12 baseline에서 0.1773/0.1667이었던 것 대비 큰 폭 개선. winsorize가 LGBM의 leaf split 안정성을 높여 threshold 0.055가 의미 있는 cut-off로 작동.
- 반대로 H10 LGBM은 baseline F1=0.0이었던 문제가 exp-A에서도 그대로 (test F1=0). LGBM의 H10 보수성은 winsorize로 해결되지 않음.

**winsorize의 horizon 의존성:**
- H12에서 효과가 크고 H10에서 작은 이유: H12는 시계가 길어 재무비율의 자연 변동 폭이 크고, 그만큼 outlier도 많아 winsorize 이득이 큼.

**XGB/GBM의 보수성은 변하지 않음:**
- XGB H10 F1=0.048, H12 F1=0.069 — baseline과 사실상 동일. winsorize는 부스팅 모델의 valid→test 일반화 gap 문제를 해결하지 못함.

---

## 5. 결론

| 항목 | 값 |
|---|---|
| Best 모델 | **Random Forest** |
| Best horizon | **H12** |
| Test PR-AUC | **0.2524** (RF, H12) |
| Test F1 | **0.3415** (RF, H12) |
| baseline H12 RF 대비 | PR-AUC **+0.017**, F1 -0.013 |

**winsorize는 H12에서 PR-AUC를 명확히 개선하지만 F1은 미세 하락.** PR-AUC 우선이면 exp-A 채택, F1/Precision 우선이면 baseline 유지가 합리적. LightGBM은 winsorize로 가장 큰 수혜를 받음.

### 산출물

```
results/exp_003_v3_winsor/
├── rf_H10.json   gbm_H10.json   xgb_H10.json   lgbm_H10.json
├── rf_H12.json   gbm_H12.json   xgb_H12.json   lgbm_H12.json
└── summary.md
```
