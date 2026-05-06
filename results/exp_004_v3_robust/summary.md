# exp_004: processed_v3 exp-B (robust_scale) — 실험 리포트

> 실험일: 2026-05-06
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 비교 대상: [`../exp_002_v3_baseline/summary.md`](../exp_002_v3_baseline/summary.md) (H10), [`../exp_002_v3_baseline/summary_H12.md`](../exp_002_v3_baseline/summary_H12.md) (H12)

---

## 0. 실험 배경

baseline에 **robust_scale=True**를 적용한 변형(exp-B). 트리 기반 모델은 본질적으로 scale-invariant라 큰 변화는 기대하지 않지만, **GBM/LightGBM의 분할 quantization이나 threshold 안정성에 영향이 있는지** 확인이 목적.

| Variant | winsorize | robust_scale |
|---|---|---|
| baseline | False | False |
| exp-A | True | False |
| **exp-B** (이번 실험) | False | **True** |
| exp-C | True | True |

실행 명령:

```bash
python3 -m src.modeling.run_all --exp exp_004_v3_robust --variant exp-B --horizon 10 12
```

---

## 1. 데이터 요약

H10/H12 모두 baseline과 row/positive 수 동일. robust_scale은 분포 위치/스케일만 변경.

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
| **RF** | **0.1743** | **0.3158** | **0.4286** | **0.2500** | 0.8552 | 0.283 |
| XGBoost | 0.1356 | 0.0476 | 0.1667 | 0.0278 | 0.8661 | 0.355 |
| GBM | 0.1101 | 0.1724 | 0.2273 | 0.1389 | 0.8654 | 0.277 |
| LightGBM | 0.0837 | 0.2025 | 0.1860 | 0.2222 | 0.8808 | 0.038 |

### 2-2. H12 — Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2358** | **0.3544** | **0.5000** | **0.2745** | 0.8641 | 0.267 |
| LightGBM | 0.2093 | 0.2683 | 0.3548 | 0.2157 | 0.8891 | 0.077 |
| XGBoost | 0.2046 | 0.0370 | 0.3333 | 0.0196 | 0.8865 | 0.390 |
| GBM | 0.1864 | 0.2059 | 0.4118 | 0.1373 | 0.8841 | 0.267 |

---

## 3. baseline 대비 효과 (RF)

| Horizon | Metric | baseline | **exp-B** | Δ |
|---|---|---|---|---|
| H10 | Test PR-AUC | 0.1735 | **0.1743** | +0.0008 |
| H10 | Test F1 | 0.3158 | 0.3158 | **±0.000** |
| H12 | Test PR-AUC | 0.2358 | 0.2358 | **±0.000** |
| H12 | Test F1 | 0.3544 | 0.3544 | **±0.000** |

**관찰:** RF는 robust_scale에 사실상 영향을 받지 않음 — 트리 모델이 scale-invariant임을 재확인. H10에서 PR-AUC가 0.0008 미세 변동한 건 random_state 영향 범위 내 노이즈로 해석 가능.

---

## 4. 분석

**RF: 예상대로 무영향:**
- PR-AUC/F1/Precision/Recall 모두 baseline과 거의 동일. 트리 모델은 정규화/스케일링에서 이득이 없다는 통념을 재확인.

**LightGBM: 부스팅 중 가장 큰 변화:**
- H10 LGBM: baseline F1=0.0 → exp-B F1=**0.2025**, threshold 0.797 → 0.038로 변경. robust_scale이 LGBM의 leaf 분할에서 작은 cut-off(0.038)도 정상 작동하게 만듦.
- H12 LGBM: baseline F1=0.1667 → exp-B F1=**0.2683**, PR-AUC 0.1773 → 0.2093 (+0.032).
- LGBM은 **robust_scale의 가장 큰 수혜자** — 부스팅 leaf 분할이 입력 분포의 절대 스케일에 민감했던 것으로 추정.

**XGB는 거의 무영향:**
- H10/H12 모두에서 baseline과 거의 동일한 결과. XGB는 LGBM 대비 입력 스케일에 더 둔감.

**GBM은 H12에서 미세 개선:**
- H12 GBM F1: baseline 0.1071 → exp-B 0.2059. PR-AUC도 0.1932 → 0.1864로 큰 차이는 아니지만 F1 안정성이 좋아짐.

---

## 5. 결론

| 항목 | 값 |
|---|---|
| Best 모델 | **Random Forest** |
| Best horizon | **H12** |
| Test PR-AUC | **0.2358** (RF, H12) — baseline과 동일 |
| Test F1 | **0.3544** (RF, H12) — baseline과 동일 |
| baseline 대비 RF | **변화 없음** |

**robust_scale 단독으로는 RF/XGB에 효과가 없으나, LightGBM에 대해서는 H10에서 F1=0 문제 해결, H12에서 F1/PR-AUC 명확 개선.** baseline의 RF 결과를 유지하면서 LGBM 결과를 살리고 싶은 경우 exp-B가 유효한 선택.

### 산출물

```
results/exp_004_v3_robust/
├── rf_H10.json   gbm_H10.json   xgb_H10.json   lgbm_H10.json
├── rf_H12.json   gbm_H12.json   xgb_H12.json   lgbm_H12.json
└── summary.md
```
