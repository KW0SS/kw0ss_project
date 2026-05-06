# exp_005: processed_v3 exp-C (winsorize + robust_scale) — 실험 리포트

> 실험일: 2026-05-06
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 비교 대상: [`../exp_002_v3_baseline/summary.md`](../exp_002_v3_baseline/summary.md), [`../exp_003_v3_winsor/summary.md`](../exp_003_v3_winsor/summary.md), [`../exp_004_v3_robust/summary.md`](../exp_004_v3_robust/summary.md)

---

## 0. 실험 배경

**winsorize와 robust_scale을 동시 적용**한 변형(exp-C). exp-A(winsor만)와 exp-B(robust만)의 효과가 결합될 때 추가 이득이 있는지, 또는 두 전처리가 서로 상쇄/중복되는지 확인이 목적.

| Variant | winsorize | robust_scale |
|---|---|---|
| baseline | False | False |
| exp-A | True | False |
| exp-B | False | True |
| **exp-C** (이번 실험) | **True** | **True** |

실행 명령:

```bash
python3 -m src.modeling.run_all --exp exp_005_v3_winsor_robust --variant exp-C --horizon 10 12
```

---

## 1. 데이터 요약

H10/H12 모두 baseline과 row/positive 수 동일.

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
| **RF** | **0.1761** | **0.3103** | **0.4091** | **0.2500** | 0.8670 | 0.266 |
| XGBoost | 0.1317 | 0.0476 | 0.1667 | 0.0278 | 0.8789 | 0.327 |
| GBM | 0.1064 | 0.1356 | 0.1739 | 0.1111 | 0.8760 | 0.310 |
| LightGBM | 0.0888 | 0.2025 | 0.1860 | 0.2222 | 0.8957 | 0.038 |

### 2-2. H12 — Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2524** | 0.3415 | 0.4516 | 0.2745 | 0.8657 | 0.234 |
| GBM | 0.2211 | 0.2424 | 0.5333 | 0.1569 | 0.8803 | 0.360 |
| LightGBM | 0.2095 | 0.3030 | 0.3125 | 0.2941 | 0.8846 | 0.054 |
| XGBoost | 0.1951 | 0.0690 | 0.2857 | 0.0392 | 0.8891 | 0.333 |

---

## 3. baseline 대비 효과 (RF)

| Horizon | Metric | baseline | **exp-C** | Δ |
|---|---|---|---|---|
| H10 | Test PR-AUC | 0.1735 | **0.1761** | **+0.0026** |
| H10 | Test F1 | 0.3158 | 0.3103 | -0.0055 |
| H12 | Test PR-AUC | 0.2358 | **0.2524** | **+0.0166** |
| H12 | Test F1 | 0.3544 | 0.3415 | -0.0129 |

**exp-A(winsor only)와 거의 동일한 수치.** robust_scale이 RF에서 무영향이라는 사실(exp-B에서 확인)이 재확인됨 — exp-C의 효과는 사실상 winsorize 단독 효과와 같음.

---

## 4. exp-A/B/C 결과 비교 (RF, Test set)

### H10

| Variant | PR-AUC | F1 | Precision |
|---|---|---|---|
| baseline | 0.1735 | **0.3158** | **0.4286** |
| exp-A (winsor) | 0.1762 | 0.3103 | 0.4091 |
| exp-B (robust) | 0.1743 | 0.3158 | 0.4286 |
| **exp-C (둘 다)** | **0.1761** | 0.3103 | 0.4091 |

### H12

| Variant | PR-AUC | F1 | Precision |
|---|---|---|---|
| baseline | 0.2358 | **0.3544** | **0.5000** |
| **exp-A (winsor)** | **0.2524** | 0.3415 | 0.4516 |
| exp-B (robust) | 0.2358 | **0.3544** | **0.5000** |
| **exp-C (둘 다)** | **0.2524** | 0.3415 | 0.4516 |

**핵심 패턴:**
- exp-A ≈ exp-C, exp-B ≈ baseline (RF 기준).
- robust_scale은 RF에 무영향, winsorize만 PR-AUC를 올림. 둘을 결합해도 winsorize 효과만 살아남음.
- F1/Precision은 baseline > exp-A=exp-C — winsorize가 극단 양성 시그널을 깎는 cost가 존재.

---

## 5. 분석

**부스팅 모델들의 H12에서의 활성화:**
- H12 GBM exp-C: F1 **0.2424**, PR-AUC **0.2211** — 4개 변형 중 GBM 최고. winsorize+robust 결합이 GBM의 leaf split을 가장 안정화.
- H12 LightGBM exp-C: F1 **0.3030**, PR-AUC 0.2095 — exp-A(F1=0.315), exp-B(F1=0.268)와 유사하게 F1 0.3 수준 유지.
- H10 LightGBM exp-C: F1 **0.2025** (exp-B와 동일) — robust_scale 효과로 F1=0 문제 해결 후 winsorize 추가는 무영향.

**XGBoost는 모든 변형에서 F1 붕괴:**
- H10/H12 모두에서 test F1이 0.04~0.07 수준 — winsorize/robust 어느 쪽도 XGB의 valid→test 일반화 gap을 해소하지 못함. XGB 별도 정규화/calibration 필요.

**전처리 결합의 비독립성:**
- exp-A + exp-B의 효과가 단순 합으로 나타나지 않음. RF는 winsorize만 살아남고, LGBM은 robust_scale이 더 큰 효과를 가지며 winsorize는 그 위에서 미세 조정하는 정도.

---

## 6. 종합 결론 (4개 변형 비교)

### 모델별 best variant (H12 기준)

| 모델 | Best variant | PR-AUC | F1 |
|---|---|---|---|
| RF | exp-A 또는 exp-C | **0.2524** | 0.3415 (exp-A/C) / **0.3544** (baseline/exp-B) |
| GBM | **exp-C** | **0.2211** | **0.2424** |
| XGBoost | baseline / exp-B | 0.2046 | 0.0370 (모두 비슷) |
| LightGBM | **exp-A** | **0.2136** | **0.3150** |

### 최종 권장

| 목적 | 추천 |
|---|---|
| **단일 best (PR-AUC 우선)** | **RF + exp-A (H12)** — PR-AUC 0.2524 |
| **단일 best (F1 우선)** | **RF + baseline (H12)** — F1 0.3544 |
| **모든 모델 균형** | **exp-C (H12)** — RF/GBM/LGBM 모두 안정적 |

### 다음 단계

1. **하이퍼파라미터 튜닝** — RF는 exp-A/H12, LGBM은 exp-A/H12, XGB는 별도 calibration 후 재평가.
2. **전 horizon sweep** — H6~H24에서 exp-A 적용 시 sweet spot 재탐색.
3. **XGB calibration** — Platt/Isotonic 또는 bootstrap 기반 threshold로 valid→test gap 해소.
4. **앙상블** — RF(exp-A) + LGBM(exp-A)의 H12 예측을 blending해 추가 이득 측정.

---

## 산출물

```
results/exp_005_v3_winsor_robust/
├── rf_H10.json   gbm_H10.json   xgb_H10.json   lgbm_H10.json
├── rf_H12.json   gbm_H12.json   xgb_H12.json   lgbm_H12.json
└── summary.md
```
