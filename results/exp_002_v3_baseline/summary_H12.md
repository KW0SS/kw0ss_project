# exp_002 (H12): processed_v3 baseline — H12 학습 리포트

> 실험일: 2026-05-06
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 관련 리포트: [`summary.md`](summary.md) (H10 baseline)

---

## 0. 실험 배경

[`summary.md`](summary.md)에서 검증한 H10 baseline에 이어 **H12 baseline**을 학습. 부도 예측 시계 12개월(H12)이 v3 데이터셋에서 H10보다 더 풍부한 학습 신호를 제공하는지, 그리고 RF baseline 우위 패턴이 horizon에 따라 유지되는지 확인하는 것이 목적.

### Variant 정의 (재게시)

| Variant | winsorize | robust_scale |
|---|---|---|
| **baseline** (이번 실험) | False | False |
| exp-A | True | False |
| exp-B | False | True |
| exp-C | True | True |

실행 명령:

```bash
python3 -m src.modeling.run_all --exp exp_002_v3_baseline --variant baseline --horizon 12
```

---

## 1. 데이터 요약 (H12 baseline)

| 항목 | H10 (참고) | **H12** |
|---|---|---|
| Features | 33개 | 33개 |
| Train | 31,360행 (pos=210) | 31,360행 (**pos=250**) |
| Valid | 5,050행 (pos=21) | 5,050행 (**pos=27**) |
| Test | 5,311행 (pos=36) | 5,311행 (**pos=51**) |
| Imbalance ratio | 148.3:1 | **124.4:1** |
| Train years | 2015–2022 | 2015–2022 |
| Valid year | 2023 | 2023 |
| Test year | 2024 | 2024 |
| Impute 후 잔존 NaN | 0 | 0 |

H12는 시계가 길어 더 많은 부도 라벨을 포착 — train +19%, valid +29%, test +42%. Imbalance ratio도 148.3 → 124.4로 명확히 완화.

---

## 2. 모델 결과

### 2-1. Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2358** | **0.3544** | **0.5000** | **0.2745** | 0.8641 | 0.267 |
| XGBoost | 0.2046 | 0.0370 | 0.3333 | 0.0196 | 0.8865 | 0.390 |
| GBM | 0.1932 | 0.1071 | 0.6000 | 0.0588 | 0.8880 | 0.573 |
| LightGBM | 0.1773 | 0.1667 | 0.2857 | 0.1176 | 0.8790 | 0.203 |

### 2-2. Valid set

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2378** | **0.2963** | 0.4000 | 0.2353 | 0.9299 | 0.267 |
| XGBoost | 0.1897 | 0.2857 | 0.5714 | 0.1905 | 0.9214 | 0.390 |
| LightGBM | 0.1891 | 0.2222 | 0.5000 | 0.1429 | 0.9107 | 0.203 |
| GBM | 0.1830 | 0.2857 | 0.4444 | 0.2105 | 0.9151 | 0.573 |

---

## 3. H10 vs H12 비교 (RF baseline)

| Metric | H10 | **H12** | Δ |
|---|---|---|---|
| Test PR-AUC | 0.1735 | **0.2358** | **+36%** |
| Test F1 | 0.3158 | **0.3544** | **+12%** |
| Test Precision | 0.4286 | **0.5000** | +0.071 |
| Test Recall | 0.2500 | 0.2745 | +0.025 |
| Test ROC-AUC | 0.8549 | **0.8641** | +0.009 |
| Threshold | 0.283 | 0.267 | -0.016 |

**H12 우위 원인 추정:**

1. **Positive 샘플 증가** — test pos 36 → 51 (+42%)로 평가 분산이 줄고 precision/recall 모두 안정.
2. **Imbalance 완화** — 148.3 → 124.4. RF가 minority class 시그널을 더 잘 학습할 수 있는 환경.
3. **시계 효과** — 12개월 전 재무비율이 부도 예측에 10개월보다 약간 더 강한 시그널을 갖는 것으로 보임 (이는 horizon sweep에서 sweet spot 탐색의 단서).

---

## 4. 분석

**RF 우위 패턴 H12에서도 재현:**
- exp_001/exp_002(H10)와 동일하게 RF가 PR-AUC/F1 모두 1위. **부스팅 모델은 H12에서도 test F1이 매우 낮음** (XGB 0.037, GBM 0.107) — threshold 0.39/0.57이 보수적이라 generalization 실패.
- LightGBM은 H10에서 test F1=0이었지만 H12에서는 0.167로 완화 — positive 수 증가로 threshold 0.203이 의미 있는 cut-off가 됨.

**LightGBM threshold 행동의 horizon 의존성:**
- H10 valid threshold 0.797 (positive 1개만 잡는 극단), H12 valid threshold 0.203 (적정 위치)
- valid positive 수(21 → 27)에 따라 threshold가 안정화되는 경향 — horizon이 길수록 극단 threshold 위험 감소.

**XGBoost의 F1 붕괴 지속:**
- H10/H12 모두에서 valid F1≈0.286인데 test F1이 0.04~0.05까지 떨어짐 — early stopping이 valid set 작은 positive(21~27)에서 과적합. threshold 안정화 또는 별도의 calibration이 필요.

---

## 5. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| Best 모델 | **Random Forest** |
| Variant | baseline (winsorize/robust_scale 모두 False) |
| Test PR-AUC (H12) | **0.2358** |
| Test F1 (H12) | **0.3544** |
| H10 대비 | PR-AUC +36%, F1 +12% |

**v3 데이터셋에서 H12 baseline이 H10 baseline을 모든 핵심 지표에서 상회.** RF baseline 전략이 horizon 12개월에서도 유효함을 확인했고, exp_001 → exp_002(H10) → exp_002(H12)로 점진적 개선 추세가 이어짐.

### 다음 단계

1. ~~**변형 비교 (exp-A/B/C)**~~ — 이번 추가 실험에서 동시 진행. 결과는 [`../exp_003_v3_winsor/summary.md`](../exp_003_v3_winsor/summary.md), [`../exp_004_v3_robust/summary.md`](../exp_004_v3_robust/summary.md), [`../exp_005_v3_winsor_robust/summary.md`](../exp_005_v3_winsor_robust/summary.md) 참조.
2. **전 horizon 확장** — H6~H24 sweep으로 H12가 진짜 sweet spot인지 검증.
3. **하이퍼파라미터 튜닝** — RF/XGB/LGBM Optuna 적용.
4. **부스팅 모델 threshold 안정화** — XGB/GBM/LGBM의 valid→test gap을 줄이기 위한 calibration (Platt/Isotonic) 또는 bootstrap CI 기반 threshold.

---

## 산출물 목록

```
results/exp_002_v3_baseline/
├── rf_H10.json          # H10 RF
├── gbm_H10.json
├── xgb_H10.json
├── lgbm_H10.json
├── rf_H12.json          # H12 RF (best)
├── gbm_H12.json
├── xgb_H12.json
├── lgbm_H12.json
├── summary.md           # H10 리포트
└── summary_H12.md       # 이 리포트
```
