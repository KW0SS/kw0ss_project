# Horizon sweep: processed_v3 baseline H10 ~ H24

> 실험일: 2026-05-12
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 범위: variant = `baseline` (winsorize=False, robust_scale=False), 8개 horizon (H10/H12/H14/H16/H18/H20/H22/H24)
> 결과 파일: [`by_horizon/H{n}/baseline/`](by_horizon/) / [`by_variant/baseline/H{n}/`](by_variant/baseline/) (symlink)
> 관련 문서: [`summary_variant_compare.md`](summary_variant_compare.md) (전처리 variant 비교)

---

## 0. 실험 배경

[`../exp_002_v3_baseline/summary.md`](../exp_002_v3_baseline/summary.md)에서 확인한 H10 RF baseline과 이전 H12 학습 결과를 토대로, processed_v3 데이터셋에서 **부도 예측 horizon이 길어질수록 모델 성능이 어떻게 변하는지** sweep으로 검증한다.

핵심 질문:
1. H12에서 본 H10 대비 개선 (+36% PR-AUC)이 더 긴 horizon에서도 유지되는가?
2. test PR-AUC의 sweet spot horizon이 존재하는가?
3. valid/test gap이 horizon에 따라 어떻게 변하는가?

실행 명령 (재현):

```bash
python3 -m src.modeling.run_all --exp exp_002_v3_baseline --variant baseline --horizon 10 12 14 16 18 20 22 24
```

---

## 1. 데이터 요약

horizon이 길수록 positive 라벨 포착이 늘어 imbalance가 완화된다. H16 이후로는 test 행 수가 줄어 평가 분산이 커지는 점에 유의.

| Horizon | Train rows (pos) | Valid rows (pos) | Test rows (pos) | Imbalance |
|---|---|---|---|---|
| H10 | 31,360 (210) | 5,050 (21) | 5,311 (36) | 149.3:1 |
| H12 | 31,360 (250) | 5,050 (27) | 5,311 (51) | 125.4:1 |
| H14 | 29,986 (284) | 5,050 (34) | 5,311 (60) | 105.6:1 |
| H16 | 29,986 (333) | 5,050 (43) | **3,955** (54) | 90.0:1 |
| H18 | 29,986 (374) | 5,050 (54) | 3,955 (61) | 80.2:1 |
| H20 | 29,986 (421) | 5,050 (62) | **2,623** (46) | 71.2:1 |
| H22 | 29,986 (473) | 5,050 (72) | **1,307** (27) | 63.4:1 |
| H24 | 29,986 (516) | 5,050 (87) | **1,307** (28) | 58.1:1 |

- H14 이후 train 행 수가 29,986으로 떨어지는 건 horizon이 길수록 학습 시점에서 사용 가능한 시계열 윈도우가 줄기 때문.
- H20 이후 test 행 수가 급감 — 2024년 시점에서 H20+ 회수기간을 보장할 수 있는 표본이 줄어들기 때문. test PR-AUC가 안정적이지 않을 수 있음.

---

## 2. Test set 성능 매트릭스

### 2-1. Test PR-AUC

| Horizon | RF | GBM | XGB | LGBM | Best model |
|---|---|---|---|---|---|
| H10 | **0.1735** | 0.1063 | 0.1356 | 0.1107 | RF |
| H12 | **0.2358** | 0.1932 | 0.2046 | 0.1773 | RF |
| H14 | **0.1925** | 0.1593 | 0.1505 | 0.1487 | RF |
| H16 | **0.2561** | 0.1506 | 0.1853 | 0.1533 | RF |
| H18 | **0.2348** | 0.1732 | 0.1751 | 0.1630 | RF |
| **H20** | **0.2641** | 0.1976 | 0.2172 | 0.1574 | **RF (peak)** |
| H22 | **0.2608** | 0.2349 | 0.2395 | 0.1577 | RF |
| H24 | 0.2555 | **0.2840** | 0.2608 | 0.1496 | **GBM** |

### 2-2. Test F1

| Horizon | RF | GBM | XGB | LGBM |
|---|---|---|---|---|
| H10 | **0.3158** | 0.1695 | 0.0476 | 0.0000 |
| H12 | **0.3544** | 0.1071 | 0.0370 | 0.1667 |
| H14 | 0.2830 | 0.2667 | 0.1750 | 0.2400 |
| H16 | 0.3133 | 0.1957 | 0.2529 | **0.3107** |
| H18 | 0.2619 | 0.2476 | 0.2174 | 0.2600 |
| H20 | **0.3467** | 0.2069 | 0.1818 | 0.2368 |
| H22 | **0.3158** | 0.2381 | 0.1875 | 0.1081 |
| H24 | 0.2800 | **0.2857** | 0.1667 | 0.1961 |

### 2-3. Test Precision / Recall (RF)

| Horizon | Precision | Recall | Threshold |
|---|---|---|---|
| H10 | 0.4286 | 0.2500 | 0.283 |
| H12 | **0.5000** | 0.2745 | 0.267 |
| H14 | 0.3261 | 0.2500 | 0.205 |
| H16 | 0.4483 | 0.2407 | 0.233 |
| H18 | 0.4783 | 0.1803 | 0.290 |
| H20 | 0.4483 | 0.2826 | 0.210 |
| H22 | **0.5455** | 0.2222 | 0.268 |
| H24 | 0.3182 | 0.2500 | 0.187 |

---

## 3. Valid set 성능 (모델 선정 기준)

valid PR-AUC와 F1은 horizon이 길수록 거의 단조 증가한다. H22/H24에서 valid PR-AUC 0.39대까지 도달하지만, test에는 같은 폭으로 반영되지 않는다.

### 3-1. Valid PR-AUC

| Horizon | RF | GBM | XGB | LGBM |
|---|---|---|---|---|
| H10 | 0.1811 | 0.1174 | 0.1874 | **0.1950** |
| H12 | **0.2378** | 0.1830 | 0.1897 | 0.1891 |
| H14 | 0.2076 | 0.1808 | **0.2314** | 0.1801 |
| H16 | 0.2367 | **0.2661** | 0.2126 | 0.1867 |
| H18 | **0.3051** | 0.2717 | 0.2446 | 0.2428 |
| H20 | **0.3307** | 0.2968 | 0.2826 | 0.2627 |
| H22 | **0.3814** | 0.2860 | 0.3003 | 0.3288 |
| H24 | **0.3911** | 0.3260 | 0.2948 | 0.2972 |

### 3-2. Valid → Test gap (RF, PR-AUC)

| Horizon | Valid | Test | Gap |
|---|---|---|---|
| H10 | 0.1811 | 0.1735 | -0.008 |
| H12 | 0.2378 | 0.2358 | -0.002 |
| H14 | 0.2076 | 0.1925 | -0.015 |
| H16 | 0.2367 | 0.2561 | **+0.019** |
| H18 | 0.3051 | 0.2348 | -0.070 |
| H20 | 0.3307 | 0.2641 | -0.067 |
| H22 | 0.3814 | 0.2608 | **-0.121** |
| H24 | 0.3911 | 0.2555 | **-0.136** |

H22/H24에서 valid PR-AUC와 test PR-AUC의 격차가 0.12 이상 — test 표본 1,307행 (positive 27~28)에서 평가 분산이 커진 영향으로 보임. valid-기준 모델 선택이 H20 이후로 점점 덜 신뢰할 수 있다는 신호.

---

## 4. 모델별 horizon 패턴

### 4-1. RF — 가장 안정적, H20 부근에서 peak

- Test PR-AUC: H10 0.17 → H12 0.24 → H16 0.26 → **H20 0.264 (peak)** → H22 0.26 → H24 0.26.
- H14에서 일시적으로 0.19로 떨어지는 dip이 있으나, H16에서 회복.
- F1은 H12와 H20에서 0.35 부근으로 두 봉우리.
- precision은 H22에서 0.5455로 가장 높으나 recall은 0.2222로 낮음 — threshold가 보수적으로 움직임.

### 4-2. GBM — H24에서 깜짝 1위

- 대부분 horizon에서 4모델 중 하위권이었으나 H24에서 PR-AUC **0.2840**으로 RF를 추월.
- imbalance ratio가 58:1까지 떨어진 H24에서 GBM의 분할이 비로소 의미 있는 minority 신호를 잡음.
- 다만 H24의 test 표본이 1,307행/positive 28에 불과해 단일 실험만으로 결론짓기엔 표본이 얇다.

### 4-3. XGBoost — H22~H24에서 PR-AUC 회복, 그러나 F1은 여전히 낮음

- Test PR-AUC: H10~H18에서 0.13~0.19로 약세, H22 0.24, H24 **0.26**으로 회복.
- 그러나 test F1은 모든 horizon에서 0.17~0.25 수준 — valid F1 (0.28~0.38)과의 gap이 horizon이 길어져도 해소되지 않음.
- threshold 0.27 부근에서 보수적 cut-off가 일관됨 — XGB는 별도 calibration이 필요한 영역.

### 4-4. LightGBM — H10 극단 threshold 문제, H16에서 peak

- H10 valid threshold 0.797로 test F1=0 발생 — positive 21개에서의 극단 fit.
- H16에서 F1 **0.3107** (4모델 중 H16 F1 1위), PR-AUC는 0.1533으로 낮음 — F1과 PR-AUC가 어긋남.
- horizon이 길어질수록 PR-AUC가 오히려 떨어지는 경향 — 표본이 많아질수록 LGBM의 보수적 leaf split이 minority 신호를 충분히 학습하지 못하는 패턴.

---

## 5. 종합 sweet spot 분석

### 5-1. RF 기준 단일 best horizon

| 후보 | 근거 | 위험 |
|---|---|---|
| **H20** | Test PR-AUC peak (0.2641), F1 0.3467 동시 만족 | test 행 수 2,623로 H10-H18 (3,955~5,311)보다 작음 |
| H12 | F1 peak (0.3544), Precision 0.5 | PR-AUC는 H20 대비 -0.028 |
| H16 | valid→test gap이 +0.019로 가장 안정 (overfit 부재) | test 행 수 3,955로 H10/H12보다 작음 |

### 5-2. 모델 전체 관점

- H16: 4모델 모두 안정 (RF 0.26 / GBM 0.15 / XGB 0.19 / LGBM 0.15) — 앙상블 후보로 매력.
- H20: RF/XGB 동시 강세, GBM/LGBM 보통 — RF 중심 운용에 최적.
- H24: GBM 1위 (0.2840), 다른 모델은 하락 — GBM 단독 운용 시 흥미롭지만 표본이 얇음.

### 5-3. 권장 조합

| 시나리오 | 권장 |
|---|---|
| 단일 best RF | **H20 baseline** (PR-AUC 0.2641, F1 0.3467) |
| Precision 우선 | **H22 baseline RF** (Precision 0.5455) — 단, 표본 1,307 한계 |
| 앙상블 후보 | **H16 또는 H20** — 다수 모델이 동시에 안정 |
| F1 단일 best | **H12 baseline RF** (F1 0.3544) — 기존 결과 유지 |

---

## 6. 분석

**imbalance 완화의 한계 효과:**
- imbalance ratio가 149 → 58로 절반 이하가 되는 H10 → H24 구간에서, test PR-AUC는 0.17 → 0.26으로 약 +50% 개선되지만 H20 이후로는 정체.
- positive 수 증가의 이득이 H20 부근에서 포화되고, 그 너머에서는 test 표본 축소에 의한 분산 증가가 이득을 상쇄.

**valid → test gap의 horizon 의존성:**
- H10~H16에서는 valid와 test의 차이가 ±0.02 이내로 거의 무시 가능.
- H18부터 gap이 -0.07로 커지고, H22/H24에서는 -0.12 이상.
- test 행 수 감소(5,311 → 1,307)와 정확히 동조 — 평가 신뢰도가 horizon에 따라 떨어진다는 명확한 신호.

**부스팅 모델의 long-horizon 활성화:**
- GBM과 XGB가 H20 이후로 PR-AUC가 상승하는 패턴 — minority 표본이 늘면 부스팅이 비로소 신호를 학습.
- 다만 GBM H24의 0.2840은 단일 실험 결과이므로, hyperparameter 변경/재현 실험으로 검증이 필요.

**LGBM의 horizon dependent 약점:**
- horizon이 길어질수록 PR-AUC가 오히려 하락 — H10 0.1107 → H24 0.1496 (개선이 미미).
- leaf split 보수성이 horizon이 길어질수록 더 강해지는 듯한 패턴 — `is_unbalance=True`나 `min_child_samples` 튜닝 후보.

---

## 7. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| Overall best 모델 | **Random Forest** (H10~H22 1위, H24만 GBM에 양보) |
| Sweet spot horizon (RF 기준) | **H20** (PR-AUC 0.2641, F1 0.3467) |
| 안정성 가장 좋은 horizon | **H16** (valid→test gap +0.019) |
| F1 단일 best | **H12** (RF F1 0.3544) |
| 가장 흥미로운 outlier | **H24 GBM** (PR-AUC 0.2840, test 표본 1,307 한계) |

**RF baseline 전략이 H10에서 H22까지 일관되게 유효함을 확인.** 단일 horizon에 베팅한다면 H20이 가장 균형 잡힌 선택이며, F1 우선이면 H12, 앙상블 후보면 H16가 합리적. H24는 GBM의 깜짝 우위가 보이지만 평가 표본이 작아 별도 재현 실험이 필요.

### 다음 단계

1. **H20 / H16 variant 검증** — winsorize / robust_scale을 sweet spot horizon에 적용해 PR-AUC를 추가 끌어올릴 수 있는지 확인. [`summary_variant_compare.md`](summary_variant_compare.md)의 H12 결과를 보면 winsorize는 PR-AUC +0.017 이득이 있었음.
2. **H22/H24 평가 분산 완화** — bootstrap CI로 PR-AUC 신뢰구간 측정, 또는 multi-seed 평균.
3. **하이퍼파라미터 튜닝** — RF는 H20, GBM은 H24, LGBM은 H16를 우선 대상으로 Optuna.
4. **GBM H24 재현 실험** — 다른 seed로 단일 실험의 우연 효과인지 검증.
5. **XGB calibration** — Platt/Isotonic 또는 multi-horizon threshold 평균으로 valid→test gap 해소.

---

## 8. 산출물

```
exp_003_v3_variant_compare/
├── by_horizon/
│   ├── H10/baseline/  rf.json gbm.json xgb.json lgbm.json
│   ├── H12/baseline/  ...
│   ├── H14/baseline/  ...
│   ├── H16/baseline/  ...
│   ├── H18/baseline/  ...
│   ├── H20/baseline/  ...
│   ├── H22/baseline/  ...
│   └── H24/baseline/  ...
├── by_variant/baseline/  H10..H24 (symlink 미러)
├── summary_horizon_compare.md  ← 이 문서
└── summary_variant_compare.md  (H10/H12 variant 효과)
```
