# exp_003 (variant compare): processed_v3 baseline vs winsor / robust / winsor+robust

> 실험일: 2026-05-12
> 브랜치: `26-model-retrain-on-eda-refined-dataset-v011`
> 통합 대상 (기존): `exp_002_v3_baseline` (H12 부분), `exp_003_v3_winsor`, `exp_004_v3_robust`, `exp_005_v3_winsor_robust`
> 결과 파일: [`by_horizon/`](by_horizon/), [`by_variant/`](by_variant/) (양쪽은 동일한 JSON을 두 가지 뷰로 제공, by_variant는 symlink)

---

## 0. 실험 배경

processed_v3 데이터셋에서 전처리 변형이 트리 기반 모델의 부도 예측 성능에 어떤 영향을 주는지 확인하기 위해 4개 variant를 동일 모델 구성으로 학습했다.

| Variant | winsorize | robust_scale | 비고 |
|---|---|---|---|
| **baseline** | False | False | 기준 |
| **exp-A** | True | False | 재무비율 극단 1% 클리핑 |
| **exp-B** | False | True | 중앙값/IQR 기반 스케일링 |
| **exp-C** | True | True | 두 처리 모두 적용 |

이 문서는 H10/H12 두 horizon에서의 variant 효과를 통합 비교한다. baseline H10-H24의 horizon sweep은 별도 문서 [`summary_horizon_compare.md`](summary_horizon_compare.md) 참조.

실행 명령 (재현):

```bash
for v in baseline exp-A exp-B exp-C; do
  exp=exp_002_v3_baseline; [[ $v == "exp-A" ]] && exp=exp_003_v3_winsor
  [[ $v == "exp-B" ]] && exp=exp_004_v3_robust
  [[ $v == "exp-C" ]] && exp=exp_005_v3_winsor_robust
  python3 -m src.modeling.run_all --exp $exp --variant $v --horizon 10 12
done
```

---

## 1. 데이터 요약

모든 variant에서 행 수와 positive 수는 동일 (winsorize/robust_scale은 분포만 변경, 행 수 불변).

| 항목 | H10 | H12 |
|---|---|---|
| Features | 33 | 33 |
| Train | 31,360 (pos=210) | 31,360 (pos=250) |
| Valid | 5,050 (pos=21) | 5,050 (pos=27) |
| Test | 5,311 (pos=36) | 5,311 (pos=51) |
| Imbalance ratio | 148.3:1 | 124.4:1 |
| Train / Valid / Test 연도 | 2015–2022 / 2023 / 2024 | 동일 |

---

## 2. Test 성능 — variant × model 매트릭스

### 2-1. H10 Test PR-AUC

| Model | baseline | exp-A (winsor) | exp-B (robust) | exp-C (둘 다) | Best |
|---|---|---|---|---|---|
| **RF** | 0.1735 | **0.1762** | 0.1743 | 0.1761 | exp-A |
| XGBoost | 0.1356 | 0.1317 | 0.1356 | 0.1317 | baseline=B |
| GBM | 0.1063 | 0.1069 | 0.1101 | 0.1064 | exp-B |
| LightGBM | 0.1107 | 0.0919 | 0.0837 | 0.0888 | baseline |

### 2-2. H10 Test F1

| Model | baseline | exp-A | exp-B | exp-C | Best |
|---|---|---|---|---|---|
| **RF** | **0.3158** | 0.3103 | **0.3158** | 0.3103 | baseline=B |
| XGBoost | 0.0476 | 0.0476 | 0.0476 | 0.0476 | tie |
| GBM | 0.1695 | 0.1639 | 0.1724 | 0.1356 | exp-B |
| LightGBM | 0.0000 | 0.0000 | **0.2025** | **0.2025** | exp-B=C |

### 2-3. H12 Test PR-AUC

| Model | baseline | exp-A | exp-B | exp-C | Best |
|---|---|---|---|---|---|
| **RF** | 0.2358 | **0.2524** | 0.2358 | **0.2524** | exp-A=C |
| GBM | 0.1932 | 0.2049 | 0.1864 | **0.2211** | exp-C |
| LightGBM | 0.1773 | **0.2136** | 0.2093 | 0.2095 | exp-A |
| XGBoost | 0.2046 | 0.1951 | 0.2046 | 0.1951 | baseline=B |

### 2-4. H12 Test F1

| Model | baseline | exp-A | exp-B | exp-C | Best |
|---|---|---|---|---|---|
| **RF** | **0.3544** | 0.3415 | **0.3544** | 0.3415 | baseline=B |
| GBM | 0.1071 | 0.1379 | 0.2059 | **0.2424** | exp-C |
| LightGBM | 0.1667 | **0.3150** | 0.2683 | 0.3030 | exp-A |
| XGBoost | 0.0370 | 0.0690 | 0.0370 | 0.0690 | exp-A=C |

---

## 3. RF baseline 대비 효과 (PR-AUC / F1)

| Horizon | Metric | baseline | exp-A | exp-B | exp-C |
|---|---|---|---|---|---|
| H10 | PR-AUC | 0.1735 | +0.0027 | +0.0008 | +0.0026 |
| H10 | F1 | 0.3158 | -0.0055 | ±0.0000 | -0.0055 |
| H12 | PR-AUC | 0.2358 | **+0.0166** | ±0.0000 | **+0.0166** |
| H12 | F1 | 0.3544 | -0.0129 | ±0.0000 | -0.0129 |

**핵심 패턴:**
- RF는 robust_scale 단독(exp-B)에는 사실상 무영향 — 트리 모델의 scale-invariance 재확인.
- winsorize가 들어간 exp-A / exp-C는 동일한 효과 (RF에서 둘은 사실상 같은 결과). H12 PR-AUC를 +1.66%p 끌어올리지만 F1은 미세 하락.
- H10에서는 어떤 variant도 의미 있는 변화를 만들지 못함 — 시계가 짧을 때 outlier 영향이 상대적으로 작기 때문으로 추정.

---

## 4. 모델별 variant 민감도 요약

### 4-1. RF — scale-invariant, winsorize만 약효

- robust_scale 단독: 무영향
- winsorize: H12에서 PR-AUC +0.017 / F1 -0.013 — PR-AUC와 F1이 반대로 움직이므로 목표 지표에 따라 선택
- 결합: 합산 효과 없고 winsorize 단독과 같음

### 4-2. LightGBM — 가장 큰 수혜자

- H10 baseline에서 test F1=0 (threshold 0.797로 과보수적). robust_scale 적용 시 threshold가 0.038로 내려가며 F1=0.2025 회복.
- H12에서는 winsorize(exp-A)가 가장 강한 효과: PR-AUC 0.1773 → **0.2136**, F1 0.1667 → **0.3150**.
- LGBM의 leaf split이 입력 분포의 절대 스케일과 outlier에 민감하다는 신호.

### 4-3. GBM — 결합이 최선

- 단독 효과는 작지만 H12 exp-C에서 PR-AUC 0.2211, F1 0.2424로 4개 variant 중 GBM 최고.
- winsorize와 robust_scale 결합이 GBM의 분할 안정성을 끌어올림.

### 4-4. XGBoost — 어떤 변형도 효과 없음

- H10/H12 모두에서 test F1 0.04~0.07 수준 정체.
- valid F1 (0.286 부근)과 test F1 (0.04~0.07)의 generalization gap이 핵심 문제 — 전처리가 아닌 calibration이나 threshold 안정화가 필요한 영역.

---

## 5. 모델별 best variant 권장 (H12 기준)

| 모델 | Best variant | Test PR-AUC | Test F1 | 비고 |
|---|---|---|---|---|
| **RF** (overall best) | exp-A (PR-AUC) / baseline (F1) | 0.2524 / 0.2358 | 0.3415 / 0.3544 | trade-off |
| **GBM** | exp-C | 0.2211 | 0.2424 | 결합 시 최선 |
| **LightGBM** | exp-A | 0.2136 | 0.3150 | winsorize가 결정적 |
| **XGBoost** | baseline | 0.2046 | 0.0370 | calibration 필요 |

### 시나리오별 최종 선택

| 시나리오 | 권장 조합 |
|---|---|
| 단일 best (PR-AUC 우선) | **RF + exp-A, H12** — PR-AUC 0.2524 |
| 단일 best (F1 우선) | **RF + baseline, H12** — F1 0.3544 |
| 모델 다양성 유지 / 앙상블 후보 | **exp-C, H12** — RF/GBM/LGBM 동시에 안정 |

---

## 6. 다음 단계

1. **horizon sweep과의 교차 확인** — [`summary_horizon_compare.md`](summary_horizon_compare.md)에서 baseline H10-H24를 본 뒤, sweet spot horizon에서 variant 효과를 재검증.
2. **variant × horizon 확장** — H14-H24도 exp-A/B/C로 학습해 winsorize 이득이 H12 이외 horizon에서도 유지되는지 검증.
3. **하이퍼파라미터 튜닝** — RF는 exp-A/H12, LGBM은 exp-A/H12, XGB는 별도 calibration.
4. **앙상블 검증** — RF(exp-A) + LGBM(exp-A) blending이 H12에서 단일 모델 대비 이득을 주는지 확인.

---

## 산출물

```
exp_003_v3_variant_compare/
├── by_horizon/
│   ├── H10/  baseline/ exp-A/ exp-B/ exp-C/  (각 폴더에 rf|gbm|xgb|lgbm.json)
│   ├── H12/  baseline/ exp-A/ exp-B/ exp-C/
│   └── H14..H24/  baseline/  (variant 학습 보류)
├── by_variant/  (by_horizon symlink 미러: variant → horizon 순)
├── summary_variant_compare.md  ← 이 문서
└── summary_horizon_compare.md  (baseline H10-H24 horizon sweep)
```
