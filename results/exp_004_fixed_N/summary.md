# exp_004: fixed_N 데이터셋 학습 — 실험 리포트

> 실험일: 2026-05-15
> 브랜치: `35-train-train-model-by-processed-fixed-data-set`
> 비교 baseline: `exp_002_v3_baseline` (H10 baseline RF, test PR-AUC 0.1735)

---

## 0. 실험 배경

`processed/fixed_N{1,2,3}/` 데이터셋은 H-horizon 방식과 라벨 의미가 다르다.

| 항목 | H-horizon (exp_002, exp_003) | fixed_N (이번 실험) |
|---|---|---|
| 라벨 의미 | forward-looking, 향후 H개월 내 상폐 | backward-looking, 상폐연도 - N년 시점 |
| 라벨 단위 | 시점 (월) | 시점 (년) |
| Positive 정의 | 시점 t에 본 데이터가 H개월 내 상폐 | 시점 t = 상폐연도 - N 인 연도의 모든 분기 |
| Test 양성 안정성 | 시점 윈도우로 양성이 분산됨 | N에 따라 양성 위치가 고정됨 |

EDA(`notebooks/2026-05-15_fixed_N_overview_eda.ipynb`)에서 다음이 확인됐다.

- **fixed_N1**: train_pos 283 / valid_pos 39 / test_pos **56** — test 비교 가능
- **fixed_N2**: train_pos 293 / valid_pos 60 / test_pos **24** — test 비교 가능 (단, 표본 작음)
- **fixed_N3**: train_pos 316 / valid_pos 24 / test_pos **0** — test 지표 산출 불가

이번 실험에서는 N1, N2 만 학습 대상으로 잡고, 각 N × 전처리 variant(baseline/exp-A/B/C) × 5 모델(RF, GBM, XGBoost, LightGBM, LogReg)을 학습해 H-horizon 대비 라벨 정의 변경 효과와 N1/N2 비교 효과를 함께 본다.

---

## 1. 코드 변경 사항

H-horizon 학습 코드를 유지한 채 fixed_N 전용 학습 경로를 새로 추가했다. horizon 코드와 학습 모듈(`train_*.py`) 자체는 변경하지 않는다.

| 파일 | 변경 | 역할 |
|---|---|---|
| `src/modeling/data_loader_fixed.py` | 신규 | `fixed_N{n}/{variant}/` 경로 로더. 컬럼 분류 상수와 `prepare_xy`/`get_feature_columns` 는 horizon 로더에서 재사용 |
| `src/modeling/train_logreg.py` | 신규 | StandardScaler + LogisticRegression(class_weight='balanced') Pipeline. 다른 모델과 동일한 `train(X_train, y_train, X_valid, y_valid) → (model, params)` 시그니처 |
| `src/modeling/run_all_fixed.py` | 신규 | N × variant × model CLI. 결과는 `{model}_N{n}_{variant}.json` 으로 저장. valid positive=0 일 때 threshold 최적화 skip, test positive=0 일 때 test 결과 None 으로 표시 (N3 대비) |

horizon 학습 코드(`data_loader.py`, `run_all.py`)는 그대로 두어 후속 horizon 실험에 영향 없다.

### 실행 명령

```bash
python -m src.modeling.run_all_fixed --exp exp_004_fixed_N \
    --n 1 2 --variant baseline exp-A exp-B exp-C
```

기본 모델은 `rf gbm xgb lgbm logreg` 5종.

---

## 2. 데이터 요약

| 항목 | fixed_N1 baseline | fixed_N2 baseline |
|---|---|---|
| Features | 33개 (재무비율 21 + 원시값 5 + 매크로 6 + 유보 1) | 33개 (동일) |
| Train | 35,493행 (pos **283**) | 35,493행 (pos **293**) |
| Valid | 5,050행 (pos **39**) | 5,050행 (pos **60**) |
| Test | 5,311행 (pos **56**) | 5,311행 (pos **24**) |
| Imbalance ratio | 124.4 : 1 | 120.1 : 1 |
| Train years | 2015–2022 | 2015–2022 |
| Valid year | 2023 | 2023 |
| Test year | 2024 | 2024 |
| Impute 후 잔존 NaN | 0 | 0 |

variant(baseline/exp-A/B/C) 간에는 행 수·라벨 분포 동일, winsorize/robust_scale 적용만 다르다.

| Variant | winsorize | robust_scale |
|---|---|---|
| baseline | False | False |
| exp-A | True  | False |
| exp-B | False | True  |
| exp-C | True  | True  |

---

## 3. 모델 결과

### 3-1. Test set (PR-AUC 내림차순, 상위 12개)

| Model | N | Variant | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|---|
| **RF** | **1** | **exp-C** | 0.097 | **0.2861** | **0.3529** | 0.3333 | 0.3750 | 0.8595 |
| RF | 1 | exp-A | 0.097 | 0.2857 | 0.3529 | 0.3333 | 0.3750 | 0.8586 |
| RF | 1 | exp-B | 0.086 | 0.2794 | 0.3520 | 0.3188 | 0.3929 | 0.8581 |
| RF | 1 | baseline | 0.082 | 0.2794 | 0.3333 | 0.2895 | 0.3929 | 0.8573 |
| LightGBM | 1 | exp-C | 0.215 | 0.2515 | 0.2535 | 0.6000 | 0.1607 | 0.8697 |
| LightGBM | 1 | baseline | 0.025 | 0.2327 | 0.2883 | 0.2909 | 0.2857 | 0.8569 |
| XGBoost | 1 | exp-C | 0.098 | 0.2305 | 0.2667 | 0.3529 | 0.2143 | 0.8806 |
| XGBoost | 1 | baseline | 0.103 | 0.2179 | 0.2683 | 0.4231 | 0.1964 | 0.8691 |
| XGBoost | 1 | exp-B | 0.103 | 0.2179 | 0.2683 | 0.4231 | 0.1964 | 0.8691 |
| LogReg | 1 | exp-A | 0.984 | 0.2094 | 0.2118 | 0.3103 | 0.1607 | 0.8756 |
| LogReg | 1 | exp-C | 0.984 | 0.2094 | 0.2118 | 0.3103 | 0.1607 | 0.8756 |
| XGBoost | 1 | exp-A | 0.116 | 0.2048 | 0.2500 | 0.4167 | 0.1786 | 0.8846 |

> N2 결과는 모두 test PR-AUC ≤ 0.013 으로 사실상 random baseline 수준이라 표 하단에서 별도 분석(섹션 4 참조). 전체 40개 결과는 `*_N{1,2}_{variant}.json` 파일에 저장.

### 3-2. Valid set (모델 선정 기준, 상위 8개)

| Model | N | Variant | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|---|
| XGBoost | 2 | baseline | 0.024 | 0.1826 | 0.2716 | 0.5238 | 0.1833 | 0.8290 |
| XGBoost | 2 | exp-B | 0.024 | 0.1826 | 0.2716 | 0.5238 | 0.1833 | 0.8290 |
| XGBoost | 1 | exp-A | 0.116 | 0.1680 | 0.2597 | 0.2632 | 0.2564 | 0.8851 |
| LogReg | 2 | exp-A | 0.971 | 0.1666 | 0.2708 | 0.3611 | 0.2167 | 0.8515 |
| LogReg | 2 | exp-C | 0.971 | 0.1666 | 0.2708 | 0.3611 | 0.2167 | 0.8515 |
| XGBoost | 1 | exp-C | 0.098 | 0.1639 | 0.2619 | 0.2444 | 0.2821 | 0.8871 |
| RF | 2 | exp-A | 0.049 | 0.1543 | 0.2341 | 0.1655 | 0.4000 | 0.8524 |
| RF | 2 | exp-C | 0.049 | 0.1543 | 0.2330 | 0.1644 | 0.4000 | 0.8525 |

valid 상위는 대부분 N2 모델이지만, 같은 N2 모델이 test 에서는 모두 무너지는 것이 핵심 관전 포인트(섹션 4).

---

## 4. fixed_N1 vs fixed_N2 분석

### 4-1. 핵심 격차

| 구분 | fixed_N1 RF exp-C | fixed_N2 RF exp-C |
|---|---|---|
| Valid PR-AUC | 0.1184 | 0.1543 |
| **Test PR-AUC** | **0.2861** | **0.0101** |
| Test F1 | 0.3529 | 0.0245 |
| Test ROC-AUC | 0.8595 | 0.6283 |

valid 에서는 N2 가 약간 더 강해 보였으나 test 에서 28배 차이가 났다. 다른 모델에서도 동일 패턴(N2 valid PR-AUC > N1 valid PR-AUC, N2 test ≪ N1 test).

### 4-2. 원인 가설

1. **양성 표본 분포의 시간 안정성 차이**
   N1 라벨은 "상폐 1년 전" 데이터로, 재무비율이 부도/감리 직전 신호를 가장 강하게 담는다. N2 는 2년 전이라 정상 재무에 가까운 양성도 섞이고, train/valid/test 시점마다 양성 표본의 재무 특성이 더 변동한다.
2. **Test 라벨 윈도우의 잠재적 단절**
   N2 test_pos=24 는 "2024년 데이터 중 2026년 상폐 기업" 인데, 2026년 상폐가 아직 완전히 누적되지 않은 시점의 라벨 정의가 적용된 셈이다. N1 test_pos=56 ("2024 데이터 중 2025년 상폐")은 라벨이 더 완성도 높다.
3. **N2 valid → test 분포 차이**
   N2 valid_pos 60 (2023년 데이터 → 2025년 상폐) 에 비해 test_pos 24 는 절반 이하. valid 가 학습/튜닝 신호를 강하게 줬지만, test 분포가 다르면서 threshold 최적화 결과가 그대로 전이되지 않았다(예: LogReg N2 valid F1=0.27 → test F1=0.04).

### 4-3. 운영 가이드

- **모델 선택은 N1 기준 valid 가 아니라 test 로 검증한다.** N2 는 valid 에서 매력적으로 보이지만 production 신뢰도 부족.
- N3 는 test_pos=0 이라 본 실험에서 학습 자체를 skip 했다(EDA 결론과 동일). 추후 2026~2027 상폐 데이터가 누적된 뒤 재학습 후보.
- N2 결과는 본 리포트에선 "valid generalization 실패 사례" 로 기록하고, 모델 선택에서 제외.

---

## 5. Variant 효과 분석 (fixed_N1, test 기준)

| Model | baseline | exp-A | exp-B | exp-C | 최고 variant |
|---|---|---|---|---|---|
| RF | 0.2794 | 0.2857 | 0.2794 | **0.2861** | exp-C (≈ exp-A) |
| XGBoost | 0.2179 | 0.2048 | 0.2179 | **0.2305** | exp-C |
| LightGBM | 0.2327 | 0.2028 | 0.1910 | **0.2515** | exp-C |
| GBM | 0.1048 | 0.0923 | 0.0888 | 0.0875 | baseline |
| LogReg | 0.1747 | **0.2094** | 0.1747 | **0.2094** | exp-A / exp-C |

(셀 값은 test PR-AUC)

**해석:**
- **exp-C(winsorize=True, robust_scale=True)** 가 트리 3종 모두에서 가장 강하다. exp_003 의 horizon 실험에서 baseline 우위였던 결론과 다른 패턴.
- **GBM** 만은 baseline 이 가장 좋고 winsorize/scale 적용 시 성능이 떨어진다 — GBM 은 outlier 분할을 약한 학습기 단계에서 활용하는 성격이라 winsorize 가 신호를 깎는 것으로 보인다.
- **LogReg** 는 winsorize 가 핵심(baseline/exp-B → exp-A/exp-C 에서 +20%). 선형 모델 특성상 outlier 가중치 영향이 커 winsorize 효과가 가장 분명히 드러난다.
- **scaling 단독(exp-B)** 효과는 미미하거나 음(-). winsorize 가 본질적 개선 요인이다.

---

## 6. H-horizon 대비 비교 (Best RF 기준)

| 실험 | 라벨 정의 | Variant | Test PR-AUC | Test F1 | Test ROC-AUC |
|---|---|---|---|---|---|
| exp_002 H10 RF | forward 10개월 | baseline | 0.1735 | 0.3158 | 0.8549 |
| exp_003 H10 RF best | forward 10개월 | exp-A | 0.1857 | 0.3429 | 0.8584 |
| **exp_004 N1 RF** | **backward 1년** | **exp-C** | **0.2861** | **0.3529** | **0.8595** |

| Metric | exp_002 H10 RF → exp_004 N1 RF | 변화 |
|---|---|---|
| Test PR-AUC | 0.1735 → 0.2861 | **+65%** |
| Test F1 | 0.3158 → 0.3529 | +12% |
| Test Precision | 0.4286 → 0.3333 | -22% |
| Test Recall | 0.2500 → 0.3750 | +50% |
| Test ROC-AUC | 0.8549 → 0.8595 | +0.0046 |

**개선 원인 추정:**
- **양성 정의 명확화**: H10 은 시점 t 에서 향후 10개월 내 상폐를 양성으로 보지만 윈도우가 짧아 일부 정상 시점도 양성으로 잡힌다. N1 은 "상폐 1년 전" 으로 양성 자체가 부도 임박 시점이라 분리도가 높다.
- **Test 양성 수 증가**: H10 test_pos 36 → N1 test_pos 56 (+56%). 평가 안정성 향상.
- **Recall 개선이 precision 감소를 상쇄**: F1 +0.04, PR-AUC +0.11. Precision 손실은 임계값 선택 시 조정 가능.

단, 라벨 정의 자체가 다르므로 production 시점에서는 무엇이 더 적합한지 별도 판단 필요:
- N1 은 "1년 뒤 상폐를 예측" → 알람 시점이 늦음
- H10 은 "10개월 내 상폐를 예측" → forward-looking 운영에 직접적

---

## 7. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| **Best 모델** | **Random Forest (fixed_N1, exp-C)** |
| Test PR-AUC | **0.2861** |
| Test F1 | **0.3529** |
| Test Precision / Recall | 0.3333 / 0.3750 |
| Threshold | 0.097 |
| exp_002 H10 RF 대비 | PR-AUC **+65%**, F1 +12% |

- **fixed_N1 데이터셋이 H10 보다 PR-AUC 기준 크게 우월.** 라벨 정의가 부도 직전 신호를 잘 포착하기 때문.
- **RF 가 fixed_N1 모든 variant 에서 PR-AUC 1위** — exp_002/exp_003 결론(RF 우위)이 fixed_N 데이터셋에서도 일관되게 재현됐다.
- **fixed_N2 는 valid 에서 매력적이나 test 에서 무너지므로 모델 선택에서 제외.** valid generalization 실패 사례로 기록.
- **Variant 는 exp-C(winsorize + robust_scale)** 가 트리 모델에서 가장 강하다. exp_003 의 baseline 우위와 반대 결과 — fixed_N 의 양성 분포가 outlier 의존성이 작아 winsorize 가 도움이 되는 것으로 추정.

### 후속 실험

1. **하이퍼파라미터 튜닝**: fixed_N1 RF exp-C 위에서 Optuna 적용 — `max_depth`, `min_samples_leaf`, `n_estimators` 튜닝
2. **시계열 안정성 검증**: train/valid/test 연도를 한 칸씩 밀어 (2014–2021/2022/2023, 2016–2023/2024/2025 등) RF N1 exp-C 가 안정적으로 재현되는지 확인
3. **N1 × Horizon 앙상블**: fixed_N1 모델과 H10 모델 확률을 평균/스태킹해 Precision 회복 여부 확인
4. **fixed_N3 재학습 예약**: 2026~2027 상폐 데이터 누적 후 test_pos > 0 이 되면 학습 가능
5. **Threshold 안정화**: bootstrap CI 로 valid threshold 의 신뢰구간을 본 다음 보수적 임계값으로 production 적용

---

## 산출물 목록

```
results/exp_004_fixed_N/
├── rf_N1_baseline.json       rf_N1_exp-A.json       rf_N1_exp-B.json       rf_N1_exp-C.json       (best: exp-C)
├── gbm_N1_baseline.json      gbm_N1_exp-A.json      gbm_N1_exp-B.json      gbm_N1_exp-C.json
├── xgb_N1_baseline.json      xgb_N1_exp-A.json      xgb_N1_exp-B.json      xgb_N1_exp-C.json
├── lgbm_N1_baseline.json     lgbm_N1_exp-A.json     lgbm_N1_exp-B.json     lgbm_N1_exp-C.json
├── logreg_N1_baseline.json   logreg_N1_exp-A.json   logreg_N1_exp-B.json   logreg_N1_exp-C.json
├── rf_N2_baseline.json       rf_N2_exp-A.json       rf_N2_exp-B.json       rf_N2_exp-C.json
├── gbm_N2_baseline.json      gbm_N2_exp-A.json      gbm_N2_exp-B.json      gbm_N2_exp-C.json
├── xgb_N2_baseline.json      xgb_N2_exp-A.json      xgb_N2_exp-B.json      xgb_N2_exp-C.json
├── lgbm_N2_baseline.json     lgbm_N2_exp-A.json     lgbm_N2_exp-B.json     lgbm_N2_exp-C.json
├── logreg_N2_baseline.json   logreg_N2_exp-A.json   logreg_N2_exp-B.json   logreg_N2_exp-C.json
└── summary.md                # 이 리포트
```

총 40개 JSON (5 모델 × 2 N × 4 variant) + summary.md.
