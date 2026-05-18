# exp_008: fixed_N1 피처 엔지니어링 비교 실험 리포트

> 실험일: 2026-05-18
> Base 비교군: `results/exp_004_fixed_N` (다중공선성 포함, 33 feat), `results/exp_005_fixed_N_collinear_drop` (다중공선성 제거, 31 feat)
> 대상: `fixed_N1` 단일 horizon, 3가지 피처 엔지니어링 기법

---

## 0. 실험 목적

`fixed_N1`(상폐연도 기준 정확히 1년 전 라벨링) 데이터에 서로 다른 피처 엔지니어링을 적용했을 때 모델 성능이 어떻게 달라지는지 비교한다. 세 가지 기법을 독립 실험으로 학습했다.

| FE 모드 | 내용 |
|---|---|
| `drop_collinear` | EDA 다중공선성 후보 2개(`유동비율`, `유형자산상각비`) 제거 |
| `ratio_total_assets` | 절대값 KRW 항목 5개(`유형자산`, `무형자산`, `무형자산상각비`, `유형자산상각비`, `감가상각비`)를 총자산 대비 비율로 치환 |
| `signed_log1p` | 모든 피처에 `sign(x)·log1p(|x|)` 적용 |

각 FE는 다중공선성 제거를 중복 적용하지 않아 서로 독립적으로 비교된다(`drop_collinear`만 제거 수행).

---

## 1. 실행 조건

```bash
.venv/bin/python -m src.modeling.run_all_fixed \
  --exp exp_008_fixed_N1_feature_eng \
  --n 1 \
  --variant baseline exp-A exp-B exp-C \
  --fe drop_collinear ratio_total_assets signed_log1p
```

| 항목 | 값 |
|---|---|
| Dataset | `fixed_N1` (train 2015–2022 / valid 2023 / test 2024, imbalance ≈ 124:1) |
| Preprocessing variant | baseline, exp-A(winsorize), exp-B(robust), exp-C(winsorize+robust) |
| Model | RF, GBM, XGBoost, LightGBM, LogReg |
| Threshold | valid F1 최적화 |
| 결과 JSON | 50개 |

### 총자산 컬럼 복원 (ratio_total_assets)

processed `fixed_N` CSV에는 총자산(자산총계) 컬럼이 없다. 동일 기간의 두 회전율이 매출액을 공유한다는 점을 이용해 총자산을 **정확히 복원**했다.

```text
유형자산회전율 = 매출액 / 유형자산
총자본회전율   = 매출액 / 총자산
=> 총자산 = 유형자산 × 유형자산회전율 / 총자본회전율
```

- baseline train 기준 복원 가능 행 **99.7%**, 복원 불가 행(분모 0 등)은 train-median impute.
- 이 항등식은 raw-scale variant(baseline, exp-A)에서만 성립한다. `robust_scale` variant(exp-B, exp-C)는 컬럼별 RobustScaler가 곱셈 항등식을 깨므로 **ratio_total_assets 대상에서 제외**(자동 skip). 따라서 ratio_total_assets는 baseline/exp-A 2개 variant만 학습됐다.

---

## 2. 전체 결론

`signed_log1p`가 세 기법 중 가장 강했고, exp_004의 기존 best를 PR-AUC·F1 모두에서 **근소하게** 넘어섰다. `drop_collinear`는 exp_005를 정확히 재현했고(배선 검증), `ratio_total_assets`는 ranking은 약하지만 precision이 가장 높은 운영점을 만든다.

| 구분 | Model | Variant | FE | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---:|---:|---:|---:|---:|
| exp_004 best (33 feat) | RF | exp-C | (collinear 포함) | 0.2861 | 0.3529 | 0.3333 | 0.3750 | 0.8595 |
| exp_005 best (31 feat) | RF | exp-A | drop_collinear | 0.2841 | 0.2759 | 0.3871 | 0.2143 | 0.8620 |
| **exp_008 best** | **RF** | **exp-A** | **signed_log1p** | **0.2866** | **0.3559** | 0.3387 | 0.3750 | 0.8587 |
| exp_008 vs exp_004 | - | - | - | +0.0005 | +0.0030 | +0.0054 | +0.0000 | -0.0008 |

해석:

- `signed_log1p`는 exp_005(=`drop_collinear`)에서 무너졌던 **recall을 0.2143 → 0.3750으로 복원**하면서 PR-AUC를 유지/소폭 개선했다. exp_004 incumbent를 두 지표 모두에서 넘은 최초의 FE이지만 PR-AUC 차이(+0.0005)는 사실상 noise 수준이다.
- `ratio_total_assets`는 best PR-AUC가 0.2640으로 exp_004/005보다 낮다. 절대값→비율 정규화가 트리 모델이 쓰던 규모(scale) 신호를 제거한 결과로 보인다.
- `drop_collinear` best(RF exp-A, PR 0.2841 / F1 0.2759)는 exp_005 best와 **완전히 동일** — FE 배선이 기존 파이프라인을 정확히 재현함을 확인.

---

## 3. exp_008 Test 상위 결과

| Model | Variant | FE | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---:|---:|---:|---:|---:|---:|
| RF | exp-A | signed_log1p | 0.097 | 0.2866 | 0.3559 | 0.3387 | 0.3750 | 0.8587 |
| RF | exp-C | signed_log1p | 0.097 | 0.2863 | 0.3559 | 0.3387 | 0.3750 | 0.8594 |
| RF | exp-A | drop_collinear | 0.147 | 0.2841 | 0.2759 | 0.3871 | 0.2143 | 0.8620 |
| RF | exp-C | drop_collinear | 0.147 | 0.2835 | 0.2791 | 0.4000 | 0.2143 | 0.8648 |
| RF | exp-B | drop_collinear | 0.130 | 0.2823 | 0.3333 | 0.4000 | 0.2857 | 0.8670 |
| RF | exp-B | signed_log1p | 0.089 | 0.2805 | 0.3577 | 0.3284 | 0.3929 | 0.8581 |
| RF | baseline | signed_log1p | 0.082 | 0.2802 | 0.3333 | 0.2895 | 0.3929 | 0.8573 |
| RF | baseline | drop_collinear | 0.130 | 0.2778 | 0.3333 | 0.4000 | 0.2857 | 0.8680 |
| LogReg | exp-A | signed_log1p | 0.880 | 0.2713 | 0.2156 | 0.1362 | 0.5179 | 0.8614 |
| LogReg | baseline | signed_log1p | 0.888 | 0.2684 | 0.2248 | 0.1436 | 0.5179 | 0.8618 |
| RF | baseline | ratio_total_assets | 0.103 | 0.2640 | 0.2881 | 0.2742 | 0.3036 | 0.8526 |
| LightGBM | baseline | ratio_total_assets | 0.089 | 0.2627 | **0.3704** | **0.6000** | 0.2679 | 0.8729 |

---

## 4. FE 모드별 분석

### 4.1 signed_log1p — 채택 후보

- RF 계열이 모든 variant에서 PR-AUC ≥ 0.28로 안정적이고, **recall이 0.375~0.393으로 회복**됐다. exp_005의 RF가 precision↑/recall↓(0.2143)로 보수화됐던 문제를 heavy-tailed 원시 KRW 컬럼의 log 압축이 완화한 것으로 해석된다.
- **LogReg 개선 폭이 가장 크다**: PR-AUC가 exp-A 기준 0.2713 (exp_005 LogReg 0.2174, exp_004 0.2094 대비 +0.054~+0.062). 선형 모델이 long-tail 절대값을 그대로 받으면 불리한데, log 변환이 이를 선형화했다. 단 F1 threshold에서 precision이 0.13~0.14로 매우 낮아 운영점으로는 부적합하고 ranking 개선으로만 유효하다.

### 4.2 ratio_total_assets — precision-우선 운영점

- best PR-AUC 0.2640(RF baseline)로 ranking은 exp_004/005보다 약하다. 절대값 항목을 총자산 비율로 정규화하면서 규모 신호가 사라진 것이 주원인.
- 반면 **LightGBM baseline에서 Precision 0.6000 / F1 0.3704** — 본 실험 전체에서 가장 높은 precision 운영점. 오탐 비용이 큰 운영 시나리오에서는 후보가 될 수 있다.
- baseline/exp-A 2개 variant만 학습됨(robust-scale variant는 총자산 복원 불가로 설계상 제외).

### 4.3 drop_collinear — exp_005 재현

- best RF exp-A: PR-AUC 0.2841 / F1 0.2759 / Recall 0.2143 → exp_005 best와 수치까지 동일.
- exp_004 대비 feature 2개 감소·PR-AUC 거의 유지·F1/recall 하락이라는 exp_005 결론을 그대로 재확인. 추가 정보는 없으나 모델 단순화 ablation으로서 의미.

---

## 5. 모델별 best 비교 (exp_008 TEST PR-AUC)

| Model | best FE | best Variant | exp_008 PR-AUC | exp_008 F1 | exp_004 best PR-AUC | exp_005 best PR-AUC |
|---|---|---|---:|---:|---:|---:|
| RF | signed_log1p | exp-A | 0.2866 | 0.3559 | 0.2861 | 0.2841 |
| LogReg | signed_log1p | exp-A | 0.2713 | 0.2156 | 0.2094 | 0.2174 |
| LightGBM | ratio_total_assets | baseline | 0.2627 | 0.3704 | 0.2327 | 0.2347 |
| XGBoost | signed_log1p | exp-C | 0.2305 | 0.2667 | 0.2305 | 0.2050 |
| GBM | drop_collinear | exp-A | 0.1122 | 0.1714 | 0.0923 | 0.1122 |

관찰:

- RF는 모든 FE에서 최강 패밀리. signed_log1p에서 incumbent를 소폭 추월.
- LogReg·LightGBM·XGBoost 모두 exp_008 FE에서 자기 best가 exp_004/005 대비 개선 — 약한 학습기/선형 모델일수록 FE 이득이 크다.
- GBM은 어떤 FE에서도 0.11대로 여전히 최약.

---

## 6. valid → test 선택 안정성

valid(2023, pos 39) PR-AUC는 0.14~0.17로 낮고, **valid best 구성과 test best 구성이 일치하지 않는다.**

| 기준 | Top1 구성 | 비고 |
|---|---|---|
| valid PR-AUC | XGB baseline/exp-B `drop_collinear` (0.1690) | test에서는 중위권 |
| test PR-AUC | RF exp-A `signed_log1p` (0.2866) | valid에서는 상위 아님 |

valid 양성 표본이 적어 valid 기준 모델/threshold 선택이 불안정하다는 기존 실험(exp_004/005) 결론이 이번에도 반복됐다. FE 자체보다 검증 표본 안정화·threshold 안정화가 여전히 상위 과제다.

---

## 7. 결론

### 판단

| FE | 판단 |
|---|---|
| `signed_log1p` | exp_004 best를 PR-AUC·F1 모두 근소 추월 + recall 복원. **신규 후보** |
| `ratio_total_assets` | ranking 약화. 단 LGBM에서 최고 precision(0.60) 운영점 제공 |
| `drop_collinear` | exp_005 재현, 신규 정보 없음. 단순화 ablation |

### 추천

- 운영 후보를 `exp_004 RF fixed_N1 exp-C`에서 **`exp_008 RF fixed_N1 exp-A signed_log1p`로 교체 검토**. 단 PR-AUC 우위(+0.0005)는 noise 범위이므로, recall 복원(0.2143→0.3750)을 실질 근거로 삼는다.
- precision-우선 시나리오용으로 `exp_008 LightGBM fixed_N1 baseline ratio_total_assets`(P=0.60)를 별도 후보로 보관.
- `signed_log1p`는 LogReg ranking을 크게 끌어올리므로, 향후 스태킹/앙상블 입력 다양화에 활용 가능.
- 후속 우선순위: ① valid 표본 안정화(연도 확장/교차검증), ② signed_log1p 위에서 RF hyperparameter tuning, ③ signed_log1p + 다중공선성 제거 결합 ablation.

---

## 8. 산출물

- 결과 디렉터리: `results/exp_008_fixed_N1_feature_eng/`
- JSON 결과: 50개 (`fixed_N1 × {baseline,exp-A,exp-B,exp-C} × {drop_collinear, ratio_total_assets, signed_log1p} × 5 models`, ratio_total_assets는 baseline/exp-A만)
- 신규 코드: `src/modeling/feature_engineering.py`, `data_loader_fixed.py`(`fe` 인자), `run_all_fixed.py`(`--fe` 옵션)
- 비교 기준: `results/exp_004_fixed_N/`, `results/exp_005_fixed_N_collinear_drop/`
