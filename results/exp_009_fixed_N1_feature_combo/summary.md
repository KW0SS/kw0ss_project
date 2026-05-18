# exp_009: fixed_N1 피처 엔지니어링 조합(combo) 실험 리포트

> 실험일: 2026-05-18
> Base 비교군: `results/exp_008_fixed_N1_feature_eng` (단일 FE 3종), `results/exp_004_fixed_N`, `results/exp_005_fixed_N_collinear_drop`
> 대상: `fixed_N1`, exp_008의 3개 FE를 조합한 4가지 파이프라인

---

## 0. 실험 목적

exp_008에서 개별 평가한 3개 피처 엔지니어링을 **누적(stacking)** 했을 때 시너지가 있는지 확인한다. 약어 정의:

| 약어 | FE 모드 | 내용 |
|---|---|---|
| **A** | `drop_collinear` | 다중공선성 2개(`유동비율`, `유형자산상각비`) 제거 |
| **B** | `ratio_total_assets` | 절대값 5개를 총자산 대비 비율로 치환 |
| **C** | `signed_log1p` | 모든 피처 `sign(x)·log1p(|x|)` |

실험한 조합: **A+B, B+C, A+C, A+B+C** (4종)

---

## 1. 실행 조건

```bash
.venv/bin/python -m src.modeling.run_all_fixed \
  --exp exp_009_fixed_N1_feature_combo \
  --n 1 \
  --variant baseline exp-A exp-B exp-C \
  --fe a+b b+c a+c a+b+c
```

| 항목 | 값 |
|---|---|
| Dataset | `fixed_N1` (train 2015–2022 / valid 2023 / test 2024, imbalance ≈ 124:1) |
| Variant | baseline, exp-A, exp-B, exp-C |
| Model | RF, GBM, XGBoost, LightGBM, LogReg |
| Threshold | valid F1 최적화 |
| 결과 JSON | 50개 |

### 정규 적용 순서 (입력 토큰 순서 무관)

조합은 토큰 입력 순서와 무관하게 항상 **A → B → C** 순으로 강제 적용한다.

1. **A(drop_collinear)** 먼저: 단순 컬럼 제거로 후속 입력을 축소. A가 `유형자산상각비`를 먼저 제거하므로, A를 포함한 조합에서는 `유형자산상각비_총자산비율`이 생성되지 않는다(비율 컬럼 5→4개). 이는 "공선성 신호를 비율 형태로도 남기지 않는다"는 A의 의도를 보존한 결과다.
2. **B(ratio_total_assets)** 다음: 총자산 복원이 raw-scale 곱셈 항등식(`총자산 = 유형자산 × 유형자산회전율 / 총자본회전율`)에 의존하므로 값 변환(C)보다 반드시 먼저 적용해야 한다.
3. **C(signed_log1p)** 마지막: 최종 피처 집합(비율 컬럼 포함)에 단조 변환.

### Variant 커버리지

B(ratio_total_assets)를 포함한 조합(A+B, B+C, A+B+C)은 robust-scale variant에서 총자산 복원이 불가하므로 **baseline·exp-A 2개 variant만** 학습(자동 skip). B 미포함 조합(A+C)은 **4개 variant 전부** 학습.

| 조합 | variant 수 | JSON |
|---|---:|---:|
| A+B | 2 | 10 |
| B+C | 2 | 10 |
| A+C | 4 | 20 |
| A+B+C | 2 | 10 |

---

## 2. 전체 결론

**어떤 조합도 exp_008의 단일 `signed_log1p`(C)를 넘지 못했다.** 조합은 시너지가 아니라 **상쇄(subtractive)** 로 작동했다.

| 구분 | Model | Variant | FE | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---:|---:|---:|---:|---:|
| exp_008 best (단일 C) | RF | exp-A | signed_log1p | **0.2866** | **0.3559** | 0.3387 | **0.3750** | 0.8587 |
| exp_004 best | RF | exp-C | (collinear 포함) | 0.2861 | 0.3529 | 0.3333 | 0.3750 | 0.8595 |
| exp_005 best (단일 A) | RF | exp-A | drop_collinear | 0.2841 | 0.2759 | 0.3871 | 0.2143 | 0.8620 |
| **exp_009 best (A+C)** | **RF** | **exp-C** | drop_collinear+signed_log1p | 0.2841 | 0.2791 | 0.4000 | 0.2143 | 0.8647 |
| exp_009 best vs exp_008 | - | - | - | **-0.0025** | **-0.0768** | +0.0613 | **-0.1607** | +0.0060 |

핵심 해석:

- exp_009 전체 best(A+C, PR 0.2841)는 exp_008 단일 C(0.2866)보다 **낮고**, 수치가 exp_005 단일 A(PR 0.2841 / F1 0.2759 / R 0.2143)와 거의 동일하다.
- 즉 **C에 A를 더하면 C가 살려낸 recall(0.3750)이 다시 A의 보수화(R 0.2143)로 무너진다.** 조합 안에서 A가 C의 이득을 상쇄한다.
- B를 포함한 모든 조합은 PR-AUC가 ≤ 0.262로, 단일 B의 약점(규모 신호 손실)이 조합으로 그대로 전파된다.

---

## 3. exp_009 Test 상위 결과

| Model | Variant | FE 조합 | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---:|---:|---:|---:|---:|---:|
| RF | exp-C | A+C | 0.147 | 0.2841 | 0.2791 | 0.4000 | 0.2143 | 0.8647 |
| RF | exp-B | A+C | 0.130 | 0.2829 | 0.3333 | 0.4000 | 0.2857 | 0.8670 |
| RF | exp-A | A+C | 0.147 | 0.2815 | 0.2791 | 0.4000 | 0.2143 | 0.8617 |
| RF | baseline | A+C | 0.130 | 0.2781 | 0.3333 | 0.4000 | 0.2857 | 0.8677 |
| LogReg | exp-A | A+C | 0.846 | 0.2726 | 0.1908 | 0.1152 | 0.5536 | 0.8665 |
| LogReg | baseline | A+C | 0.849 | 0.2694 | 0.1944 | 0.1179 | 0.5536 | 0.8669 |
| LightGBM | baseline | B+C | 0.089 | 0.2618 | **0.3704** | **0.6000** | 0.2679 | 0.8729 |
| RF | baseline | B+C | 0.106 | 0.2607 | 0.2807 | 0.2759 | 0.2857 | 0.8525 |
| RF | exp-A | B+C | 0.123 | 0.2568 | 0.2941 | 0.3261 | 0.2679 | 0.8515 |
| XGBoost | exp-A | A+B+C | 0.093 | 0.2531 | 0.2917 | 0.3500 | 0.2500 | 0.8686 |
| RF | exp-A | A+B | 0.104 | 0.2511 | 0.3390 | 0.3226 | 0.3571 | 0.8381 |
| RF | exp-A | A+B+C | 0.104 | 0.2497 | 0.3361 | 0.3175 | 0.3571 | 0.8381 |

---

## 4. 조합별 분석

### 4.1 A+C (drop_collinear + signed_log1p) — 가장 강하지만 C 단독보다 후퇴

- exp_009 best 조합(RF, PR 0.2841). 그러나 RF의 A+C 수치(exp-C 0.2841/F1 0.2791/R 0.2143, baseline 0.2781)는 exp_008의 단일 C(RF, PR 0.286대/R 0.375)가 아니라 **exp_005 단일 A(R 0.2143)** 를 그대로 따라간다.
- 결론: RF에서 **A가 C를 지배**한다. `유동비율` 등 공선성 후보가 signed_log1p 환경에서 recall에 기여하고 있었고, 이를 제거하면 C의 recall 회복 효과가 사라진다. 두 기법을 합치면 "C의 recall + A의 단순화"가 아니라 "A의 보수화 + C의 무효화"가 된다.
- LogReg A+C는 PR 0.2726으로 ranking은 살아있으나 precision 0.11~0.12로 운영점 부적합(단일 C와 동일한 한계).

### 4.2 B+C (ratio_total_assets + signed_log1p) — B와 사실상 동일

- best LightGBM baseline: PR 0.2618 / F1 0.3704 / **Precision 0.6000**. 이는 exp_008의 단일 B LightGBM(PR 0.2627 / F1 0.3704 / P 0.6000)과 거의 완전히 동일하다.
- 트리 모델은 scale-invariant라서 비율 컬럼에 log를 덧씌워도 분할이 거의 안 바뀐다. **C는 B 위에서 정보를 더하지 않는다.** 즉 B+C ≈ B.

### 4.3 A+B (drop_collinear + ratio_total_assets) — PR-AUC 추가 하락

- best RF exp-A: PR 0.2511 / F1 0.3390 / R 0.3571. 단일 B(0.2640)보다도 PR-AUC가 더 낮다. A의 컬럼 제거 + B의 규모 신호 손실이 누적된다.
- 다만 운영점 F1/recall(0.339 / 0.357)은 비교적 건강. LightGBM A+B exp-A는 F1 0.3509로, 절대값을 비율화한 뒤에도 트리가 쓸 만한 F1을 만든다. ranking이 약할 뿐.

### 4.4 A+B+C (전체 누적) — 회복 없음

- best XGBoost exp-A: PR 0.2531 / F1 0.2917. RF A+B+C exp-A는 PR 0.2497 / F1 0.3361 / R 0.3571로 A+B와 거의 동일.
- B의 PR-AUC 손실이 바닥을 결정하고, A·C를 더해도 복구되지 않는다. 풀스택은 중하위권(~0.25)에 머문다.

### 4.5 상호작용 요약

| 관찰 | 내용 |
|---|---|
| A는 C를 무효화 | C 단독의 recall 회복(0.375)이 A 결합 시 0.214로 붕괴 (A+C, A+B+C) |
| B는 모든 조합에 약점 전파 | B 포함 조합 PR-AUC ≤ 0.262, 어떤 파트너도 규모 신호를 복구 못함 |
| C는 B 위에서 무의미 | 트리 scale-invariant → B+C ≈ B (P=0.60 운영점도 단일 B에 이미 존재) |

→ **세 기법은 누적 시 상보적이지 않고 상쇄적이다.**

---

## 5. 모델별 best 비교 (exp_009 TEST PR-AUC)

| Model | best 조합 | best Variant | exp_009 PR-AUC | exp_009 F1 | exp_008 단일 best PR-AUC |
|---|---|---|---:|---:|---:|
| RF | A+C | exp-C | 0.2841 | 0.2791 | 0.2866 (C) |
| LogReg | A+C | exp-A | 0.2726 | 0.1908 | 0.2713 (C) |
| LightGBM | B+C | baseline | 0.2618 | 0.3704 | 0.2627 (B) |
| XGBoost | A+B+C | exp-A | 0.2531 | 0.2917 | 0.2305 (C) |
| GBM | A+C | exp-B | ~0.10 | ~0.16 | 0.1122 (A) |

- RF·LightGBM·LogReg 모두 자기 best 조합이 exp_008 단일 best와 사실상 동률이거나 소폭 낮음 → **조합으로 얻는 순이득 없음**.
- XGBoost만 A+B+C(0.2531)가 단일 C(0.2305)보다 높지만, 그래도 RF 단일 C(0.2866) 수준에는 못 미친다.

---

## 6. valid → test 선택 안정성

valid PR-AUC 상위는 `XGB · drop_collinear+ratio_total_assets`류(vPR 0.19대)지만 test PR-AUC는 0.22~0.25로 중위권이다. exp_004/005/008과 동일하게 **valid 기준 선택이 test best(RF A+C 0.2841)를 못 짚는다.** 검증 표본(2023, pos 39) 부족에 따른 선택 불안정 문제는 조합 실험에서도 그대로 반복된다.

---

## 7. 결론

### 판단

| 조합 | 판단 |
|---|---|
| A+C | exp_009 best이나 단일 C 대비 후퇴(recall 붕괴). A가 C를 지배 |
| B+C | 단일 B와 동일(P=0.60 운영점). C가 추가 가치 없음 |
| A+B | PR-AUC 추가 하락. F1만 보존 |
| A+B+C | 회복 없음, 중하위권 |

### 추천

- **운영 후보 변경 없음**: `exp_008 RF fixed_N1 exp-A signed_log1p`(단일 C)가 여전히 fixed_N1 최강. 조합 실험은 "FE를 쌓지 말 것"을 ablation으로 확정.
- 특히 **C에 A를 더하지 말 것** — C의 핵심 이득(recall 0.375 복원)이 사라진다.
- precision-우선 운영점이 필요하면 단일 B(또는 동일 성능의 B+C) LightGBM(P=0.60)을 사용. 굳이 조합으로 만들 필요 없음.
- 후속 우선순위(exp_008과 동일 유지): ① valid 표본 안정화, ② 단일 signed_log1p 위에서 RF hyperparameter tuning. **FE 조합 탐색은 종료.**

---

## 8. 산출물

- 결과 디렉터리: `results/exp_009_fixed_N1_feature_combo/`
- JSON 결과: 50개 (A+B 10, B+C 10, A+C 20, A+B+C 10)
- 신규 코드: `feature_engineering.py`(combo 파이프라인 `normalize_fe_token`/`canonical_fe_label`/FE_ORDER), `run_all_fixed.py`(`--fe` 조합 토큰 `a+b` 등 지원)
- 비교 기준: `results/exp_008_fixed_N1_feature_eng/`, `results/exp_004_fixed_N/`, `results/exp_005_fixed_N_collinear_drop/`
