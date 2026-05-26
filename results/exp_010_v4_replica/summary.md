# exp_010 — v4 보고서 재현 + 라벨링 전략 비교

> 실험일: 2026-05-26
> 베이스: 박민서 v4 보고서 ("전체 raw 데이터 기반 Group Split XGBoost 학습 결과")
> 비교군: 우리 기존 exp_004 ~ exp_009 (Temporal Split + 정밀 N1 라벨)
> 목적: PDF 보고서 세팅을 우리 환경에서 재현하되, **라벨링 전략 두 가지**를 비교한다.

---

## 0. 실험 목적

박민서 v4 보고서는 `delisted/` 폴더의 모든 분기 행을 양성(=1)으로 두는 단순 라벨링을 사용한다. 우리 기존 파이프라인(`fixed_N1`)은 상폐 시점 정확히 1년 전 분기만 양성으로 보는 더 엄격한 라벨링이다. 두 방식의 성능 차이가 "모델"의 차이인지 "라벨 정의"의 차이인지 분리하기 위해, **동일한 raw 데이터·동일한 split 정책·동일한 XGBoost 하이퍼파라미터** 위에서 라벨링만 바꿔 비교했다.

- **all_delisted** (PDF v4 방식): `delisted/` 폴더 모든 행 = 1
- **only_n1** (우리 fixed_N1 방식): 상폐 연도 정확히 1년 전 분기만 = 1, 그 외 상폐기업 행은 제거

---

## 1. 실행 조건

### 데이터 파이프라인

신규 파일 2개. 기존 코드(`build_master_dataset.py`, `build_fixed_datasets.py`, `train_xgboost.py` 등)는 **건드리지 않음**.

```bash
# 1) 전처리 (raw → train/valid/test CSV)
.venv/bin/python preprocess/build_v4_replica.py --workers 8

# 2) XGBoost 학습 (variant 별)
.venv/bin/python -m src.modeling.train_xgb_v4_replica --variant all
```

| 항목 | 값 |
|---|---|
| 입력 raw | `data/raw/raw/{healthy,delisted}/<sector>/*.json` (56,235개) |
| 사용 보고기간 | Q1 / H1 / Q3 / ANNUAL 4종 모두 |
| Split | **Group Split** (stock_code 기준 70/15/15, seed=42) — PDF와 동일 |
| 성장률 | 같은 quarter year-1 YoY (매출액/순이익/영업이익) |
| 결측 처리 | (sector × quarter) 중앙값 → quarter 중앙값 → global → 0; `_missing` flag 동반 |
| 거시경제 | macro_quarterly.csv 6개 컬럼 left join (`year`, `quarter` 키) |
| Feature 수 | 70 (재무비율 30 + missing flag 30 + period flag 4 + macro 6) |
| 모델 | XGBoost (PDF grid1_02 그대로) |
| 하이퍼파라미터 | `max_depth=3`, `lr=0.03`, `n_estimators=400`, `subsample=0.8`, `colsample_bytree=0.8`, `scale_pos_weight=neg/pos` |
| Threshold | validation F1 최대화 grid (0.05–0.95, step 0.01) |
| Seed | 42 |

### 출력

```
preprocess/data/processed/v4_replica/
  all_delisted/{train,valid,test}.csv, meta.json
  only_n1/{train,valid,test}.csv,     meta.json
results/exp_010_v4_replica/
  all_delisted.json, only_n1.json, summary.md
```

---

## 2. 데이터 변환 결과

### 2.1 raw → master 단계 (variant 공통)

| 항목 | 값 |
|---|---|
| 입력 JSON | 56,235개 |
| 변환 성공 (healthy + delisted) | 56,021행 |
| (code, year, quarter) dedup | 56,021 → 56,021 (중복 없음) |
| 상장일 필터 제거 | 340행 |
| 2015년 이전 제거 | 114행 |
| **master 최종** | **55,567행** |
| 고유 종목 | 1,890개 |

PDF의 "raw JSON 로드 56,235 / 학습 행 56,233 / 고유 종목 1,983"과 비교해 우리는 약간 더 보수적으로 동작한다:
- 우리는 구조적 상폐 4개(`048260`, `029960`, `006580`, `115960`) + 더미(`999999`) 5개를 제거 → PDF는 미제거로 추정
- 상장일 필터(340행) 적용 → PDF는 미적용으로 추정
- 2015년 이전 제거(114행) → PDF는 모든 연도 사용

### 2.2 variant 별 분포

| variant | rows | label=1 | label=0 | 고유 종목 | 양성 비율 |
|---|---:|---:|---:|---:|---:|
| all_delisted | 55,567 | 2,575 | 52,992 | 1,890 | 4.63% |
| only_n1      | 53,370 |   378 | 52,992 | 1,885 | 0.71% |

### 2.3 Group Split 결과 (양쪽 동일 seed)

**all_delisted**

| split | rows | pos | neg | 고유 종목 |
|---|---:|---:|---:|---:|
| train | 39,260 | 1,839 | 37,421 | ~1,322 |
| valid |  7,973 |   288 |  7,685 |   ~283 |
| test  |  8,334 |   448 |  7,886 |   ~285 |

train 불균형 = **20.35 : 1** (PDF는 18.80 : 1)

**only_n1**

| split | rows | pos | neg | 고유 종목 |
|---|---:|---:|---:|---:|
| train | 37,440 | 251 | 37,189 | ~1,320 |
| valid |  7,902 |  69 |  7,833 |   ~283 |
| test  |  8,028 |  58 |  7,970 |   ~282 |

train 불균형 = **148.16 : 1** (≈ PDF v4의 약 8배)

split 간 stock_code 중복은 0 (assert로 검증).

---

## 3. XGBoost 학습 결과

### 3.1 핵심 지표 비교

| variant | thr | valid F1 | test F1 | test Precision | test Recall | test ROC-AUC | test PR-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| **all_delisted** (PDF v4 방식) | 0.820 | **0.3598** | **0.3743** | 0.3853 | 0.3638 | 0.8457 | **0.3342** |
| **only_n1** (우리 fixed_N1 방식) | 0.770 | 0.2537 | 0.2246 | 0.1628 | 0.3621 | 0.8743 | 0.2793 |
| Δ (only_n1 − all_delisted) | −0.05 | −0.1061 | −0.1497 | −0.2225 | −0.0017 | +0.0286 | −0.0549 |

### 3.2 PDF v4 보고서와 직접 비교 (all_delisted 기준)

| 지표 | PDF v4 grid1_02 | 우리 all_delisted | 차이 |
|---|---:|---:|---:|
| threshold | 0.7400 | 0.8200 | +0.08 |
| valid F1 | 0.3983 | 0.3598 | −0.0385 |
| test F1 | 0.3315 | **0.3743** | **+0.0428** |
| test Precision | 0.3214 | 0.3853 | +0.0639 |
| test Recall | 0.3423 | 0.3638 | +0.0215 |
| test ROC-AUC | 0.8092 | 0.8457 | +0.0365 |
| test PR-AUC | 0.2895 | **0.3342** | **+0.0447** |

→ 같은 라벨 정책(전체 delisted = 1) + 같은 모델(XGBoost grid1_02)에서 **우리 환경 점수가 약간 더 높다**. 차이는 (a) 우리가 추가한 거시경제 6개 feature, (b) 우리의 상장일/구조적 상폐 필터로 인한 노이즈 감소, (c) seed 차이에서 기인하는 것으로 보인다.

---

## 4. 분석

### 4.1 라벨 정의가 가장 큰 변수다

`all_delisted → only_n1` 로 라벨만 바꾸었을 때:

- **양성 수가 1/7로 줄고** (2,575 → 378)
- **불균형은 7배 심해지고** (20:1 → 148:1)
- test F1은 **40% 감소** (0.374 → 0.225)
- test Precision은 **58% 감소** (0.385 → 0.163)
- test Recall은 **거의 동일** (0.364 → 0.362)
- test PR-AUC는 16% 감소 (0.334 → 0.279)
- test ROC-AUC는 **오히려 상승** (0.846 → 0.874)

이 패턴이 말하는 것:

1. **모델의 ranking 능력은 only_n1에서 더 좋다** (ROC-AUC ↑, recall 유지). 즉 "정확히 상폐 1년 전" 시점은 신호가 더 강하다.
2. 그러나 **양성이 너무 적어 운영점 F1이 폭락한다** — precision이 무너지면서 false positive 비용이 커진다.
3. recall이 두 variant에서 거의 같은 것은, 두 라벨링 모두 결국 "상폐 1년 전 시점" 행을 핵심 신호로 학습하기 때문이다. all_delisted는 추가로 상폐 2~5년 전 행도 양성으로 보지만, 이는 noise성 양성이 섞이는 효과로 precision은 올라가는 대신 PR-AUC ranking이 살아남는다.

### 4.2 PDF v4와 우리 환경의 점수 격차 해석

우리 all_delisted가 PDF보다 일관되게 좋은 점수가 나온 이유를 분리하면:

| 요인 | 효과 추정 |
|---|---|
| 거시경제 6개 feature 추가 | feature importance top15에 `cpi_yoy`, `gdp_growth_yoy` 포함 → 분명히 기여 |
| 구조적 상폐 5개 제거 (자진상폐 등) | 라벨 노이즈 감소 → precision 향상에 기여 |
| 상장일 필터 (340행) | 종목코드 재사용 데이터 제거 → 신호 정합성 향상 |
| 2015년 이전 제거 (114행) | 영향 미미 |
| seed/split 무작위성 | ±1~2% F1 노이즈 |

이는 **PDF v4 보고서 결과를 단순 재현했을 때 우리 환경이 더 좋게 나올 수 있다**는 것을 보여준다. 즉 PDF의 "test F1 0.3315"는 데이터 정제 추가 만으로도 0.37대로 올릴 여지가 있다.

### 4.3 feature importance 비교

| 순위 | all_delisted | only_n1 |
|---:|---|---|
| 1 | 총자본순이익률 (0.090) | 유보액/납입자본비율 (0.102) |
| 2 | 유보액/납입자본비율 (0.081) | 총자본영업이익률 (0.059) |
| 3 | 총자본영업이익률 (0.071) | 총자산증가율 (0.048) |
| 4 | 총자본투자효율 (0.050) | 매출채권회전율 (0.042) |
| 5 | 매출액순이익률 (0.046) | 매출액순이익률 (0.041) |
| 7 | cpi_yoy (0.029) | — |
| 9 | gdp_growth_yoy (0.025) | — |
| 13 | — | vix_avg (0.021) |

공통 시그널: **수익성 (ROA, 영업이익률, 유보액/납입자본)**. all_delisted에서는 macro의 거시지표가 더 빨리 올라오고, only_n1에서는 성장성/활동성 변수가 더 부각된다 — 양성 시점이 더 좁아서 "상폐 직전 1년의 성장률 변화"가 신호로 살아남는 것으로 해석된다.

### 4.4 우리 기존 exp_009 결과와의 비교 (only_n1 기준)

| 출처 | split 정책 | 라벨 | 모델 | feature | test F1 | test PR-AUC |
|---|---|---|---|---|---:|---:|
| exp_009 (RF best, A+C) | Temporal (2015–22 / 23 / 24) | N1 정밀 | RF | drop_collinear+signed_log1p | 0.2791 | 0.2841 |
| **exp_010 (only_n1)** | **Group** | **N1 정밀** | **XGBoost** | raw + macro | **0.2246** | **0.2793** |

같은 N1 정밀 라벨에서 **Group Split + XGBoost**가 **Temporal Split + RF**보다 F1은 낮고 PR-AUC는 비슷. 즉 단순히 split 정책을 PDF식으로 바꾼다고 우리 모델이 좋아지지 않는다. 우리 기존 `exp_008 RF signed_log1p (test F1 0.3559)`가 여전히 최강.

### 4.5 threshold가 0.77–0.82로 높은 이유

`scale_pos_weight`를 20~148로 크게 잡으면 sigmoid 출력이 전체적으로 위로 끌려 올라가서 best threshold도 자연스럽게 높아진다. PDF에서도 grid1_02 threshold가 0.74였던 것과 같은 현상. 차용 시 **PDF의 threshold 숫자를 그대로 가져오면 안 되고**, 본인 validation에서 다시 잡아야 한다는 결론이 재확인된다.

---

## 5. 결론

1. **라벨 정의가 점수 차이의 주범**이다. 같은 모델·split·하이퍼파라미터에서도 라벨링만 바꾸면 F1이 40% 변한다. PDF v4와 우리 fixed_N1을 점수만으로 비교하면 안 된다.

2. **PDF v4의 단순 라벨링은 운영점 F1이 더 좋게 나오는 라벨 노이즈 효과**가 있다. 양성 시점을 넓게 잡으면 precision이 올라가고 PR-AUC가 높아진다. 다만 이는 "상폐 1년 전 시점을 정확히 예측"하는 운영 목표와 다르다 — 평가가 쉬워지는 것뿐.

3. **우리 환경에서 PDF 세팅 그대로 재현해도 PDF 점수보다 ~10% 좋다** (test F1 0.374 vs 0.331, PR-AUC 0.334 vs 0.290). 거시경제 + 구조적 상폐 제거 + 상장일 필터 만으로 얻은 마진. 즉 PDF 점수는 "데이터 정제 부족"한 상태의 baseline.

4. **운영 후보 변경 없음**: 정확한 상폐 예측이 목표라면 우리 기존 best (`exp_008 RF fixed_N1 signed_log1p`, test F1 0.3559) 가 여전히 유효. all_delisted 방식의 0.374는 "더 쉬운 문제"에서 나온 숫자이지 더 좋은 모델이 아니다.

5. **다음 단계 권고**:
   - PDF의 하이퍼파라미터 그리드 24개를 우리 only_n1에 그대로 돌려서 우리 정밀 라벨에 맞는 best XGBoost를 찾기
   - 거시경제 feature를 우리 기존 `fixed_N1` 파이프라인에도 backport (효과 확인됨)
   - threshold를 F1뿐 아니라 "recall ≥ 0.45" 제약 하 precision 최대 기준으로도 비교 — PDF "다음 실험 방향"과 동일 권고

---

## 6. 산출물

| 경로 | 내용 |
|---|---|
| `preprocess/build_v4_replica.py` | 신규 전처리 스크립트 (raw → split CSV) |
| `src/modeling/train_xgb_v4_replica.py` | 신규 XGBoost 학습 스크립트 |
| `preprocess/data/processed/v4_replica/all_delisted/` | all_delisted variant 데이터 (train/valid/test.csv + meta.json) |
| `preprocess/data/processed/v4_replica/only_n1/` | only_n1 variant 데이터 |
| `results/exp_010_v4_replica/all_delisted.json` | all_delisted XGBoost 학습 결과 |
| `results/exp_010_v4_replica/only_n1.json` | only_n1 XGBoost 학습 결과 |
| `results/exp_010_v4_replica/summary.md` | 본 보고서 |

기존 파일은 모두 그대로 유지되었다 (`grep -l "v4_replica"` 결과 신규 파일에만 존재).
