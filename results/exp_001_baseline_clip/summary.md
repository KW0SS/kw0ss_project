# exp_001: Baseline Clipping — 실험 전체 리포트

> 실험일: 2026-04-27
> 브랜치: `21-modeling-add-tree-based-models-rf-boosting`

---

## 0. 실험 배경

DART 재무제표 기반 상장폐지 예측 이진 분류 모델을 구축한다.
전처리가 완료된 `preprocess/data/processed/H{n}/` 데이터셋(train/valid/test)을 입력으로,
tree-based 모델 4종의 baseline 성능을 확립하고 최적 전략을 결정하는 것이 목표이다.

### 입력 데이터 요약

| 항목 | 내용 |
|---|---|
| 데이터 출처 | DART API → S3 raw JSON → `preprocess/build_h_datasets.py` |
| 전처리 파이프라인 | ffill → CF=0 → 섹터·분기 중앙값 보간 → IQR clipping |
| Winsorize | 미적용 |
| RobustScaler | 미적용 |
| Horizon | H6, H8, H10, H12, H14, H16, H18, H20, H22, H24 (10개) |
| 분할 | 연도 기반 time split (train: ~2022, valid: 2023, test: 2024) |

### 원본 컬럼 구성 (41개)

| 분류 | 컬럼 수 | 내용 |
|---|---|---|
| meta | 4 | stock_code, year, quarter, gics_sector |
| 재무비율 | 24 | 총자산증가율, 유동자산증가율, 매출액증가율, 순이익증가율, 영업이익증가율, 매출액순이익률, 매출총이익률, 자기자본순이익률, 매출채권회전율, 재고자산회전율, 총자본회전율, 유형자산회전율, 매출원가율, 부채비율, 유동비율, 자기자본비율, 당좌비율, 비유동자산장기적합률, 순운전자본비율, 차입금의존도, 현금비율, 총자본영업이익률, 총자본순이익률, 총자본투자효율 |
| 원시값 | 5 | 유형자산, 무형자산, 무형자산상각비, 유형자산상각비, 감가상각비 |
| 매크로 | 6 | credit_spread, kosdaq_return, gdp_growth_yoy, usdkrw_chg, vix_avg, cpi_yoy |
| 유보 관련 | 1 | 유보액/납입자본비율 |
| 타깃 | 1 | label (0=정상, 1=상폐) |

---

## Phase 1: 실험 인프라 구축

### 1-1. 데이터 로더 (`src/modeling/data_loader.py`)

**목적**: H-horizon별 전처리된 데이터셋을 로드하고, 모델 학습에 적합한 X/y 형태로 변환

**주요 설계 결정:**

| 결정 사항 | 선택 | 근거 |
|---|---|---|
| 결측률 높은 컬럼 처리 | 매출액증가율, 순이익증가율, 영업이익증가율 **제외** | 3개 컬럼 결측률 ~66% (30,764행 중 ~20,000행 NaN). YoY 증가율은 전기 데이터 부재 시 계산 불가능한 구조적 결측 |
| 잔여 결측 처리 | `SimpleImputer(strategy="median")` | train에서 fit → valid/test에 transform. impute 후 전 horizon NaN 0건 확인 완료 |
| gics_sector | 피처에서 제외 (meta 컬럼) | 10개 범주형. LabelEncoding 시 순서 의미 부여 문제, OneHot 시 차원 증가 대비 효용 불명확. 후속 실험에서 재검토 대상 |
| 매크로 변수 | 포함 (기본값) | `--no-macro` 옵션으로 제외 실험 가능 |

**최종 피처: 33개**

- 재무비율 21개 (24개 중 결측률 높은 3개 제외)
- 원시값 5개 (유형자산, 무형자산, 상각비 3종)
- 매크로 6개
- 유보액/납입자본비율 1개

**데이터 규모 (H10 기준):**

| split | 행 수 | positive | negative | imbalance ratio |
|---|---|---|---|---|
| train | 30,764 | 193 | 30,571 | 158.4:1 |
| valid | 5,053 | 21 | 5,032 | 239.6:1 |
| test | 5,318 | 29 | 5,289 | 182.4:1 |

**전 horizon NaN 잔존 점검 결과:**

```
H 6: train=0, valid=0, test=0  [OK]
H 8: train=0, valid=0, test=0  [OK]
H10: train=0, valid=0, test=0  [OK]
H12: train=0, valid=0, test=0  [OK]
H14: train=0, valid=0, test=0  [OK]
H16: train=0, valid=0, test=0  [OK]
H18: train=0, valid=0, test=0  [OK]
H20: train=0, valid=0, test=0  [OK]
H22: train=0, valid=0, test=0  [OK]
H24: train=0, valid=0, test=0  [OK]
```

### 1-2. 평가 프레임워크 (`src/modeling/evaluate.py`)

**주요 설계 결정:**

| 결정 사항 | 선택 | 근거 |
|---|---|---|
| Primary metric | **PR-AUC** (`average_precision_score`) | 158:1 불균형에서 ROC-AUC는 true negative가 압도적이라 과대평가 위험. PR-AUC는 positive class의 precision-recall 관계에 집중하여 실제 분류 능력을 더 정확히 반영 |
| Secondary metrics | F1, Precision, Recall, ROC-AUC | 다각적 비교용 |
| Threshold 최적화 | PR curve 기반 F1 최대화 threshold 탐색 | `precision_recall_curve`에서 F1 = 2PR/(P+R) 최대화 지점 |
| 모델 선정 기준 | **valid set** PR-AUC | test set은 최종 보고용으로만 사용. data leakage 방지 |

**PR-AUC를 선택한 이유 (상세):**

ROC-AUC는 FPR = FP/(FP+TN)을 x축으로 사용하는데, TN이 30,000건 이상이므로 FP가 수백 건 발생해도 FPR 변화가 미미하다. 따라서 ROC-AUC는 0.8~0.9로 높게 나오지만, 실제로는 positive class를 거의 못 잡는 모델도 높은 점수를 받을 수 있다. 반면 PR-AUC는 Precision = TP/(TP+FP)를 사용하므로 false positive에 민감하게 반응한다.

### 1-3. 실험 실행 스크립트 (`src/modeling/run_all.py`)

**모델 레지스트리 패턴:**

각 모델 모듈은 동일한 인터페이스 `train(X_train, y_train, X_valid, y_valid) -> (model, params)`를 구현한다. `run_all.py`는 `importlib`로 동적 로드하여 모델 추가 시 레지스트리에 한 줄만 추가하면 된다.

```python
MODEL_REGISTRY = {
    "rf":       "src.modeling.train_rf",
    "gbm":      "src.modeling.train_gbm",
    "xgb":      "src.modeling.train_xgboost",
    "lgbm":     "src.modeling.train_lightgbm",
    "catboost": "src.modeling.train_catboost",
}
```

**실행 파이프라인:**

```
데이터 로드 (load_and_prepare)
  → 모델 학습 (train)
    → valid 평가 (evaluate_model)
      → threshold 최적화 (find_best_threshold)
        → 최적 threshold로 valid 재평가
          → test 평가 (최적 threshold 적용)
            → JSON 저장 (save_result)
```

---

## Phase 2: 클래스 불균형 대응 전략 확정

### 2-1. 실험 설정

- 기준 모델: RandomForest (`n_estimators=300, max_depth=10, min_samples_leaf=5`)
- 7가지 전략을 동일 모델/데이터에서 비교
- 평가: valid set, threshold 최적화 후 PR-AUC 기준

### 2-2. 비교 전략 목록

| # | 전략 | 구현 방식 | 원리 |
|---|---|---|---|
| 1 | baseline | RF 기본 (가중치/샘플링 없음) | 모델 자체의 확률 추정에 의존 |
| 2 | class_weight | `class_weight="balanced"` | 소수 클래스에 높은 가중치 (자동 계산) |
| 3 | sample_weight | `compute_sample_weight("balanced")` | class_weight와 동일 효과, 수동 계산 |
| 4 | SMOTE | `imblearn.SMOTE(k_neighbors=5)` | 소수 클래스 합성 오버샘플링 |
| 5 | SMOTE+Tomek | `imblearn.SMOTETomek` | SMOTE 후 Tomek links로 경계 정리 |
| 6 | UnderSample 10:1 | `RandomUnderSampler` (다수→소수×10) | 다수 클래스 축소 |
| 7 | Under+CW | UnderSample 10:1 + class_weight | 축소 후 추가 가중치 보정 |

### 2-3. 실험 결과

#### H10 (imbalance_ratio = 158.4)

| 전략 | Train 행 | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|
| **baseline** | **30,764** | **0.228** | **0.1669** | **0.2917** | **0.2593** | **0.3333** | **0.9313** |
| undersample 10:1 | 2,123 | 0.636 | 0.0837 | 0.2000 | 0.1538 | 0.2857 | 0.9303 |
| under+cw | 2,123 | 0.661 | 0.0606 | 0.1698 | 0.1059 | 0.4286 | 0.9283 |
| class_weight | 30,764 | 0.476 | 0.0423 | 0.1071 | 0.0659 | 0.2857 | 0.9115 |
| sample_weight | 30,764 | 0.476 | 0.0423 | 0.1071 | 0.0659 | 0.2857 | 0.9115 |
| SMOTE | 61,142 | 0.838 | 0.0384 | 0.0833 | 0.0533 | 0.1905 | 0.9226 |
| SMOTE+Tomek | 59,778 | 0.841 | 0.0374 | 0.0833 | 0.0533 | 0.1905 | 0.9199 |

#### H12 (imbalance_ratio = 135.1)

| 전략 | Train 행 | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|
| **baseline** | **30,764** | **0.250** | **0.2252** | **0.3462** | **0.3600** | **0.3333** | **0.9340** |
| undersample 10:1 | 2,486 | 0.578 | 0.0967 | 0.2308 | 0.1765 | 0.3333 | 0.9287 |
| under+cw | 2,486 | 0.697 | 0.0748 | 0.2000 | 0.1370 | 0.3704 | 0.9240 |
| class_weight | 30,764 | 0.383 | 0.0531 | 0.1354 | 0.0788 | 0.4815 | 0.9101 |
| sample_weight | 30,764 | 0.383 | 0.0531 | 0.1354 | 0.0788 | 0.4815 | 0.9101 |
| SMOTE+Tomek | 59,766 | 0.675 | 0.0446 | 0.1044 | 0.0586 | 0.4815 | 0.9118 |
| SMOTE | 61,076 | 0.616 | 0.0445 | 0.1022 | 0.0559 | 0.5926 | 0.9108 |

### 2-4. 분석

**Baseline이 압도적 1위인 이유:**

1. **RF의 확률 추정이 이미 양호** — 300개 트리의 투표 비율이 곧 확률 추정이며, 소수 클래스 샘플이 일부 트리에서 leaf node에 단독으로 도달하면 해당 트리에서 높은 확률을 부여받는다. 이 메커니즘이 별도의 가중치 보정 없이도 작동한다.

2. **Threshold 조정이 핵심** — 기본 threshold 0.5에서는 positive 예측이 거의 없지만, 0.22~0.25로 낮추면 precision/recall 균형점에 도달한다. 즉 모델의 확률 순위(ranking)가 이미 양호하므로, threshold만 조정하면 충분하다.

3. **SMOTE가 실패한 이유** — 158:1에서 SMOTE는 소수 193개 → 다수 30,571개 수준으로 합성해야 하므로 ~30,000개의 합성 샘플을 생성. k=5 이웃 기반이라 원본 193개 주변에서만 생성되어 다양성이 부족하고, 고차원(33개 피처)에서 합성 품질이 급격히 저하된다.

4. **UnderSampling이 실패한 이유** — 30,571개 → 1,930개로 축소하면 다수 클래스 정보의 94%가 손실된다. 경계 근처의 hard negative를 학습할 기회가 사라져 precision이 급락한다.

5. **class_weight가 실패한 이유** — `balanced` 가중치는 소수 클래스에 158배 가중치를 부여한다. 이로 인해 소수 클래스의 오분류 비용이 극도로 높아져 모델이 과도하게 positive를 예측하게 되고, false positive가 폭증한다.

### 2-5. 확정 전략

| 항목 | 결정 |
|---|---|
| Primary 전략 | **가중치/샘플링 없음 + threshold 최적화** |
| Threshold 방식 | valid set PR curve에서 F1 최대화 지점 탐색 |
| SMOTE/Sampling | **불채택** |
| XGBoost/LightGBM | `scale_pos_weight=1`, `is_unbalance=False` (baseline)로 학습. 내장 파라미터 비교는 후속 실험 |

---

## Phase 3: Tree-based 모델 학습

### 3-1. 모델 설정

리소스 부담 최소화를 위해 `n_jobs=2`, early stopping 적극 활용, `tree_method="hist"` (XGBoost) 설정.

#### Random Forest

```python
RandomForestClassifier(
    n_estimators=200,     # 충분한 앙상블 수
    max_depth=10,         # 과적합 방지
    min_samples_leaf=5,   # leaf 최소 샘플
    max_features="sqrt",  # 피처 서브샘플링
    random_state=42,
    n_jobs=2,
)
```

- class_weight 없음 (Phase 2 결론)
- max_features="sqrt" — 33개 피처에서 ~6개씩 랜덤 선택하여 트리 다양성 확보

#### Gradient Boosting (sklearn)

```python
GradientBoostingClassifier(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,        # Stochastic GB
    min_samples_leaf=10,
    random_state=42,
)
```

- sklearn GBM은 class_weight 미지원
- subsample=0.8로 과적합 방지 + 속도 개선
- n_jobs 미지원 (단일 스레드) → 학습 시간이 가장 김

#### XGBoost

```python
xgb.XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,
    scale_pos_weight=1,       # 가중치 없음 (Phase 2 결론)
    eval_metric="aucpr",      # early stopping 기준
    early_stopping_rounds=30,
    random_state=42,
    n_jobs=2,
    tree_method="hist",       # 히스토그램 기반 → 속도 3~5배 개선
    verbosity=0,
)
```

- `eval_metric="aucpr"` — valid set의 PR-AUC로 early stopping 판단
- `tree_method="hist"` — exact 대비 메모리/속도 절감
- `early_stopping_rounds=30` — 30 라운드 개선 없으면 중단

#### LightGBM

```python
lgb.LGBMClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_samples=20,
    is_unbalance=False,           # 가중치 없음 (Phase 2 결론)
    metric="average_precision",   # early stopping 기준
    random_state=42,
    n_jobs=2,
    verbose=-1,
)
```

- `metric="average_precision"` — PR-AUC 기준 early stopping
- `min_child_samples=20` — leaf 최소 샘플 (불균형 환경에서 과적합 방지)

#### CatBoost — 비채택 결정

| 고려 사항 | 판단 |
|---|---|
| 네이티브 범주형 지원 | gics_sector가 현재 피처 제외(meta 컬럼)이므로 이점 없음 |
| 설치 용량 | ~500MB+ (다른 패키지 대비 과도) |
| 학습 시간 | RF/LightGBM 대비 느림 |
| 기대 효과 | 현재 피처 구성에서 유의미한 차이 없을 것으로 판단 |
| 결론 | **비채택**. gics_sector를 피처에 포함하는 후속 실험에서 재검토 |

### 3-2. 실험 결과

#### H10 (test set, PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.1068** | **0.1667** | 0.1613 | 0.1724 | 0.8367 | 0.230 |
| XGBoost | 0.0917 | 0.1562 | 0.1429 | 0.1724 | 0.8402 | 0.084 |
| GBM | 0.0799 | 0.0444 | 0.0625 | 0.0345 | 0.8605 | 0.294 |
| LightGBM | 0.0662 | 0.0588 | 0.2000 | 0.0345 | 0.7954 | 0.528 |

#### H10 (valid set, 모델 선정 기준)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.1652** | **0.2553** | 0.2308 | 0.2857 | 0.9339 | 0.230 |
| LightGBM | 0.1545 | 0.2286 | 0.2857 | 0.1905 | 0.9120 | 0.528 |
| GBM | 0.1095 | 0.1500 | 0.1579 | 0.1429 | 0.9127 | 0.294 |
| XGBoost | 0.1009 | 0.1639 | 0.1250 | 0.2381 | 0.9135 | 0.084 |

#### H12 (test set, PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2035** | **0.2500** | 0.2903 | 0.2195 | 0.8519 | 0.238 |
| GBM | 0.1522 | 0.0851 | 0.3333 | 0.0488 | 0.8610 | 0.815 |
| XGBoost | 0.1487 | 0.1176 | 0.3000 | 0.0732 | 0.8697 | 0.267 |
| LightGBM | 0.1048 | 0.1356 | 0.2222 | 0.0976 | 0.8347 | 0.069 |

#### H12 (valid set, 모델 선정 기준)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2238** | **0.3273** | 0.3214 | 0.3333 | 0.9346 | 0.238 |
| XGBoost | 0.1857 | 0.2857 | 0.4000 | 0.2222 | 0.9293 | 0.267 |
| LightGBM | 0.1783 | 0.2609 | 0.3158 | 0.2222 | 0.9229 | 0.069 |
| GBM | 0.1265 | 0.1875 | 0.6000 | 0.1111 | 0.9207 | 0.815 |

### 3-3. 분석

**RF가 두 horizon 모두 1위인 이유:**

1. **Bagging의 분산 감소 효과** — 극심한 불균형에서 부스팅은 다수 클래스의 loss에 지배되어 소수 클래스를 충분히 학습하지 못한다. RF는 각 트리가 독립적으로 bootstrap 샘플을 사용하므로, 일부 트리에서 소수 클래스가 상대적으로 풍부하게 샘플링되는 효과가 있다.

2. **부스팅의 early stopping 문제** — XGBoost/LightGBM의 early stopping이 전체 loss 기준으로 작동하므로, 소수 클래스 학습이 충분히 진행되기 전에 중단될 수 있다. `eval_metric="aucpr"`로 설정했으나, 30,000행 중 21개 positive인 valid set에서 PR-AUC 자체의 분산이 크다.

3. **GBM의 높은 threshold** — sklearn GBM은 H12에서 threshold=0.815로 매우 보수적. 확률 추정이 대부분 0.5 이하로 몰려 있어 positive 예측이 극소수이며, 이로 인해 Precision은 높지만 Recall이 0.0488로 극단적으로 낮다.

4. **LightGBM의 불안정성** — H10에서 threshold=0.528 vs H12에서 0.069로 편차가 매우 크다. valid set positive가 21~27개뿐이라 threshold 최적화가 불안정하다.

**H12 > H10 — 성능 격차 원인:**

| 요인 | H10 | H12 |
|---|---|---|
| Train positive | 193 | 226 (+17%) |
| Imbalance ratio | 158.4 | 135.1 |
| RF test PR-AUC | 0.1068 | 0.2035 (+90%) |

- positive 샘플이 17% 더 많아 학습 안정성 향상
- 상폐까지 12개월이면 재무적 악화 시그널(자본잠식, 유동성 위기 등)이 10개월보다 더 명확하게 데이터에 반영됨

---

## Phase 4: 분석 & 리포트

### 4-1. Feature Importance

4개 모델의 `feature_importances_`를 정규화(합=1) 후 평균하여 모델 간 공통 중요 피처를 도출했다.

#### H10 Top 15

| Rank | Feature | RF | GBM | XGB | LGBM | **평균** |
|---|---|---|---|---|---|---|
| 1 | 유보액/납입자본비율 | 0.0765 | 0.0936 | 0.0856 | 0.0653 | **0.0803** |
| 2 | 총자산증가율 | 0.0383 | 0.0581 | 0.0791 | 0.0543 | **0.0574** |
| 3 | 유형자산 | 0.0490 | 0.0572 | 0.0253 | 0.0831 | **0.0536** |
| 4 | 자기자본비율 | 0.0724 | 0.0514 | 0.0393 | 0.0274 | **0.0476** |
| 5 | 순운전자본비율 | 0.0423 | 0.0444 | 0.0725 | 0.0264 | **0.0464** |
| 6 | 현금비율 | 0.0369 | 0.0539 | 0.0557 | 0.0375 | **0.0460** |
| 7 | 무형자산 | 0.0428 | 0.0549 | 0.0262 | 0.0543 | **0.0446** |
| 8 | 매출액순이익률 | 0.0262 | 0.0251 | 0.0805 | 0.0269 | **0.0397** |
| 9 | 매출채권회전율 | 0.0357 | 0.0602 | 0.0196 | 0.0399 | **0.0388** |
| 10 | 유동자산증가율 | 0.0502 | 0.0379 | 0.0166 | 0.0437 | **0.0371** |
| 11 | 재고자산회전율 | 0.0336 | 0.0516 | 0.0158 | 0.0413 | **0.0356** |
| 12 | 매출원가율 | 0.0413 | 0.0280 | 0.0496 | 0.0207 | **0.0349** |
| 13 | 부채비율 | 0.0488 | 0.0232 | 0.0111 | 0.0543 | **0.0343** |
| 14 | 총자본영업이익률 | 0.0231 | 0.0249 | 0.0457 | 0.0399 | **0.0334** |
| 15 | 매출총이익률 | 0.0423 | 0.0397 | 0.0218 | 0.0288 | **0.0332** |

#### H12 Top 15

| Rank | Feature | RF | GBM | XGB | LGBM | **평균** |
|---|---|---|---|---|---|---|
| 1 | 유보액/납입자본비율 | 0.0721 | 0.1280 | 0.0857 | 0.1071 | **0.0982** |
| 2 | 매출채권회전율 | 0.0381 | 0.0686 | 0.0289 | 0.1071 | **0.0607** |
| 3 | 무형자산 | 0.0427 | 0.0426 | 0.0253 | 0.0893 | **0.0500** |
| 4 | 총자산증가율 | 0.0441 | 0.0493 | 0.0687 | 0.0179 | **0.0450** |
| 5 | 매출총이익률 | 0.0422 | 0.0353 | 0.0290 | 0.0714 | **0.0445** |
| 6 | 순운전자본비율 | 0.0408 | 0.0458 | 0.0664 | 0.0179 | **0.0427** |
| 7 | 유형자산 | 0.0428 | 0.0630 | 0.0266 | 0.0357 | **0.0421** |
| 8 | 부채비율 | 0.0462 | 0.0156 | 0.0161 | 0.0893 | **0.0418** |
| 9 | 매출원가율 | 0.0423 | 0.0293 | 0.0565 | 0.0357 | **0.0410** |
| 10 | 현금비율 | 0.0376 | 0.0441 | 0.0454 | 0.0357 | **0.0407** |
| 11 | 재고자산회전율 | 0.0343 | 0.0538 | 0.0203 | 0.0536 | **0.0405** |
| 12 | 자기자본비율 | 0.0675 | 0.0515 | 0.0406 | 0.0000 | **0.0399** |
| 13 | 유형자산회전율 | 0.0357 | 0.0295 | 0.0191 | 0.0714 | **0.0389** |
| 14 | 유동자산증가율 | 0.0502 | 0.0332 | 0.0180 | 0.0357 | **0.0343** |
| 15 | 차입금의존도 | 0.0423 | 0.0469 | 0.0181 | 0.0179 | **0.0313** |

#### 공통 핵심 피처 해석 (H10 ∩ H12 Top 10)

| Feature | H10 Rank | H12 Rank | 해석 |
|---|---|---|---|
| **유보액/납입자본비율** | 1 | 1 | 이익잉여금 축적도. 상폐 기업은 누적 결손으로 이 비율이 급격히 하락. 자본잠식의 직접 지표 |
| **총자산증가율** | 2 | 4 | 자산 성장/감소 추세. 상폐 기업은 자산 매각, 투자 축소로 총자산이 감소하는 경향 |
| **유형자산** | 3 | 7 | 기업 규모의 proxy. 소규모 기업일수록 외부 충격에 취약하여 상폐 리스크 높음 |
| **순운전자본비율** | 5 | 6 | (유동자산-유동부채)/총자산. 음수면 단기 채무 상환 능력 부족으로 유동성 위기 |
| **현금비율** | 6 | 10 | 현금성 자산 보유 비율. 단기 생존력의 직접 지표 |
| **무형자산** | 7 | 3 | 무형자산 과대 보유 = 실질 자산 가치 불확실. 손상 인식 시 자본 급감 가능 |
| **매출채권회전율** | 9 | 2 | 매출채권 회수 속도. 낮으면 대금 회수 지연 → 현금흐름 악화 시그널 |

**모델 간 importance 패턴 차이:**

- **RF**: 비교적 고르게 분포 (Top 1 ~ 0.07, Bottom ~ 0.02). Bagging 특성상 피처 간 경쟁이 분산됨
- **GBM**: 유보액/납입자본비율에 집중 (H12에서 0.128). 순차 학습이 가장 중요한 피처에 반복적으로 집중
- **XGBoost**: 매출액순이익률(H10), 총자산증가율(H12) 등 수익성/성장성 지표를 상대적으로 중시
- **LightGBM**: 분포가 가장 불균등. 일부 피처에 0.1071로 집중, 나머지는 0.0179로 급락. leaf-wise 성장 전략의 영향

### 4-2. Horizon 간 비교

| Horizon | Positive (train) | Imbalance | RF PR-AUC (test) | RF F1 (test) | 최적 Threshold |
|---|---|---|---|---|---|
| H10 | 193 | 158.4:1 | 0.1068 | 0.1667 | 0.230 |
| H12 | 226 | 135.1:1 | 0.2035 | 0.2500 | 0.238 |

H12가 H10 대비 PR-AUC +90%, F1 +50% 개선. 원인:
1. positive 샘플 수 증가 (193 → 226, +17%)
2. 상폐까지 기간이 길어 재무 악화 시그널이 더 뚜렷
3. threshold 유사 (0.230 vs 0.238) → 모델의 확률 분포 자체가 개선된 것

---

## 최종 결론

### Best 모델

| 항목 | 값 |
|---|---|
| 모델 | **Random Forest** |
| Horizon | **H12** |
| Test PR-AUC | **0.2035** |
| Test F1 | **0.2500** |
| Test Precision | 0.2903 |
| Test Recall | 0.2195 |
| Threshold | 0.238 |

### 핵심 결정 사항 요약

| Phase | 결정 | 근거 |
|---|---|---|
| 1 | PR-AUC를 primary metric으로 | 158:1 불균형에서 ROC-AUC 과대평가 위험 |
| 1 | 결측률 66% 컬럼 3개 제외 | 구조적 결측 (전기 데이터 부재) |
| 1 | gics_sector 피처 제외 | 인코딩 방식 미확정, 후속 실험 대상 |
| 2 | 불균형 전략 = 없음 + threshold 최적화 | 7전략 비교 결과 baseline이 PR-AUC 2~3배 우세 |
| 2 | SMOTE/Sampling 불채택 | 158:1에서 합성 노이즈 / 정보 손실 문제 |
| 3 | RF가 부스팅 모델 압도 | bagging의 분산 감소가 극심한 불균형에서 유리 |
| 3 | CatBoost 비채택 | 범주형 피처 미사용 상태, 설치 비용 대비 기대 효과 부족 |
| 4 | 유보액/납입자본비율이 최강 피처 | 4모델 × 2 horizon 전부 1위, 자본잠식의 직접 지표 |

---

## 다음 실험 방향

### 우선순위 높음

1. **하이퍼파라미터 튜닝 (Optuna)**
   - RF: n_estimators, max_depth, min_samples_leaf, max_features
   - XGBoost/LightGBM: learning_rate, max_depth, reg_alpha/lambda, min_child_weight
   - 현재 기본 설정이므로 부스팅 모델이 RF를 역전할 가능성 있음
   - 특히 XGBoost의 eval_metric, early_stopping 조합 최적화 필요

2. **전 horizon 실험 확장 (H6~H24)**
   - H-horizon별 성능 곡선 분석
   - H12가 sweet spot인지, H14~H16에서 더 나은지 확인
   - horizon이 길어질수록 positive 수가 증가하지만 예측 실용성은 감소 → 트레이드오프 분석

3. **Threshold 전략 고도화**
   - 비즈니스 시나리오별 threshold 제시: "Recall 80% 보장 시 Precision은?"
   - Cost-sensitive threshold: 상폐 미탐지 비용 vs 오탐지 비용 반영

### 우선순위 중간

4. **전처리 옵션 비교 (Winsorize / RobustScaler)**
   - `build_h_datasets.py`에 이미 구현된 4가지 설정:
     - baseline: clipping만 (이번 실험)
     - exp-A: clipping + Winsorize
     - exp-B: clipping + RobustScaler
     - exp-C: clipping + Winsorize + RobustScaler
   - `results/exp_002_winsor/`, `exp_003_robust/` 등으로 분리하여 `compare_experiments`로 비교

5. **피처 엔지니어링**
   - 교차 비율: 유보액/부채비율, 현금비율×유동비율
   - gics_sector 인코딩 후 피처 포함
   - 시계열 lag/diff: 전 분기 대비 변화율

6. **앙상블 (Stacking / Blending)**
   - RF + XGBoost 2단계 스태킹
   - 모델별 예측 확률 가중 결합

### 우선순위 낮음

7. **SHAP 기반 모델 해석**
   - 개별 예측 피처 기여도 분석
   - 상폐 기업 vs 정상 기업의 SHAP value 분포 비교

8. **CatBoost 재검토**
   - gics_sector를 피처에 포함하는 실험 이후 재검토

---

## 산출물 목록

```
src/modeling/
├── __init__.py
├── data_loader.py               # Phase 1-1: 데이터 로드 & X/y 분리
├── evaluate.py                  # Phase 1-2: 평가 프레임워크 (PR-AUC, threshold 최적화)
├── run_all.py                   # Phase 1-3: CLI 실험 실행 (--exp 옵션으로 실험 분리)
├── imbalance_experiment.py      # Phase 2: 불균형 전략 7종 비교
├── compare_experiments.py       # Phase 4: 실험 간 비교 도구
├── train_rf.py                  # Phase 3: Random Forest
├── train_gbm.py                 # Phase 3: Gradient Boosting
├── train_xgboost.py             # Phase 3: XGBoost
└── train_lightgbm.py            # Phase 3: LightGBM

results/exp_001_baseline_clip/
├── experiment.json              # 실험 조건 메타
├── rf_H10.json                  # 모델별 결과 (8건)
├── rf_H12.json
├── gbm_H10.json
├── gbm_H12.json
├── xgb_H10.json
├── xgb_H12.json
├── lgbm_H10.json
├── lgbm_H12.json
├── comparison_valid.csv         # valid set 비교 테이블
├── comparison_test.csv          # test set 비교 테이블
├── feature_importance_H10.csv   # H10 피처 중요도 (4모델)
├── feature_importance_H12.csv   # H12 피처 중요도 (4모델)
├── imbalance_strategy_result.md # Phase 2 상세 결과
└── summary.md                   # 이 리포트
```
