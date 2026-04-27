# Tree-based 모델링 실험 결과 리포트

## 1. 실험 개요

| 항목 | 내용 |
|---|---|
| 목적 | DART 재무제표 기반 상장폐지 예측 (이진 분류) |
| 데이터 | `preprocess/data/processed/H{n}/` (전처리 완료) |
| 피처 | 33개 (재무비율 21 + 원시값 5 + 매크로 6 + 성장률 1) |
| 실험 horizon | H10 (imbalance 158:1), H12 (imbalance 135:1) |
| 모델 | Random Forest, Gradient Boosting, XGBoost, LightGBM |
| 불균형 전략 | baseline + threshold 최적화 (Phase 2에서 확정) |
| Primary metric | PR-AUC (극심한 불균형에서 ROC-AUC보다 신뢰적) |

---

## 2. 모델별 성능 비교

### Test Set (PR-AUC 순)

#### H10 (상폐까지 10개월 이전 예측)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.1068** | **0.1667** | 0.1613 | 0.1724 | 0.8367 | 0.230 |
| XGBoost | 0.0917 | 0.1562 | 0.1429 | 0.1724 | 0.8402 | 0.084 |
| GBM | 0.0799 | 0.0444 | 0.0625 | 0.0345 | 0.8605 | 0.294 |
| LightGBM | 0.0662 | 0.0588 | 0.2000 | 0.0345 | 0.7954 | 0.528 |

#### H12 (상폐까지 12개월 이전 예측)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.2035** | **0.2500** | 0.2903 | 0.2195 | 0.8519 | 0.238 |
| GBM | 0.1522 | 0.0851 | 0.3333 | 0.0488 | 0.8610 | 0.815 |
| XGBoost | 0.1487 | 0.1176 | 0.3000 | 0.0732 | 0.8697 | 0.267 |
| LightGBM | 0.1048 | 0.1356 | 0.2222 | 0.0976 | 0.8347 | 0.069 |

### Valid Set (PR-AUC 순, 모델 선정 기준)

| Model | Horizon | PR-AUC | F1 | ROC-AUC |
|---|---|---|---|---|
| RF | H12 | 0.2238 | 0.3273 | 0.9346 |
| XGBoost | H12 | 0.1857 | 0.2857 | 0.9293 |
| LightGBM | H12 | 0.1783 | 0.2609 | 0.9229 |
| RF | H10 | 0.1652 | 0.2553 | 0.9339 |
| LightGBM | H10 | 0.1545 | 0.2286 | 0.9120 |
| GBM | H12 | 0.1265 | 0.1875 | 0.9207 |
| GBM | H10 | 0.1095 | 0.1500 | 0.9127 |
| XGBoost | H10 | 0.1009 | 0.1639 | 0.9135 |

---

## 3. 핵심 분석

### 3-1. RF가 두 horizon 모두 1위

Random Forest가 PR-AUC 기준으로 H10/H12 모두 최고 성능을 기록했다.

**원인 분석:**
- 극심한 불균형(158:1)에서 부스팅 모델은 다수 클래스에 편향되기 쉬움
- RF의 bagging 메커니즘이 소수 클래스 샘플을 다양한 서브트리에서 학습하여 일반화에 유리
- 부스팅 모델의 early stopping이 PR-AUC 기준이 아닌 경우 소수 클래스 학습이 조기 종료될 수 있음

### 3-2. H12 > H10 — 예측 horizon이 길수록 성능 개선

| Horizon | RF PR-AUC (test) | RF F1 (test) |
|---|---|---|
| H10 | 0.1068 | 0.1667 |
| H12 | 0.2035 | 0.2500 |

- H12가 H10 대비 PR-AUC 약 1.9배 향상
- 상폐까지 기간이 길수록 재무적 악화 시그널이 데이터에 더 많이 반영됨
- positive 샘플 수도 H12(226개)가 H10(193개)보다 많아 학습 안정성 향상

### 3-3. 불균형 전략: baseline + threshold 최적화가 최적

Phase 2에서 7가지 전략을 비교한 결과:
- SMOTE, UnderSampling, class_weight 모두 baseline 대비 성능 저하
- Threshold 조정(0.23~0.25)만으로 Precision/Recall 균형 확보 가능
- 158:1 수준의 극단적 불균형에서는 샘플링이 오히려 노이즈를 추가

---

## 4. Feature Importance (4개 모델 평균)

### H10 Top 10

| Rank | Feature | Avg Importance | 해석 |
|---|---|---|---|
| 1 | 유보액/납입자본비율 | 0.0803 | 이익잉여금 축적 → 재무 건전성 핵심 지표 |
| 2 | 총자산증가율 | 0.0574 | 자산 성장세 → 상폐 기업은 자산 감소 경향 |
| 3 | 유형자산 | 0.0536 | 자산 규모 → 소규모 기업 상폐 리스크 높음 |
| 4 | 자기자본비율 | 0.0476 | 자본 안정성 → 자본잠식 직결 |
| 5 | 순운전자본비율 | 0.0464 | 단기 유동성 → 운전자본 부족 = 유동성 위기 |
| 6 | 현금비율 | 0.0460 | 현금 보유 → 단기 생존력 |
| 7 | 무형자산 | 0.0446 | 자산 구성 → 무형자산 과대 = 리스크 |
| 8 | 매출액순이익률 | 0.0397 | 수익성 → 지속 적자 = 상폐 시그널 |
| 9 | 매출채권회전율 | 0.0388 | 영업 효율성 → 매출채권 회수 지연 = 위험 |
| 10 | 유동자산증가율 | 0.0371 | 유동성 변화 추세 |

### H12 Top 10

| Rank | Feature | Avg Importance | 해석 |
|---|---|---|---|
| 1 | 유보액/납입자본비율 | 0.0982 | H10과 동일하게 1위 — 가장 강력한 상폐 시그널 |
| 2 | 매출채권회전율 | 0.0607 | H12에서 중요도 상승 — 장기 영업 효율성 반영 |
| 3 | 무형자산 | 0.0500 | |
| 4 | 총자산증가율 | 0.0450 | |
| 5 | 매출총이익률 | 0.0445 | H12에서 순위 상승 — 장기 수익 구조 반영 |
| 6 | 순운전자본비율 | 0.0427 | |
| 7 | 유형자산 | 0.0421 | |
| 8 | 부채비율 | 0.0418 | |
| 9 | 매출원가율 | 0.0410 | |
| 10 | 현금비율 | 0.0407 | |

### 공통 핵심 피처 (H10 ∩ H12 Top 10)

두 horizon에서 모두 Top 10에 포함된 피처 (모델 4개 평균 기준):

1. **유보액/납입자본비율** — 양쪽 1위, 상폐 예측의 가장 강력한 지표
2. **총자산증가율** — 자산 성장/감소 추세
3. **유형자산** — 기업 규모 proxy
4. **순운전자본비율** — 단기 유동성
5. **현금비율** — 현금 보유력
6. **무형자산** — 자산 구성의 질
7. **매출채권회전율** — 영업 효율성

---

## 5. 모델별 하이퍼파라미터

### RF (최적 모델)
```
n_estimators=200, max_depth=10, min_samples_leaf=5,
max_features="sqrt", n_jobs=2
```

### XGBoost
```
n_estimators=300, max_depth=6, learning_rate=0.05,
subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
scale_pos_weight=1, eval_metric="aucpr",
early_stopping_rounds=30, tree_method="hist"
```

### LightGBM
```
n_estimators=300, max_depth=6, learning_rate=0.05,
subsample=0.8, colsample_bytree=0.8, min_child_samples=20,
is_unbalance=False, metric="average_precision",
early_stopping=30
```

### GBM (sklearn)
```
n_estimators=200, max_depth=5, learning_rate=0.05,
subsample=0.8, min_samples_leaf=10
```

---

## 6. 다음 실험 방향

### 우선순위 높음

1. **하이퍼파라미터 튜닝 (Optuna)**
   - RF: n_estimators, max_depth, min_samples_leaf, max_features
   - XGBoost/LightGBM: learning_rate, max_depth, reg_alpha/lambda
   - 현재 기본 설정이므로 튜닝으로 부스팅 모델이 RF를 역전할 가능성 있음

2. **전 horizon 실험 확장 (H6~H24)**
   - H-horizon별 성능 변화 곡선 분석
   - 최적 예측 시점 확인 (H12가 sweet spot인지 H14~H16이 더 나은지)

3. **Threshold 전략 고도화**
   - PR curve 기반 운영 시나리오별 threshold 제시
   - "Recall 80% 보장 시 Precision은?" 등 비즈니스 요구사항 대응

### 우선순위 중간

4. **피처 엔지니어링**
   - 교차 비율: 유보액/부채비율, 현금비율×유동비율 등
   - 시계열 lag/diff: 전 분기 대비 변화율
   - gics_sector 인코딩 후 피처로 포함 실험

5. **앙상블 (Stacking/Blending)**
   - RF + XGBoost 2단계 스태킹
   - 다양한 모델의 예측 확률 결합

6. **SHAP 기반 모델 해석**
   - 개별 예측에 대한 피처 기여도 분석
   - 상폐 기업 vs 정상 기업의 SHAP value 분포 비교

### 우선순위 낮음

7. **전처리 옵션 비교 (Winsorize/RobustScaler)**
   - `build_h_datasets.py`에 이미 구현된 4가지 설정 비교
   - 최적 모델(RF)에 대해서만 실험

8. **CatBoost 재검토**
   - gics_sector를 피처에 포함하는 경우 네이티브 범주형 처리 이점 발생
   - 피처 엔지니어링(#4) 이후 재검토

---

## 7. 산출물 목록

```
src/modeling/
├── __init__.py
├── data_loader.py               # 데이터 로드 & X/y 분리
├── evaluate.py                  # 평가 프레임워크
├── train_rf.py                  # Random Forest
├── train_gbm.py                 # Gradient Boosting
├── train_xgboost.py             # XGBoost
├── train_lightgbm.py            # LightGBM
├── imbalance_experiment.py      # 불균형 전략 비교
└── run_all.py                   # CLI 실행 스크립트

results/
├── comparison_valid.csv         # valid set 모델 비교
├── comparison_test.csv          # test set 모델 비교
├── feature_importance_H10.csv   # H10 피처 중요도
├── feature_importance_H12.csv   # H12 피처 중요도
├── imbalance_strategy_result.md # 불균형 전략 비교 결과
├── rf_H10.json                  # 개별 모델 결과 (8건)
├── rf_H12.json
├── gbm_H10.json
├── gbm_H12.json
├── xgb_H10.json
├── xgb_H12.json
├── lgbm_H10.json
├── lgbm_H12.json
└── summary.md                   # 이 리포트
```
