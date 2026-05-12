
---

# 2. `ml-training-strategy/templates/training-strategy-template.md`

```md
# 모델 학습 전략

## 1. 데이터 개요

### 1-1. 데이터셋 요약

- 데이터셋 이름:
- 데이터 기간:
- 예측 대상:
- target/label 정의:
- positive 기준:
- negative 기준:
- horizon:
- window 사용 여부:
- split 방식:
- 주요 feature 그룹:

### 1-2. 전처리 의도 요약

- 전처리 목적:
- EDA에서 확인된 주요 특징:
- feature 구성 의도:
- horizon/window 구성 의도:

### 1-3. 학습 전 확인 필요 사항

- label 생성 방식:
- train/valid/test 분할 기준:
- positive/negative 비율:
- feature 사용 가능 시점:
- 전처리 fit/apply 기준:
- leakage 가능성:

---

## 2. 학습 실험 후보

### Experiment 1. Baseline 모델 학습

- 목적:
- 사용 데이터:
- 모델 후보:
  - Logistic Regression
  - Random Forest
  - LightGBM
  - XGBoost
- 주요 평가 지표:
  - PR-AUC
  - ROC-AUC
  - Precision
  - Recall
  - F1
- 확인할 점:

### Experiment 2. Tabular 모델 비교

- 목적:
- 사용 데이터:
- 모델 후보:
  - Logistic Regression
  - Random Forest
  - XGBoost
  - LightGBM
- 주요 평가 지표:
  - PR-AUC
  - Precision
  - Recall
  - F1
  - ROC-AUC
- 확인할 점:

### Experiment 3. 전처리 버전 비교

- 목적:
- 비교 대상:
- 모델 후보:
- 주요 평가 지표:
- 확인할 점:

### Experiment 4. Horizon별 비교

- 목적:
- 비교 대상:
  - H10
  - H12
  - H14
  - H16
- 모델 후보:
- 주요 평가 지표:
- 확인할 점:

### Experiment 5. Window Feature 기반 학습

- 목적:
- 사용 feature:
- 모델 후보:
- 주요 평가 지표:
- 확인할 점:

### Experiment 6. Sequence Model 학습

- 목적:
- 입력 데이터:
- 모델 후보:
  - LSTM
  - GRU
  - Temporal CNN
  - Transformer Encoder
- 주요 평가 지표:
- 확인할 점:

### Experiment 7. Class Imbalance 처리 방식 비교

- 목적:
- 비교 방법:
  - class_weight
  - scale_pos_weight
  - undersampling
  - threshold tuning
  - focal loss
- 모델 후보:
- 주요 평가 지표:
- 확인할 점:

### Experiment 8. Threshold / High-risk Bucket 분석

- 목적:
- 분석 방법:
  - threshold별 Precision/Recall/F1
  - Precision@K
  - Recall@K
  - Lift@K
  - high-risk bucket별 positive rate
- 확인할 점:

### Experiment 9. Sector / 기간별 성능 분석

- 목적:
- 분석 대상:
- 주요 평가 지표:
- 확인할 점:

### Experiment 10. Feature Ablation

- 목적:
- 비교 방법:
- 모델 후보:
- 주요 평가 지표:
- 확인할 점:

---

## 3. 실험 우선순위

### Priority 1. 먼저 수행할 실험

- Baseline 모델 학습
- Tabular 모델 비교
- Class Imbalance 처리 방식 비교

### Priority 2. 데이터 구조상 바로 가능한 실험

- Horizon별 비교
- 전처리 버전 비교
- Threshold / High-risk Bucket 분석

### Priority 3. 추가 데이터 정리가 필요한 실험

- Window Feature 기반 학습
- Sequence Model 학습
- Sector / 기간별 성능 분석
- Feature Ablation

---

## 4. 실험별 산출물

| 실험 | 주요 산출물 |
|---|---|
| Baseline 모델 학습 | 기본 성능표, confusion matrix |
| Tabular 모델 비교 | 모델별 성능 비교표 |
| 전처리 버전 비교 | 전처리 버전별 성능 변화표 |
| Horizon별 비교 | horizon별 성능 비교표 |
| Window Feature 실험 | window feature 사용 전후 비교 |
| Sequence Model 실험 | tabular baseline 대비 성능 비교 |
| Threshold 분석 | threshold별 Precision/Recall/F1 |
| High-risk Bucket 분석 | top-k bucket별 positive capture rate |
| Sector / 기간별 분석 | sector/기간별 성능표 |
| Feature Ablation | feature group별 성능 기여도 |

---

## 5. 주의사항

- 상장폐지 예측은 positive 수가 적기 때문에 Accuracy 중심 평가는 적절하지 않다.
- PR-AUC, Precision, Recall, F1을 중심으로 비교한다.
- horizon별 비교 시 positive 수와 데이터 기간 차이를 확인한다.
- window나 sequence 데이터를 만들 때 미래 정보가 포함되지 않도록 확인한다.
- threshold는 valid set에서 정하고 test set에서는 고정해서 평가하는 것이 바람직하다.
- LSTM 등 복잡한 모델은 tabular baseline을 만든 뒤 비교한다.