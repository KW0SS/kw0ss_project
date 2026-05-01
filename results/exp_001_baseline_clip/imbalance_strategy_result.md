# 클래스 불균형 대응 전략 비교 결과

## 실험 설정

- 기준 모델: RandomForest (n_estimators=300, max_depth=10, min_samples_leaf=5)
- 평가: valid set, PR-AUC 기준 (threshold는 F1 최적화 후 적용)
- 대상 horizon: H10 (imbalance_ratio=158.4), H12 (imbalance_ratio=135.1)

## H10 결과

| 전략 | Train 행 | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|
| **baseline (없음)** | 30,764 | 0.228 | **0.1669** | **0.2917** | 0.2593 | 0.3333 | 0.9313 |
| undersample 10:1 | 2,123 | 0.636 | 0.0837 | 0.2000 | 0.1538 | 0.2857 | 0.9303 |
| under + class_weight | 2,123 | 0.661 | 0.0606 | 0.1698 | 0.1059 | 0.4286 | 0.9283 |
| class_weight | 30,764 | 0.476 | 0.0423 | 0.1071 | 0.0659 | 0.2857 | 0.9115 |
| sample_weight | 30,764 | 0.476 | 0.0423 | 0.1071 | 0.0659 | 0.2857 | 0.9115 |
| SMOTE | 61,142 | 0.838 | 0.0384 | 0.0833 | 0.0533 | 0.1905 | 0.9226 |
| SMOTE + Tomek | 59,778 | 0.841 | 0.0374 | 0.0833 | 0.0533 | 0.1905 | 0.9199 |

## H12 결과

| 전략 | Train 행 | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|
| **baseline (없음)** | 30,764 | 0.250 | **0.2252** | **0.3462** | 0.3600 | 0.3333 | 0.9340 |
| undersample 10:1 | 2,486 | 0.578 | 0.0967 | 0.2308 | 0.1765 | 0.3333 | 0.9287 |
| under + class_weight | 2,486 | 0.697 | 0.0748 | 0.2000 | 0.1370 | 0.3704 | 0.9240 |
| class_weight | 30,764 | 0.383 | 0.0531 | 0.1354 | 0.0788 | 0.4815 | 0.9101 |
| sample_weight | 30,764 | 0.383 | 0.0531 | 0.1354 | 0.0788 | 0.4815 | 0.9101 |
| SMOTE + Tomek | 59,766 | 0.675 | 0.0446 | 0.1044 | 0.0586 | 0.4815 | 0.9118 |
| SMOTE | 61,076 | 0.616 | 0.0445 | 0.1022 | 0.0559 | 0.5926 | 0.9108 |

## 분석

### 1. Baseline(없음) + threshold 조정이 압도적 1위
- PR-AUC 기준 2위 대비 2~3배 차이
- RF의 내장 확률 추정이 이미 양호하며, threshold만 낮추면(0.22~0.25) precision/recall 균형 확보 가능

### 2. 샘플링 전략(SMOTE, UnderSampling)은 오히려 성능 저하
- SMOTE: 합성 샘플이 158:1 수준의 극단적 불균형에서 노이즈만 추가
- UnderSampling: 정보 손실이 심해 precision 급락
- 결합(SMOTE+Tomek, Under+CW)도 개선 효과 없음

### 3. class_weight/sample_weight는 recall 개선에만 기여
- Recall은 올라가지만 Precision이 급락하여 F1/PR-AUC 모두 하락
- 극심한 불균형에서는 가중치 보정이 false positive를 과도하게 증가시킴

## 확정 전략

| 항목 | 결정 |
|---|---|
| Primary 전략 | **없음 (baseline RF) + threshold 최적화** |
| Threshold 방식 | valid set PR curve 기반 F1 최적 threshold 탐색 |
| 보조 전략 | XGBoost/LightGBM의 `scale_pos_weight` 개별 실험 (Phase 3에서) |
| SMOTE/Sampling | **불채택** — 극심한 불균형(158:1)에서 효과 없음 |

## Phase 3 적용 방침

1. sklearn RF/GBM: class_weight 없이 학습 → threshold 최적화
2. XGBoost: `scale_pos_weight` 파라미터 ON/OFF 비교
3. LightGBM: `is_unbalance` 파라미터 ON/OFF 비교
4. 모든 모델에 threshold 최적화 적용 (PR curve 기반)
