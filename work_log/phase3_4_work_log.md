# Phase 3 & 4 작업 수행 기록

작업일: 2026-04-08

---

## 사전 작업: requirements.txt 패키지 추가

**파일**: `requirements.txt`

Phase 3, 4에서 필요한 패키지 3개를 추가했다.

| 패키지 | 버전 | 용도 |
|---|---|---|
| `scikit-learn` | >=1.3.0 | baseline 모델 학습/평가 |
| `matplotlib` | >=3.7.0 | 시각화 |
| `seaborn` | >=0.12.0 | 시각화 |

---

## Phase 3: EDA 템플릿 노트북

**산출물**: `notebooks/eda_template.ipynb`

`src/analysis/utils.py`의 함수들을 import하여 `clean_data`를 즉시 분석할 수 있는 8개 섹션 구성의 노트북을 작성했다.

### 노트북 섹션 구성

| # | 섹션 | 사용 함수 / 내용 |
|---|---|---|
| 1 | 설정 & 데이터 로드 | `load_csv()`, `df.shape`, `df.head()`, 한글 폰트 설정 |
| 2 | 컬럼 분류 | `classify_columns()` → meta/ratio/raw_value/macro 4분류 출력 |
| 3 | 기본 통계 | `summarize_dataframe()`, `df.describe().T` |
| 4 | 결측 분석 | `missing_summary()`, `high_missing_columns(threshold=0.3)`, `plot_missing_heatmap()` |
| 5 | 라벨 분포 | label value_counts, 불균형 비율 계산, gics_sector별 label 분포 stacked bar chart |
| 6 | 단변량 분포 | 주요 비율 10개 선정 → `analyze_single_feature()`, `plot_histogram()`, `plot_boxplot_by_label()`, `compare_group_stats_by_label()` |
| 7 | 상관관계 | `plot_correlation_heatmap()`, `get_high_corr_pairs(threshold=0.8)`, 다중공선성 제거 후보 자동 추출 |
| 8 | Baseline 모델 | Phase 4 함수 import → `time_split` → LogisticRegression + DecisionTree 학습/평가 |

### 단변량 분석 대상 피처 (10개)

`부채비율`, `유동비율`, `자기자본비율`, `매출액순이익률`, `자기자본순이익률`, `총자본영업이익률`, `총자본회전율`, `차입금의존도`, `매출총이익률`, `현금비율`

### 데이터 경로

기본값은 `preprocess/data/output/clean_data_no_macro.csv`이며, 매크로 포함 데이터는 주석 전환으로 사용 가능하다.

---

## Phase 4: Baseline 모델 뼈대

**산출물**: `src/baseline/__init__.py`, `src/baseline/run_baseline.py`

### 구현 함수 목록

#### 4-1. 전처리

| 함수 | 기능 |
|---|---|
| `prepare_features(df, target="label")` | `classify_columns()`로 ratio + raw_value 컬럼 선택, `SimpleImputer(strategy="median")`으로 결측 대체, X/y 분리 |
| `time_split(df, train_end, val_end)` | year 기준 분할. train ≤ train_end, val ≤ val_end, test = 나머지 |
| `random_split(df, test_size, val_size, seed)` | 랜덤 분할 (기본 test 20%, val 10%) |

#### 4-2. 모델 학습

| 함수 | 기능 |
|---|---|
| `train_logistic(X_train, y_train)` | `StandardScaler` 적용 + `LogisticRegression(class_weight="balanced")` |
| `train_decision_tree(X_train, y_train)` | `DecisionTreeClassifier(max_depth=5, class_weight="balanced")` |

두 모델 모두 `class_weight="balanced"`를 적용하여 label 불균형에 대응했다.

#### 4-3. 평가

| 함수 | 기능 |
|---|---|
| `evaluate(model, X_test, y_test)` | f1, precision, recall, roc_auc, pr_auc 5개 지표 반환 |
| `print_report(results)` | 모델별 지표 비교 테이블 출력 |

`_predict()` 헬퍼가 LogisticRegression의 `_scaler`를 자동 감지하여 스케일링을 적용한다.

#### 4-4. 메인 실행

| 함수 | 기능 |
|---|---|
| `run_baseline(csv_path, split, train_end, val_end)` | 전체 파이프라인: 로드 → 전처리 → 분할 → 학습 → val/test 평가 |

### CLI 사용법

```bash
# 기본 실행 (time split, train ≤ 2021, val ≤ 2022, test > 2022)
python -m src.baseline.run_baseline

# 랜덤 분할
python -m src.baseline.run_baseline --split random

# 커스텀 연도 분할
python -m src.baseline.run_baseline --train-end 2020 --val-end 2021

# 매크로 포함 데이터
python -m src.baseline.run_baseline --data preprocess/data/output/clean_data.csv
```

### 설계 결정사항

| 항목 | 결정 | 이유 |
|---|---|---|
| 기본 분할 | time split (2021/2022) | 시계열 데이터 특성상 미래 데이터 누수 방지 |
| 결측 대체 | median | 재무비율의 이상치에 강건 |
| 스케일링 | LogisticRegression만 적용 | DecisionTree는 스케일링 불필요 |
| class_weight | balanced | 상폐 기업(label=1) 비율이 낮을 것으로 예상 |
| tree max_depth | 5 | baseline 과적합 방지 |

---

## 생성/수정 파일 요약

| 파일 | 작업 |
|---|---|
| `requirements.txt` | scikit-learn, matplotlib, seaborn 추가 |
| `notebooks/eda_template.ipynb` | 신규 생성 (Phase 3) |
| `src/baseline/__init__.py` | 신규 생성 |
| `src/baseline/run_baseline.py` | 신규 생성 (Phase 4) |
