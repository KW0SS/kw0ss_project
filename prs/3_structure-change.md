# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...3-ml-pipeline-prep`
- 총 변경: 16개 파일 (16 files changed, 13129 insertions(+))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
### 변경 배경/동기
- A가 생성해 전달할 `clean_data` 계열 CSV를 B 담당자가 바로 검증·탐색·baseline 학습에 사용할 수 있도록 데이터 계약, raw 스키마 해석, 분석 유틸리티, EDA 노트북, baseline 실행 뼈대를 한 브랜치에서 정리했다.
- 기존 raw JSON/S3 수집 흐름과 분석 단계 사이의 연결 규칙이 문서와 코드로 분리되어 있지 않아, 재무비율 컬럼명·dtype·결측 표현·유일키·label 기준을 먼저 고정하고 이후 Phase 3/4 작업이 같은 계약을 참조하도록 했다.

### 주요 변경 사항
- raw DART JSON의 최상위 구조, 분기별 금액 필드 차이, 결측 표현, 계정 매핑과 재무비율 계산식 기준을 문서화하고, A가 생성해야 하는 `clean_data_no_macro.csv`/`clean_data.csv` 스키마와 정규화 규칙을 명시했다.
- `preprocess.src.etl`에 raw JSON을 `account_mapper`와 `ratio_calculator`로 연결해 기업·연도·분기 단위 재무비율 CSV로 변환하는 single/batch CLI를 추가했다.
- `src.analysis.utils`에 CSV 로드, 컬럼 분류, 기본 통계, 결측 분석, 단변량/label별 비교, 상관관계, IQR 기반 이상치 탐지 및 읽기 쉬운 이상치 판정표 렌더링 함수를 추가했다.
- `notebooks/eda_template.ipynb`와 실행 HTML을 추가해 `clean_data_no_macro.csv` 기준으로 기본 통계, 결측, 라벨 분포, 단변량 분포, 상관관계, 이상치 요약, baseline 모델 결과를 한 번에 확인할 수 있게 했다.
- `src.baseline.run_baseline`에 수치형 feature 선택, median imputation, time/random split, Logistic Regression/Decision Tree 학습, f1·precision·recall·roc_auc·pr_auc 평가 CLI를 추가했다.
- Phase 3/4 작업 로그와 README를 추가해 분석 파이프라인의 사용법, 산출물, 의존 패키지(`scikit-learn`, `matplotlib`, `seaborn`)를 추적 가능하게 했다.

### 주의할 점
- PR 점검 결과는 `PASS 2 / WARN 0 / FAIL 3`이다. 실패 항목은 현재 브랜치 범위와 맞지 않는 기존 점검 명령(`automation.run_checks`, `collect.py --help`, `src.s3_uploader_v2 --help`)이 없는 모듈/파일을 참조해서 발생했다. 재실행 명령: `python3 scripts/pr_pipeline.py --type auto --base main --output-json prs/context.json`
- baseline의 `prepare_features()`는 train/val/test 각각에서 median imputer를 fit한다. 현재 `clean_data_no_macro.csv`는 결측 0개라 영향이 제한적이지만, 결측이 있는 데이터로 확장할 경우 train fit 후 val/test transform 구조로 바꾸는 것이 더 엄밀하다.
- EDA 실행 HTML이 포함되어 PR 크기가 커졌다. 리뷰 목적에 따라 실행 산출물을 유지할지, 노트북 원본만 남길지 결정이 필요하다.

### 영향 범위
- 기존 수집/S3 조회 코드의 런타임 동작을 직접 변경하기보다, 분석 단계에서 사용할 신규 `src/analysis`, `src/baseline`, `notebooks`, `docs` 산출물을 추가하는 영향이 크다.
- `requirements.txt`에 ML/시각화 의존성이 추가되어 분석 노트북과 baseline 실행 환경 설치 범위가 확장된다.
- 데이터 계약서 기준으로 `stock_code`, `year`, `quarter`, `label`, `gics_sector` 및 재무비율 컬럼명을 고정하므로, A의 병합 산출물이 이 스키마와 다르면 EDA/baseline 쪽에서 컬럼 분류나 모델 입력 생성이 실패할 수 있다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `d46e015` | 2026-04-08 | hann | reafctor : edit util.py for analysing #3 |
| `abfd9d2` | 2026-04-08 | hann | feat : create part of eda #3 |
| `92b7498` | 2026-04-07 | hann | docs : edit contract #3 |
| `4b26b25` | 2026-04-07 | hann | Merge branch 'main' into 3-ml-pipeline-prep |
| `3ec36e5` | 2026-04-06 | hann | docs : edit Data Contract for a #3 |
| `b8f7b54` | 2026-04-06 | hann | refactor : remove etl/ratio_calculator.py for duplication & unify form #3 |
| `ef3a9b5` | 2026-04-06 | hann | feat : add analysis logic #3 |
| `ed0c76d` | 2026-04-06 | hann | Merge branch 'main' into 3-ml-pipeline-prep |
| `51ddd5b` | 2026-04-03 | hann | docs : write data contract for A #3 |
| `ac503af` | 2026-04-03 | hann | feat : ratio calculator #3 |
| `d9250bd` | 2026-04-03 | hann | docs : write raw_schema_check.md #3 |
| `26a729b` | 2026-04-02 | hann | docs : b todo plan md file #3 |

</details>

<details>
<summary>변경 파일 상세</summary>

**src/**
  - `src/__init__.py` (추가)
  - `src/analysis/__init__.py` (추가)
  - `src/analysis/utils.py` (추가)
  - `src/baseline/__init__.py` (추가)
  - `src/baseline/run_baseline.py` (추가)
**root/**
  - `README.md` (추가)
  - `requirements.txt` (수정)
**other/**
  - `docs/b_work_plan.md` (추가)
  - `docs/data_contract_for_A.md` (추가)
  - `docs/raw_schema_check.md` (추가)
  - `notebooks/eda_template.executed.html` (추가)
  - `notebooks/eda_template.ipynb` (추가)
  - `preprocess/__init__.py` (추가)
  - `preprocess/src/__init__.py` (추가)
  - `preprocess/src/etl.py` (추가)
  - `work_log/phase3_4_work_log.md` (추가)

</details>

## 점검 결과 (S3 제외)
- 요약: PASS 2 / WARN 0 / FAIL 3
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | FAIL | automation non-s3 checks failed |
| collect_help | FAIL | command failed: python3 collect.py --help |
| s3_uploader_v2_help | FAIL | command failed: python3 -m src.s3_uploader_v2 --help |
| py_compile | PASS | ok |

## 점검 상세
### ❌ automation_non_s3 (FAIL)
- automation non-s3 checks failed
  - /opt/anaconda3/bin/python3: Error while finding module specification for 'automation.run_checks' (ModuleNotFoundError: No module named 'automation')
### ❌ collect_help (FAIL)
- command failed: python3 collect.py --help
  - python3: can't open file '/Users/hann/Project/kwoss/kw0ss_project/collect.py': [Errno 2] No such file or directory
### ❌ s3_uploader_v2_help (FAIL)
- command failed: python3 -m src.s3_uploader_v2 --help
  - /opt/anaconda3/bin/python3: No module named src.s3_uploader_v2

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
