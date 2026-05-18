# EDA: fixed_N 개별 전처리 데이터셋 탐색 노트북 추가

## 개요
- PR 타입: `structure`
- 비교 기준: `main...33-eda-check-processed_fixed_v2-data-set`
- 총 변경: 4개 파일 (4 files changed, 9209 insertions(+))
- 설명: `processed/fixed_N*` 데이터셋의 품질, 라벨 구성, 전처리 variant 효과, 피처 분포를 확인하는 EDA 노트북 추가 PR입니다.

## 변경 요약

### 변경 배경/동기
기존 horizon 기반 H 데이터셋과 별도로, 상폐연도 기준 정확히 N년 전 데이터를 양성으로 정의한 `fixed_N1`, `fixed_N2`, `fixed_N3` 개별 전처리 데이터셋이 추가됐다. 이 데이터셋은 horizon window 방식과 라벨 의미가 다르므로, 모델 학습 전에 라벨 분포, split 품질, variant별 전처리 효과, 주요 피처의 label 분리 가능성을 별도로 점검할 EDA 경로가 필요했다.

### 주요 변경 사항
- `fixed_N` 전체 비교용 overview EDA 노트북을 추가했다. `fixed_N1~N3`와 `baseline`, `exp-A`, `exp-B`, `exp-C` variant를 탐색하며 split별 row 수, positive rate, 결측률, 중복 키, 라벨 구성 차이를 확인할 수 있다.
- `fixed_N` 상세 feature EDA 노트북을 추가했다. `SELECTED_N`, `SELECTED_VARIANT` 값을 바꿔 특정 데이터셋의 컬럼 분류, 결측, 이상치, 라벨 분포, 주요 피처 단변량 통계, 상관관계, split 누수 여부를 확인한다.
- label별 scatter/pairplot 분석을 추가했다. 부채비율과 자기자본비율처럼 회계 항등식에 가까운 조합은 제외하고, 부채비율과 수익성, 차입금의존도와 현금비율처럼 label 분리 해석이 가능한 조합 위주로 구성했다.
- overview 산점도는 기본적으로 `fixed_N1`, `fixed_N2`만 비교하도록 했다. `fixed_N3`는 현재 test split에 양성 샘플이 없어 최종 성능 비교 목적에는 적합하지 않기 때문이다.
- Jupyter에서 열린 노트북이 저장 상태를 덮어쓰는 경우를 고려해 동일한 checkpoint 파일도 함께 반영됐다.

### 주의할 점
- 이번 변경은 노트북 추가이며, 모델 학습 코드나 데이터 생성 파이프라인의 런타임 동작은 변경하지 않는다.
- `fixed_N3`는 train/valid EDA 참고용으로는 사용할 수 있지만, 현재 test 양성 샘플이 없어 PR-AUC, recall, F1 같은 test 성능 비교에는 부적합하다.
- `.ipynb_checkpoints` 파일이 PR diff에 포함되어 있다. 리뷰 정책상 checkpoint를 제외하려면 후속 커밋에서 제거하거나 `.gitignore` 반영이 필요하다.
- 파이프라인 점검은 기존 프로젝트 구조와 맞지 않는 `automation.run_checks`, `collect.py`, `src.s3_uploader_v2` 확인에서 실패했다. 변경 파일 자체는 노트북 JSON 유효성 검사를 별도로 통과했다.

### 영향 범위
- `preprocess/data/processed/fixed_N*/{variant}` 데이터를 분석하는 문서형 EDA 경로가 추가된다.
- 기존 H horizon EDA, 모델링 로더, 학습 실행 스크립트, 결과 리포트에는 직접적인 동작 변경이 없다.
- 향후 `fixed_N1`, `fixed_N2` 중심 모델 학습 후보 선정과 `fixed_N3` 제외/보조 분석 판단에 참고할 수 있다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `06e56ce` | 2026-05-15 | hann | EDA : process_fixed data set EDA #33 |

</details>

<details>
<summary>변경 파일 상세</summary>

**other/**
  - `notebooks/.ipynb_checkpoints/2026-05-15_fixed_N_feature_eda-checkpoint.ipynb` (추가)
  - `notebooks/.ipynb_checkpoints/2026-05-15_fixed_N_overview_eda-checkpoint.ipynb` (추가)
  - `notebooks/2026-05-15_fixed_N_feature_eda.ipynb` (추가)
  - `notebooks/2026-05-15_fixed_N_overview_eda.ipynb` (추가)

</details>

## 점검 결과 (S3 제외)
- 요약: PASS 2 / WARN 0 / FAIL 3
| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | FAIL | automation non-s3 checks failed |
| collect_help | FAIL | command failed: python3 collect.py --help |
| s3_uploader_v2_help | FAIL | command failed: python3 -m src.s3_uploader_v2 --help |
| py_compile | PASS | no changed python files |

## 점검 상세
### ❌ automation_non_s3 (FAIL)
- automation non-s3 checks failed
  - /opt/homebrew/opt/python@3.14/bin/python3.14: Error while finding module specification for 'automation.run_checks' (ModuleNotFoundError: No module named 'automation')
### ❌ collect_help (FAIL)
- command failed: python3 collect.py --help
  - /opt/homebrew/Cellar/python@3.14/3.14.4/Frameworks/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python: can't open file '/Users/hann/Project/kwoss/kw0ss_project/collect.py': [Errno 2] No such file or directory
### ❌ s3_uploader_v2_help (FAIL)
- command failed: python3 -m src.s3_uploader_v2 --help
  - /opt/homebrew/opt/python@3.14/bin/python3.14: No module named src.s3_uploader_v2

## 추가 점검
- `python3 -m json.tool notebooks/2026-05-15_fixed_N_overview_eda.ipynb`
- `python3 -m json.tool notebooks/2026-05-15_fixed_N_feature_eda.ipynb`
- 결과: PASS

## 재실행 명령

```bash
python3 scripts/pr_pipeline.py --type auto --base main --output-json prs/context.json
```

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
