# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...35-train-train-model-by-processed-fixed-data-set`
- 총 변경: 137개 파일 (137 files changed, 1418 insertions(+), 2366 deletions(-))
- 설명: fixed_N/horizon 모델링 구조 추가와 실험 summary 보존, per-run artifact 추적 해제 PR입니다.

## 변경 요약
### 변경 배경/동기

기존 horizon 기반 라벨(H개월 내 상폐)만으로는 라벨 정의와 horizon 길이에 따른 성능 차이를 충분히 비교하기 어려웠다. 이번 변경은 `processed_v4` 기반 데이터에서 backward-looking fixed_N 라벨과 forward-looking horizon 라벨을 같은 모델군·전처리 variant 기준으로 재학습해, 상폐 예측에 더 안정적인 라벨 정의와 운영 후보 조합을 찾기 위한 실험 산출물이다.

### 주요 변경 사항

- fixed_N 전용 데이터 로더와 실행 CLI를 추가해 `fixed_N{n}/{variant}` 구조의 train/valid/test 데이터를 기존 모델 학습 인터페이스로 처리할 수 있게 했다.
- 공통 모델 레지스트리에 Logistic Regression 학습 모듈을 추가해 tree 기반 모델과 선형 baseline을 동일한 `train()` 시그니처와 `predict_proba` 평가 흐름으로 비교했다.
- fixed_N N1/N2 실험, 다중공선성 후보 피처 제거 ablation, horizon H10~H24 재학습 결과를 JSON과 summary 리포트로 저장해 라벨 정의·horizon 길이·variant별 성능을 추적 가능하게 했다.
- 실험 결과상 fixed_N1 RF exp-C가 전체 best로 유지되며, 긴 horizon에서는 H24 GBM baseline이 horizon best로 fixed_N1과의 PR-AUC 격차를 크게 줄이는 것으로 정리됐다.

### 주의할 점

- 이번 PR은 per-run JSON과 노트북 산출물을 Git 추적에서 제외하고 summary 중심으로 결과를 남기는 정책을 포함한다. 리뷰 시 핵심 로직은 fixed_N 로더·실행기·LogReg 추가에 집중하는 것이 적절하다.
- `data_loader_fixed.py`는 import 시 `preprocess/data/processed/fixed_N*` 디렉터리를 스캔하므로, 해당 데이터가 없는 환경에서는 fixed_N CLI 초기화 단계에서 실패할 수 있다.
- `run_all.py`의 horizon 결과 저장은 variant를 파일명에 포함하지 않아, 실험 리포트처럼 variant별 하위 디렉터리로 실행해야 결과 파일 충돌을 피할 수 있다.
- PR 파이프라인 체크는 구조/수집 스크립트 존재를 전제로 한 일부 검사가 실패했지만, 변경된 Python 파일의 `py_compile`은 통과했다.

### 영향 범위

- 기존 horizon 학습 흐름은 `logreg` 모델 키만 추가되고 기존 모델 키와 데이터 로딩 방식은 유지된다.
- fixed_N 실험은 새 CLI와 새 로더로 분리되어 있어 기존 DART 수집/S3 업로드 경로에는 직접 영향이 없다.
- 후속 모델 선택·튜닝 작업에서는 fixed_N1 RF exp-C, horizon H24 GBM baseline, 표본이 더 두터운 H16 GBM exp-A를 주요 후보로 비교할 수 있다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `4193213` | 2026-05-18 | hann | train : train else horizon data set (H16, 18, 22, 24) #35 |
| `c7d6c54` | 2026-05-17 | hann | refactor : add data set view in eda #35 |
| `95c37da` | 2026-05-15 | hann | train : train models using processed_v4 data & horizon method #35 |
| `78e73ff` | 2026-05-15 | hann | feat : train 005 fixed n collinear drop #35 |
| `a5cfc92` | 2026-05-15 | hann | feat : train 004 fixed n #35 |

</details>

<details>
<summary>변경 파일 상세</summary>

**코드**
  - `src/modeling/data_loader_fixed.py` (추가)
  - `src/modeling/run_all.py` (수정: `logreg` 모델 등록)
  - `src/modeling/run_all_fixed.py` (추가)
  - `src/modeling/train_logreg.py` (추가)

**실험 리포트(summary 유지)**
  - `results/exp_004_fixed_N/summary.md` (추가)
  - `results/exp_005_fixed_N_collinear_drop/summary.md` (추가)
  - `results/exp_006_horizon_vs_004/summary.md` (추가)
  - `results/exp_007_horizon_vs_004/summary.md` (추가)

**Git 추적 해제/ignore 정책**
  - `results/**/*.json` per-run 결과 파일 추적 해제
  - `results/**/feature_importance_*.csv` 추적 해제
  - `notebooks/2026-05-15_fixed_N_feature_eda.ipynb` 추적 해제
  - `prs/context.json` 및 notebook checkpoint ignore 유지

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
  - /opt/homebrew/opt/python@3.14/bin/python3.14: Error while finding module specification for 'automation.run_checks' (ModuleNotFoundError: No module named 'automation')
### ❌ collect_help (FAIL)
- command failed: python3 collect.py --help
  - /opt/homebrew/Cellar/python@3.14/3.14.4/Frameworks/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python: can't open file '/Users/hann/Project/kwoss/kw0ss_project/collect.py': [Errno 2] No such file or directory
### ❌ s3_uploader_v2_help (FAIL)
- command failed: python3 -m src.s3_uploader_v2 --help
  - /opt/homebrew/opt/python@3.14/bin/python3.14: No module named src.s3_uploader_v2

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
