# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...35-train-train-model-by-processed-fixed-data-set`
- 총 변경: 249개 파일 (249 files changed, 14613 insertions(+))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
### 변경 배경/동기

기존 horizon 기반 라벨(H개월 내 상폐)만으로는 라벨 정의와 horizon 길이에 따른 성능 차이를 충분히 비교하기 어려웠다. 이번 변경은 `processed_v4` 기반 데이터에서 backward-looking fixed_N 라벨과 forward-looking horizon 라벨을 같은 모델군·전처리 variant 기준으로 재학습해, 상폐 예측에 더 안정적인 라벨 정의와 운영 후보 조합을 찾기 위한 실험 산출물이다.

### 주요 변경 사항

- fixed_N 전용 데이터 로더와 실행 CLI를 추가해 `fixed_N{n}/{variant}` 구조의 train/valid/test 데이터를 기존 모델 학습 인터페이스로 처리할 수 있게 했다.
- 공통 모델 레지스트리에 Logistic Regression 학습 모듈을 추가해 tree 기반 모델과 선형 baseline을 동일한 `train()` 시그니처와 `predict_proba` 평가 흐름으로 비교했다.
- fixed_N N1/N2 실험, 다중공선성 후보 피처 제거 ablation, horizon H10~H24 재학습 결과를 JSON과 summary 리포트로 저장해 라벨 정의·horizon 길이·variant별 성능을 추적 가능하게 했다.
- 실험 결과상 fixed_N1 RF exp-C가 전체 best로 유지되며, 긴 horizon에서는 H24 GBM baseline이 horizon best로 fixed_N1과의 PR-AUC 격차를 크게 줄이는 것으로 정리됐다.

### 주의할 점

- 이번 PR은 학습 파이프라인 코드보다 실험 결과 산출물이 대부분을 차지한다. 리뷰 시 대량 JSON은 결과 재현성/보관 목적의 변경으로 보고, 핵심 로직은 fixed_N 로더·실행기·LogReg 추가에 집중하는 것이 적절하다.
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

**src/**
  - `src/modeling/data_loader_fixed.py` (추가)
  - `src/modeling/run_all.py` (수정)
  - `src/modeling/run_all_fixed.py` (추가)
  - `src/modeling/train_logreg.py` (추가)
**other/**
  - `notebooks/2026-05-15_fixed_N_feature_eda.ipynb` (추가)
  - `results/exp_004_fixed_N/gbm_N1_baseline.json` (추가)
  - `results/exp_004_fixed_N/gbm_N1_exp-A.json` (추가)
  - `results/exp_004_fixed_N/gbm_N1_exp-B.json` (추가)
  - `results/exp_004_fixed_N/gbm_N1_exp-C.json` (추가)
  - `results/exp_004_fixed_N/gbm_N2_baseline.json` (추가)
  - `results/exp_004_fixed_N/gbm_N2_exp-A.json` (추가)
  - `results/exp_004_fixed_N/gbm_N2_exp-B.json` (추가)
  - `results/exp_004_fixed_N/gbm_N2_exp-C.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N1_baseline.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N1_exp-A.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N1_exp-B.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N1_exp-C.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N2_baseline.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N2_exp-A.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N2_exp-B.json` (추가)
  - `results/exp_004_fixed_N/lgbm_N2_exp-C.json` (추가)
  - `results/exp_004_fixed_N/logreg_N1_baseline.json` (추가)
  - `results/exp_004_fixed_N/logreg_N1_exp-A.json` (추가)
  - `results/exp_004_fixed_N/logreg_N1_exp-B.json` (추가)
  - `results/exp_004_fixed_N/logreg_N1_exp-C.json` (추가)
  - `results/exp_004_fixed_N/logreg_N2_baseline.json` (추가)
  - `results/exp_004_fixed_N/logreg_N2_exp-A.json` (추가)
  - `results/exp_004_fixed_N/logreg_N2_exp-B.json` (추가)
  - `results/exp_004_fixed_N/logreg_N2_exp-C.json` (추가)
  - `results/exp_004_fixed_N/rf_N1_baseline.json` (추가)
  - `results/exp_004_fixed_N/rf_N1_exp-A.json` (추가)
  - `results/exp_004_fixed_N/rf_N1_exp-B.json` (추가)
  - `results/exp_004_fixed_N/rf_N1_exp-C.json` (추가)
  - `results/exp_004_fixed_N/rf_N2_baseline.json` (추가)
  - `results/exp_004_fixed_N/rf_N2_exp-A.json` (추가)
  - `results/exp_004_fixed_N/rf_N2_exp-B.json` (추가)
  - `results/exp_004_fixed_N/rf_N2_exp-C.json` (추가)
  - `results/exp_004_fixed_N/summary.md` (추가)
  - `results/exp_004_fixed_N/xgb_N1_baseline.json` (추가)
  - `results/exp_004_fixed_N/xgb_N1_exp-A.json` (추가)
  - `results/exp_004_fixed_N/xgb_N1_exp-B.json` (추가)
  - `results/exp_004_fixed_N/xgb_N1_exp-C.json` (추가)
  - `results/exp_004_fixed_N/xgb_N2_baseline.json` (추가)
  - `results/exp_004_fixed_N/xgb_N2_exp-A.json` (추가)
  - `results/exp_004_fixed_N/xgb_N2_exp-B.json` (추가)
  - `results/exp_004_fixed_N/xgb_N2_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N1_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N1_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N1_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N1_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N2_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N2_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N2_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/gbm_N2_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N1_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N1_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N1_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N1_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N2_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N2_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N2_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/lgbm_N2_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N1_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N1_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N1_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N1_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N2_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N2_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N2_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/logreg_N2_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N1_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N1_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N1_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N1_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N2_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N2_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N2_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/rf_N2_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/summary.md` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N1_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N1_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N1_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N1_exp-C.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N2_baseline.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N2_exp-A.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N2_exp-B.json` (추가)
  - `results/exp_005_fixed_N_collinear_drop/xgb_N2_exp-C.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/gbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/gbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/gbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/gbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/lgbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/lgbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/lgbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/lgbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/logreg_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/logreg_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/logreg_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/logreg_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/rf_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/rf_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/rf_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/rf_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/xgb_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/xgb_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/xgb_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/baseline/xgb_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/gbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/gbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/gbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/gbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/lgbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/lgbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/lgbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/lgbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/logreg_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/logreg_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/logreg_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/logreg_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/rf_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/rf_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/rf_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/rf_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/xgb_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/xgb_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/xgb_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-A/xgb_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/gbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/gbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/gbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/gbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/lgbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/lgbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/lgbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/lgbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/logreg_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/logreg_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/logreg_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/logreg_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/rf_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/rf_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/rf_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/rf_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/xgb_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/xgb_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/xgb_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-B/xgb_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/gbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/gbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/gbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/gbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/lgbm_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/lgbm_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/lgbm_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/lgbm_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/logreg_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/logreg_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/logreg_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/logreg_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/rf_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/rf_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/rf_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/rf_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/xgb_H10.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/xgb_H12.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/xgb_H14.json` (추가)
  - `results/exp_006_horizon_vs_004/exp-C/xgb_H20.json` (추가)
  - `results/exp_006_horizon_vs_004/summary.md` (추가)
  - `results/exp_007_horizon_vs_004/baseline/gbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/gbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/gbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/gbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/lgbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/lgbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/lgbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/lgbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/logreg_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/logreg_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/logreg_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/logreg_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/rf_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/rf_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/rf_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/rf_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/xgb_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/xgb_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/xgb_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/baseline/xgb_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/gbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/gbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/gbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/gbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/lgbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/lgbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/lgbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/lgbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/logreg_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/logreg_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/logreg_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/logreg_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/rf_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/rf_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/rf_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/rf_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/xgb_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/xgb_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/xgb_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-A/xgb_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/gbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/gbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/gbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/gbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/lgbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/lgbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/lgbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/lgbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/logreg_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/logreg_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/logreg_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/logreg_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/rf_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/rf_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/rf_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/rf_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/xgb_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/xgb_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/xgb_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-B/xgb_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/gbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/gbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/gbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/gbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/lgbm_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/lgbm_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/lgbm_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/lgbm_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/logreg_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/logreg_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/logreg_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/logreg_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/rf_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/rf_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/rf_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/rf_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/xgb_H16.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/xgb_H18.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/xgb_H22.json` (추가)
  - `results/exp_007_horizon_vs_004/exp-C/xgb_H24.json` (추가)
  - `results/exp_007_horizon_vs_004/summary.md` (추가)

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
