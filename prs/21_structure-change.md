# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...21-modeling-add-tree-based-models-rf-boosting`
- 총 변경: 93개 파일 (93 files changed, 466340 insertions(+), 119338 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
**변경 배경/동기**

- 선행 전처리 단계에서 생성된 H-horizon 데이터셋을 바로 학습에 연결할 수 있는 모델링 실행 구조와 평가 기준이 필요했다.
- 상장폐지 예측 데이터는 클래스 불균형이 극심해 accuracy나 기본 threshold로는 모델 비교가 왜곡되므로, 실험 기준을 먼저 고정해야 했다.
- 이번 작업의 핵심은 전처리 자체를 새로 만드는 것이 아니라, 기존 산출물을 입력으로 받아 baseline 실험을 반복 가능하게 만드는 모델링 파이프라인을 정리하는 데 있었다.

**주요 변경 사항**

- `preprocess/data/processed/H{n}/` 구조를 입력 계약으로 삼아 데이터 로더, 피처 선택, 결측 대체, threshold 최적화, 결과 저장 형식을 포함한 모델링 실행 파이프라인을 추가했다.
- RandomForest 기준으로 불균형 대응 전략 7가지를 비교해, 샘플링이나 class weight보다 `baseline + threshold 최적화`가 더 낫다는 기준을 확정했다.
- RandomForest, sklearn GBM, XGBoost, LightGBM 학습 모듈과 `run_all.py`, `compare_experiments.py`를 추가해 H10/H12 중심 baseline 실험과 결과 비교를 자동화했다.
- 실험 산출물로 `exp_001_baseline_clip` 결과 패키지를 정리했고, H10/H12 기준에서는 샘플링이나 가중치보다 `baseline + threshold 최적화`가 유리하다는 결론과 함께 RF H12가 test PR-AUC 0.2035로 최고 성능을 기록했다.

**주의할 점**

- 학습용 가공 데이터(`preprocess/data/processed/H*/`)와 실험 결과 파일이 PR에 대량 포함되어 있어 리뷰 단위가 크고, 코드 변경과 데이터 산출물을 분리하지 않으면 diff 해석 비용이 높다.
- `run_all.py`의 모델 레지스트리에 `catboost`가 남아 있지만 현재 브랜치에는 `train_catboost.py`가 포함되지 않아 해당 옵션 실행 시 ImportError가 발생할 수 있다.
- `train_xgboost.py`, `train_lightgbm.py`, `imbalance_experiment.py`는 외부 라이브러리에 의존하지만 현재 `requirements.txt` 반영 범위는 제한적이므로, 동일 환경 재현 시 별도 패키지 설치 여부를 확인해야 한다.
- 자동 점검 결과는 `FAIL`이다. 기존 저장소 기준 자동화 엔트리(`automation.run_checks`, `collect.py`, `src.s3_uploader_v2`)를 찾지 못해 PR 생성 파이프라인의 비-S3 체크가 실패했다.

**영향 범위**

- 기존 전처리 산출물을 동일한 입력 계약으로 재사용하면서, 이후 실험은 같은 split/전처리 규칙 위에서 반복 실행할 수 있다.
- 모델 학습 코드와 실험 리포트가 `processed/H{horizon}/train|valid|test|meta.json` 구조를 공통 입력으로 사용하게 되어, 후속 튜닝 실험의 비교 기준이 명확해진다.
- baseline 성능 기준점과 실험 결론이 정리되었기 때문에, 다음 단계에서는 전처리 옵션 비교나 하이퍼파라미터 튜닝을 이 기준선 대비 개선 여부로 판단할 수 있다.

검증 상태: `FAIL`

재실행 명령:

```bash
python3 scripts/pr_pipeline.py --type auto --base main --output-json prs/context.json
```

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `7eba6f5` | 2026-04-29 | hann | docs : edit summary.md file #21 |
| `c39b53b` | 2026-04-27 | hann | feat : phase 4 & clean up result package #21 |
| `c15b2c7` | 2026-04-27 | hann | feat : make train py files through working process of phase 3 #21 |
| `a118011` | 2026-04-27 | hann | feat : phase 2 in c_work_plan.md #21 |
| `0fe0038` | 2026-04-27 | hann | feat : phase 1 in c_work_plan.md #21 |
| `2f57f19` | 2026-04-27 | hann | docs : write c_work_plan.md #21 |
| `9de3726` | 2026-04-24 | PRAHE | feat : robust preprocessing & build H datasets #17 (#19) |
| `51ef9f6` | 2026-04-24 | PRAHE | [Preprocess] feat : YoY 방식으로 IS 증가율 계산 교체 (브랜치 히스토리 정리) (#18) |
| `1757e87` | 2026-04-24 | PRAHE | [DATA] fix : mapping data (#14) |
| `fec37b8` | 2026-04-16 | PRAHE | [Preprocess] fix : EDA 이전 데이터 정제  (#11) |
| `53ae064` | 2026-04-14 | JEONGHAN | Merge pull request #9 from KW0SS/3-ml-pipeline-prep |
| `6e4db47` | 2026-04-08 | hann | docs : 3 pr md file #3 |
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
  - `src/modeling/__init__.py` (추가)
  - `src/modeling/compare_experiments.py` (추가)
  - `src/modeling/data_loader.py` (추가)
  - `src/modeling/evaluate.py` (추가)
  - `src/modeling/imbalance_experiment.py` (추가)
  - `src/modeling/run_all.py` (추가)
  - `src/modeling/train_gbm.py` (추가)
  - `src/modeling/train_lightgbm.py` (추가)
  - `src/modeling/train_rf.py` (추가)
  - `src/modeling/train_xgboost.py` (추가)
**root/**
  - `.gitignore` (수정)
  - `README.md` (추가)
  - `requirements.txt` (수정)
**other/**
  - `docs/b_work_plan.md` (추가)
  - `docs/c_work_plan.md` (추가)
  - `docs/data_contract_for_A.md` (추가)
  - `docs/raw_schema_check.md` (추가)
  - `notebooks/eda_template.executed.html` (추가)
  - `notebooks/eda_template.ipynb` (추가)
  - `preprocess/__init__.py` (추가)
  - `preprocess/build_h_datasets.py` (추가)
  - `preprocess/build_master_dataset.py` (추가)
  - `preprocess/data/meta/download_log_20260411_164440.json` (추가)
  - `preprocess/data/meta/s3_snapshot_20260411_114335.json` (추가)
  - `preprocess/data/processed/H10/meta.json` (추가)
  - `preprocess/data/processed/H10/test.csv` (추가)
  - `preprocess/data/processed/H10/train.csv` (추가)
  - `preprocess/data/processed/H10/valid.csv` (추가)
  - `preprocess/data/processed/H12/meta.json` (추가)
  - `preprocess/data/processed/H12/test.csv` (추가)
  - `preprocess/data/processed/H12/train.csv` (추가)
  - `preprocess/data/processed/H12/valid.csv` (추가)
  - `preprocess/data/processed/H14/meta.json` (추가)
  - `preprocess/data/processed/H14/test.csv` (추가)
  - `preprocess/data/processed/H14/train.csv` (추가)
  - `preprocess/data/processed/H14/valid.csv` (추가)
  - `preprocess/data/processed/H16/meta.json` (추가)
  - `preprocess/data/processed/H16/test.csv` (추가)
  - `preprocess/data/processed/H16/train.csv` (추가)
  - `preprocess/data/processed/H16/valid.csv` (추가)
  - `preprocess/data/processed/H18/meta.json` (추가)
  - `preprocess/data/processed/H18/test.csv` (추가)
  - `preprocess/data/processed/H18/train.csv` (추가)
  - `preprocess/data/processed/H18/valid.csv` (추가)
  - `preprocess/data/processed/H20/meta.json` (추가)
  - `preprocess/data/processed/H20/test.csv` (추가)
  - `preprocess/data/output/financial_raw.csv → preprocess/data/processed/H20/train.csv` (이름변경)
  - `preprocess/data/processed/H20/valid.csv` (추가)
  - `preprocess/data/processed/H22/meta.json` (추가)
  - `preprocess/data/processed/H22/test.csv` (추가)
  - `preprocess/data/output/clean_data_no_macro.csv → preprocess/data/processed/H22/train.csv` (이름변경)
  - `preprocess/data/processed/H22/valid.csv` (추가)
  - `preprocess/data/processed/H24/meta.json` (추가)
  - `preprocess/data/processed/H24/test.csv` (추가)
  - `preprocess/data/output/clean_data.csv → preprocess/data/processed/H24/train.csv` (이름변경)
  - `preprocess/data/processed/H24/valid.csv` (추가)
  - `preprocess/data/processed/H6/meta.json` (추가)
  - `preprocess/data/processed/H6/test.csv` (추가)
  - `preprocess/data/processed/H6/train.csv` (추가)
  - `preprocess/data/processed/H6/valid.csv` (추가)
  - `preprocess/data/processed/H8/meta.json` (추가)
  - `preprocess/data/processed/H8/test.csv` (추가)
  - `preprocess/data/processed/H8/train.csv` (추가)
  - `preprocess/data/processed/H8/valid.csv` (추가)
  - `preprocess/data/output/financial_with_macro.csv → preprocess/data/processed/combined_raw.csv` (이름변경)
  - `preprocess/src/__init__.py` (추가)
  - `preprocess/src/account_mapper.py` (수정)
  - `preprocess/src/etl.py` (추가)
  - `preprocess/src/ratio_calculator.py` (수정)
  - `preprocess/tools/verify_patch.py` (추가)
  - `prs/3_structure-change.md` (추가)
  - `results/exp_001_baseline_clip/comparison_test.csv` (추가)
  - `results/exp_001_baseline_clip/comparison_valid.csv` (추가)
  - `results/exp_001_baseline_clip/experiment.json` (추가)
  - `results/exp_001_baseline_clip/feature_importance_H10.csv` (추가)
  - `results/exp_001_baseline_clip/feature_importance_H12.csv` (추가)
  - `results/exp_001_baseline_clip/gbm_H10.json` (추가)
  - `results/exp_001_baseline_clip/gbm_H12.json` (추가)
  - `results/exp_001_baseline_clip/imbalance_strategy_result.md` (추가)
  - `results/exp_001_baseline_clip/lgbm_H10.json` (추가)
  - `results/exp_001_baseline_clip/lgbm_H12.json` (추가)
  - `results/exp_001_baseline_clip/rf_H10.json` (추가)
  - `results/exp_001_baseline_clip/rf_H12.json` (추가)
  - `results/exp_001_baseline_clip/summary.md` (추가)
  - `results/exp_001_baseline_clip/xgb_H10.json` (추가)
  - `results/exp_001_baseline_clip/xgb_H12.json` (추가)
  - `s3/s3_downloader.py` (추가)
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
