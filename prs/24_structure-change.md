# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...24-eda-re-run-exploratory-data-analysis-on-the-cleaned-dataset`
- 총 변경: 171개 파일 (171 files changed, 1710964 insertions(+), 45722 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약

### 변경 배경/동기
- 기존 raw JSON 기반 재무비율 데이터셋에는 종목코드 재사용으로 인한 비상장 기간 데이터, 섹터 폴더/CFS·OFS 변형에 따른 중복 키, 증가율 결측 과다 문제가 섞여 있어 모델 학습·평가의 신뢰도가 낮아질 수 있었습니다.
- H별 rolling label 데이터셋을 재생성하면서, H6~H24 전 구간과 baseline/전처리 실험군을 동일한 split 기준으로 비교할 수 있는 구조가 필요했습니다.
- 새로 생성된 Hn/baseline 데이터의 라벨 분포, 결측, 이상치, horizon별 품질 차이를 빠르게 확인하기 위한 EDA 노트북도 함께 추가했습니다.

### 주요 변경 사항
- `build_master_dataset` 단계에 `(stock_code, year, quarter)` dedup과 KRX 상장일 기반 pre-listing 행 제거를 통합해, 원천 데이터 정합성 처리를 파이프라인 내부에서 재현 가능하게 만들었습니다.
- `build_h_datasets` 단계에서 H6~H24 전 horizon을 처리하고, `baseline`, `exp-A`, `exp-B`, `exp-C` 실험 디렉터리별로 `train/valid/test/meta.json`을 저장하도록 구조화했습니다.
- 각 실험군은 동일한 rolling label과 time split을 공유하되, winsorize/robust scaling 적용 여부만 달라지도록 구성해 전처리 효과 비교가 가능해졌습니다.
- 정제 결과 보고서와 raw scan 리포트를 추가해 제거된 시간역행 데이터, 중복 제거 영향, H별 양성 샘플 변화, 재학습 권장사항을 문서화했습니다.
- `2026-05-01_Hn_baseline_eda.ipynb`를 추가해 Hn/baseline 데이터의 메타 비교, 결측/이상치, 라벨 분포, 상관관계, split 누수 점검, 간단 baseline 모델 확인을 한 노트북에서 수행할 수 있게 했습니다.

### 주의할 점
- 대량의 CSV/JSON 산출물이 PR에 포함되어 diff 규모가 매우 큽니다. 리뷰 시 코드 변경과 산출 데이터 검증을 분리해서 보는 것이 좋습니다.
- `notebooks/.ipynb_checkpoints/2026-05-01_Hn_baseline_eda-checkpoint.ipynb`가 포함되어 있어, 의도한 산출물이 아니면 PR 전 제외 여부를 확인해야 합니다.
- `preprocess/build_master_dataset.py`, `preprocess/build_h_datasets.py`의 `BASE_DIR`이 Windows 절대경로로 설정되어 있어 다른 환경에서 재실행하려면 경로 설정 조정이 필요합니다.
- 파이프라인 점검 중 `automation.run_checks`, `collect.py`, `src.s3_uploader_v2` 관련 체크는 현재 저장소 구조에서 모듈/파일을 찾지 못해 실패했습니다.

### 영향 범위
- `combined_raw.csv`와 H6~H24 전체 processed dataset이 갱신되어, 기존 모델 학습/평가 결과와 직접 비교하기 어렵고 재학습이 필요합니다.
- 정합성 개선으로 H별 train/test 양성 샘플 수가 증가하고 클래스 불균형이 일부 완화되어, PR-AUC 등 평가 지표의 통계적 안정성이 달라질 수 있습니다.
- 모델링 코드는 새 저장 구조인 `preprocess/data/processed/H{H}/{experiment}/`를 기준으로 데이터를 읽어야 하며, baseline 외 실험군 비교가 가능해집니다.
- S3 업로드나 기존 자동화 스크립트는 변경된 데이터 경로/파일 수 증가를 반영해 별도 점검이 필요합니다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `f6a2965` | 2026-05-01 | hann | feat : EDA files #24 |
| `11b88ec` | 2026-05-01 | hann | Merge remote-tracking branch 'origin/20-fix-data' into 24-eda-re-run-exploratory-data-analysis-on-the-cleaned-dataset |
| `f90c7ca` | 2026-04-30 | PRAHE | feat : update sector sample distribution (H12 train) |
| `874663b` | 2026-04-30 | PRAHE | docs: 데이터 정합성 정제 보고서 추가 |
| `5a46502` | 2026-04-30 | PRAHE | data: KRX 상장법인목록/폐지현황 추가 |
| `63cd843` | 2026-04-30 | PRAHE | data: combined_raw + H별 데이터셋 갱신 (정합성 처리 적용) |
| `30f33e8` | 2026-04-30 | PRAHE | feat(pipeline): build_master에 정합성 처리 통합 |
| `ad8ae37` | 2026-04-27 | PRAHE | feat: result for scanning raw data |
| `bd1164e` | 2026-04-27 | PRAHE | chore: change the description |
| `f93fd61` | 2026-04-27 | PRAHE | feat: build_master_dataset 병렬처리 추가 |

</details>

<details>
<summary>변경 파일 상세</summary>

**root/**
  - `.gitignore` (수정)
**other/**
  - `docs/raw_scan_report.md` (추가)
  - `notebooks/.ipynb_checkpoints/2026-05-01_Hn_baseline_eda-checkpoint.ipynb` (추가)
  - `notebooks/2026-05-01_Hn_baseline_eda.ipynb` (추가)
  - `preprocess/build_h_datasets.py` (수정)
  - `preprocess/build_master_dataset.py` (수정)
  - `preprocess/data/processed/H10/baseline/meta.json` (추가)
  - `preprocess/data/processed/H10/baseline/test.csv` (추가)
  - `preprocess/data/processed/H10/baseline/train.csv` (추가)
  - `preprocess/data/processed/H10/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H10/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H10/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H10/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H10/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H10/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H10/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H10/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H10/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H10/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H10/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H10/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H10/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H12/baseline/meta.json` (추가)
  - `preprocess/data/processed/H12/baseline/test.csv` (추가)
  - `preprocess/data/processed/H12/baseline/train.csv` (추가)
  - `preprocess/data/processed/H12/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H12/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H12/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H12/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H12/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H12/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H12/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H12/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H12/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H12/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H12/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H12/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H12/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H14/baseline/meta.json` (추가)
  - `preprocess/data/processed/H14/baseline/test.csv` (추가)
  - `preprocess/data/processed/H14/baseline/train.csv` (추가)
  - `preprocess/data/processed/H14/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H14/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H14/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H14/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H14/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H14/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H14/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H14/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H14/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H14/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H14/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H14/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H14/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H16/baseline/meta.json` (추가)
  - `preprocess/data/processed/H16/baseline/test.csv` (추가)
  - `preprocess/data/processed/H16/baseline/train.csv` (추가)
  - `preprocess/data/processed/H16/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H16/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H16/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H16/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H16/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H16/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H16/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H16/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H16/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H16/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H16/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H16/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H16/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H18/baseline/meta.json` (추가)
  - `preprocess/data/processed/H18/baseline/test.csv` (추가)
  - `preprocess/data/processed/H18/baseline/train.csv` (추가)
  - `preprocess/data/processed/H18/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H18/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H18/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H18/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H18/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H18/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H18/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H18/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H18/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H18/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H18/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H18/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H18/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H20/baseline/meta.json` (추가)
  - `preprocess/data/processed/H20/baseline/test.csv` (추가)
  - `preprocess/data/processed/H20/baseline/train.csv` (추가)
  - `preprocess/data/processed/H20/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H20/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H20/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H20/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H20/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H20/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H20/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H20/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H20/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H20/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H20/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H20/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H20/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H22/baseline/meta.json` (추가)
  - `preprocess/data/processed/H22/baseline/test.csv` (추가)
  - `preprocess/data/processed/H22/baseline/train.csv` (추가)
  - `preprocess/data/processed/H22/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H22/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H22/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H22/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H22/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H22/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H22/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H22/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H22/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H22/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H22/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H22/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H22/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H24/baseline/meta.json` (추가)
  - `preprocess/data/processed/H24/baseline/test.csv` (추가)
  - `preprocess/data/processed/H24/baseline/train.csv` (추가)
  - `preprocess/data/processed/H24/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H24/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H24/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H24/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H24/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H24/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H24/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H24/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H24/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H24/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H24/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H24/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H24/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H6/baseline/meta.json` (추가)
  - `preprocess/data/processed/H6/baseline/test.csv` (추가)
  - `preprocess/data/processed/H6/baseline/train.csv` (추가)
  - `preprocess/data/processed/H6/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H6/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H6/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H6/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H6/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H6/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H6/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H6/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H6/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H6/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H6/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H6/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H6/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/H8/baseline/meta.json` (추가)
  - `preprocess/data/processed/H8/baseline/test.csv` (추가)
  - `preprocess/data/processed/H8/baseline/train.csv` (추가)
  - `preprocess/data/processed/H8/baseline/valid.csv` (추가)
  - `preprocess/data/processed/H8/exp-A/meta.json` (추가)
  - `preprocess/data/processed/H8/exp-A/test.csv` (추가)
  - `preprocess/data/processed/H8/exp-A/train.csv` (추가)
  - `preprocess/data/processed/H8/exp-A/valid.csv` (추가)
  - `preprocess/data/processed/H8/exp-B/meta.json` (추가)
  - `preprocess/data/processed/H8/exp-B/test.csv` (추가)
  - `preprocess/data/processed/H8/exp-B/train.csv` (추가)
  - `preprocess/data/processed/H8/exp-B/valid.csv` (추가)
  - `preprocess/data/processed/H8/exp-C/meta.json` (추가)
  - `preprocess/data/processed/H8/exp-C/test.csv` (추가)
  - `preprocess/data/processed/H8/exp-C/train.csv` (추가)
  - `preprocess/data/processed/H8/exp-C/valid.csv` (추가)
  - `preprocess/data/processed/combined_raw.csv` (수정)
  - `"preprocess/data/\354\203\201\354\236\245\353\262\225\354\235\270\353\252\251\353\241\235.xlsx"` (추가)
  - `"preprocess/data/\354\203\201\354\236\245\355\217\220\354\247\200\355\230\204\355\231\251.xlsx"` (추가)
  - `preprocess/docs/data_cleaning_report.md` (추가)
  - `preprocess/src/dart_api.py` (추가)

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

## 재실행 명령
- PR 컨텍스트와 점검 재생성: `python3 scripts/pr_pipeline.py --type auto --base main --output-json prs/context.json`

## 앞으로 진행할 내용
- 필요 시 `python3 -m automation.run_checks --mode s3-only`로 S3 무결성 별도 점검
- PR 리뷰 반영 후 커밋 정리 및 머지
