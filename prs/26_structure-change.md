# feat: processed_v3 모델 재학습 및 horizon/variant 비교 리포트

## 개요
- PR 타입: `structure`
- 비교 기준: `main...26-model-retrain-on-eda-refined-dataset-v011`
- 분석 범위: issue `#26`
- 제외 범위: issue `#27` 관련 스킬/PR 문서 변경은 이번 PR 요약에서 제외
- 총 변경: 144개 파일 (144 files changed, 13516 insertions(+), 132 deletions(-))

## 변경 요약

### 변경 배경/동기
EDA 재정제 이후 생성된 `processed_v3` 데이터셋이 기존 모델 성능을 유지하거나 개선하는지 확인하기 위해 트리 기반 모델 재학습 결과를 정리했다. 기존 H10 단일 baseline 검증에서 더 나아가, 부도 예측 horizon 길이와 전처리 variant가 성능에 미치는 영향을 비교할 수 있도록 실험 실행 경로와 결과 리포트를 확장했다.

### 주요 변경 사항
- 모델링 데이터 로더가 `preprocess/data/processed/H{n}/{variant}/` 구조를 읽도록 확장됐다. `baseline`, `exp-A`, `exp-B`, `exp-C` variant를 선택할 수 있고, 기존 `H{n}/train.csv` 구조도 fallback으로 유지해 하위 호환성을 보존했다.
- `src.modeling.run_all` CLI에 `--variant` 옵션을 추가해 동일한 모델/호라이즌 설정으로 baseline, winsorize, robust scale, winsorize+robust scale 실험을 반복 실행할 수 있게 했다.
- `exp_002_v3_baseline` 결과를 추가해 H10 baseline에서 RF가 기존 exp_001 대비 PR-AUC와 F1을 크게 개선했음을 문서화했다.
- `exp_003_v3_variant_compare`에 H10~H24 baseline horizon sweep 결과와 H10/H12 variant 비교 결과를 저장했다. RF는 전반적으로 가장 안정적이며, H20 baseline에서 PR-AUC peak가 확인됐다.
- 전처리 variant 비교에서는 H12 기준 winsorize가 RF/LGBM의 PR-AUC 개선에 기여하고, robust scale 단독은 RF에는 거의 영향이 없음을 정리했다.
- 작업 계획 문서에 label별 scatter plot 분석 항목을 추가해 주요 피처 조합의 분리 가능성 검토를 후속 EDA 흐름에 반영했다.

### 주의할 점
- 이번 브랜치의 실제 `main...HEAD` diff에는 issue `#27` 관련 `.agents/skills`, `.claude/skills`, `prs/27_structure-change.md` 변경이 섞여 있다. 이 PR 본문은 해당 변경을 제외하고 issue `#26` 실험/모델링 변경만 설명한다.
- 결과 파일이 대량 추가되어 diff 규모가 크다. 특히 `by_variant/`는 `by_horizon/` 결과를 variant 관점으로 다시 볼 수 있게 구성한 산출물이라 리뷰 시 요약 문서를 먼저 확인하는 편이 효율적이다.
- H20 이후 horizon은 test 표본 수가 줄어 PR-AUC 평가 분산이 커질 수 있다. H22/H24 결과는 bootstrap CI 또는 multi-seed 재실험 전까지 보수적으로 해석해야 한다.
- XGBoost와 LightGBM 일부 실험은 valid threshold가 test에 안정적으로 일반화되지 않는 패턴을 보인다. 후속 calibration 또는 threshold 안정화 검증이 필요하다.

### 영향 범위
- 기존 모델 학습 명령은 `--variant` 기본값이 `baseline`이므로 그대로 동작한다.
- 신규 processed 데이터 디렉터리 구조를 사용하는 실험은 명시적으로 `--variant`를 바꿔 재현할 수 있다.
- 모델링 결과와 리포트가 추가되지만, 수집 파이프라인이나 S3 업로드 로직에는 직접적인 런타임 변경을 주지 않는다.
- PR 리뷰에서는 `src/modeling/data_loader.py`, `src/modeling/run_all.py`, `results/exp_002_v3_baseline/summary.md`, `results/exp_003_v3_variant_compare/summary_horizon_compare.md`, `results/exp_003_v3_variant_compare/summary_variant_compare.md`를 우선 확인하면 된다.

## 주요 커밋 (issue #27 제외)

| hash | date | author | message |
|---|---|---|---|
| `f26e282` | 2026-05-12 | hann | docs : write new report by training Horizon method & exp-A/B/C #26 |
| `d0ef023` | 2026-05-06 | hann | docs : add plot_scatter_by_label part in b_work_plan.md #26 |
| `b33a294` | 2026-05-06 | hann | feat : add test of v0.1.1(H12 & winsor, robust, winsor+robust) #26 |
| `79cf7e7` | 2026-05-04 | hann | feat : learning exp_002_v3_baseline #26 |

## 핵심 변경 파일 (issue #27 제외)

- `src/modeling/data_loader.py`
- `src/modeling/run_all.py`
- `docs/b_work_plan.md`
- `results/exp_002_v3_baseline/summary.md`
- `results/exp_002_v3_baseline/*.json`
- `results/exp_003_v3_variant_compare/summary_horizon_compare.md`
- `results/exp_003_v3_variant_compare/summary_variant_compare.md`
- `results/exp_003_v3_variant_compare/by_horizon/**`
- `results/exp_003_v3_variant_compare/by_variant/**`

## 점검 결과 (S3 제외)
- 요약: `FAIL` (PASS 2 / WARN 0 / FAIL 3)

| check | status | summary |
|---|---|---|
| pr_type_alignment | PASS | auto selected -> structure |
| automation_non_s3 | FAIL | `automation.run_checks` 모듈을 찾지 못함 |
| collect_help | FAIL | `collect.py` 파일을 찾지 못함 |
| s3_uploader_v2_help | FAIL | `src.s3_uploader_v2` 모듈을 찾지 못함 |
| py_compile | PASS | ok |

## 재실행 명령

```bash
python3 scripts/pr_pipeline.py --type auto --base main --output-json prs/context.json
```
