# feat: pipeline/structure update

## 개요
- PR 타입: `structure`
- 비교 기준: `main...37-preprocess-train-clean-feature`
- 총 변경: 5개 파일 (5 files changed, 647 insertions(+), 39 deletions(-))
- 설명: 수집/파이프라인 구조 변경 중심 PR입니다.

## 변경 요약
### 변경 배경/동기

fixed_N1 학습에서 기존 다중공선성 제거(exp_005)만으로는 recall이 크게 낮아지는 문제가 있었고, 절대값 재무 항목의 long-tail 분포와 회사 규모 신호가 모델 성능에 어떤 영향을 주는지 별도 검증이 필요했다. 이에 단일 피처 엔지니어링 3종과 그 조합을 동일한 fixed_N1 학습 파이프라인에서 비교할 수 있도록 `--fe` 실행 옵션과 전용 변환 모듈을 추가했다.

### 주요 변경 사항

- fixed_N 데이터셋용 피처 엔지니어링 모듈을 추가했다. `drop_collinear`, `ratio_total_assets`, `signed_log1p`를 지원하며, 조합 입력은 항상 `drop_collinear → ratio_total_assets → signed_log1p` 순서로 정규화해 적용한다.
- `ratio_total_assets`는 processed fixed_N CSV에 총자산 컬럼이 없는 점을 고려해 `총자산 = 유형자산 × 유형자산회전율 / 총자본회전율` 항등식으로 총자산을 복원한 뒤 절대값 5개를 총자산 대비 비율로 치환한다. 이 항등식이 깨지는 robust-scale variant(exp-B, exp-C)는 자동 skip하도록 했다.
- `run_all_fixed.py`에 `--fe` 옵션을 추가해 단일 FE와 `a+b`, `a+c`, `a+b+c` 같은 조합 토큰을 실험 단위로 실행하고, 결과 JSON 파일명과 payload에 FE 라벨을 남기도록 확장했다.
- `load_and_prepare_fixed()`는 FE가 지정된 경우 변환 모듈에 피처 정리를 위임하고, FE가 없는 기존 실행에서는 기존 다중공선성 제거 동작을 유지해 exp_004/005 재현성을 보존한다.
- exp_008 리포트에서 단일 FE 3종을 비교했고, `signed_log1p`가 RF exp-A 기준 PR-AUC 0.2866 / F1 0.3559 / Recall 0.3750으로 fixed_N1 신규 후보임을 확인했다.
- exp_009 리포트에서 FE 조합 4종을 비교했고, 어떤 조합도 단일 `signed_log1p`를 넘지 못해 FE 조합 탐색을 종료하는 근거를 남겼다.

### 주의할 점

- `ratio_total_assets`는 raw-scale 값의 곱셈 항등식에 의존하므로 robust-scale variant에서는 실행되지 않는다. B를 포함한 조합(A+B, B+C, A+B+C)은 baseline·exp-A만 학습된다.
- FE가 지정되면 기존 로더의 자동 다중공선성 제거가 비활성화된다. `drop_collinear`를 명시한 실험만 해당 컬럼 제거를 수행하므로, 기존 결과와 비교할 때 FE 라벨을 반드시 함께 봐야 한다.
- 실험 결론상 FE를 누적 적용하는 방식은 상보적이지 않았다. 특히 `drop_collinear+signed_log1p`는 단일 `signed_log1p`의 recall 회복 효과를 상쇄한다.
- PR 점검은 `py_compile`만 PASS했고, 자동화 모듈/구 엔트리포인트(`automation.run_checks`, `collect.py`, `src.s3_uploader_v2`) 부재로 3개 check가 FAIL했다. 변경 코드 자체의 문법 검사는 통과했지만, 프로젝트의 PR 자동 점검 스크립트 의존성은 별도 정리가 필요하다.

### 영향 범위

- 기존 `run_all_fixed.py`의 기본 실행(`--fe none`)은 기존 다중공선성 제거 동작을 유지한다.
- 신규 FE 실험은 `fixed_N` 모델링 파이프라인에만 영향을 주며, DART 수집/S3 업로드 경로에는 직접 변경이 없다.
- 결과 산출물은 요약 리포트만 Git에 포함되고, 개별 실험 JSON은 `.gitignore` 정책에 따라 로컬/외부 저장 대상으로 유지된다.
- 운영 후보는 조합 실험 결과가 아니라 exp_008의 `RF fixed_N1 exp-A signed_log1p`를 우선 검토하는 방향으로 정리됐다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `2200517` | 2026-05-18 | hann | train : train using clean-up feature combo in 009 #37 |
| `2d850b3` | 2026-05-18 | hann | train : train using clean-up feature in 008 #37 |

</details>

<details>
<summary>변경 파일 상세</summary>

**src/**
  - `src/modeling/data_loader_fixed.py` (수정)
  - `src/modeling/feature_engineering.py` (추가)
  - `src/modeling/run_all_fixed.py` (수정)
**other/**
  - `results/exp_008_fixed_N1_feature_eng/summary.md` (추가)
  - `results/exp_009_fixed_N1_feature_combo/summary.md` (추가)

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
