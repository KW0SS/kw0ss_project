# docs: add ML experiment skills

## 개요
- PR 타입: `structure`
- 비교 기준: `26-model-retrain-on-eda-refined-dataset-v011...27-feat-add-summary-of-learning-result-skill-in-claude-codex`
- 총 변경: 10개 파일 (10 files changed, 926 insertions(+))
- 설명: Claude/Codex에서 ML 학습 전략과 실험 결과 보고서를 일관되게 작성하기 위한 skill 문서 추가 PR입니다.

## 변경 요약

### 변경 배경/동기

26번 브랜치에서 정제 데이터 기반 H10/H12 재학습과 전처리 variant 비교가 진행되면서, 이후 실험을 어떤 순서로 설계하고 결과를 어떤 형식으로 정리할지 표준화할 필요가 생겼다. 이번 PR은 모델 학습 전략 수립과 실험 결과 보고서 작성을 Claude/Codex 양쪽에서 같은 기준으로 수행할 수 있도록 전용 skill과 템플릿을 추가한다.

### 주요 변경 사항

- `ml-training-strategy` skill을 추가했다.
  - 현재 baseline, 목표 지표, horizon sweep, 전처리 variant, threshold policy, 튜닝, 해석 단계를 순서대로 정리하도록 workflow를 정의했다.
  - 실험 우선순위를 baseline 검증 → horizon sweep → 전처리 비교 → 튜닝 → threshold 정책 → feature engineering/ensemble/interpretation 순서로 안내한다.
  - 실행 가능한 run queue, stop/go 기준, 산출물 계획을 포함하는 training strategy 템플릿을 추가했다.

- `ml-result-report-summary` skill을 추가했다.
  - 단일 실험 보고서와 비교 보고서를 구분해 작성하도록 report mode를 정의했다.
  - metric 값을 임의로 추정하지 않고, 관찰과 추천을 분리하며, base-vs-candidate 비교는 양쪽 결과가 있을 때만 수행하도록 규칙을 명시했다.
  - 단일 실험용 템플릿과 실험 비교용 템플릿을 추가해 데이터 구성, 모델 결과, threshold, error analysis, next experiments까지 같은 구조로 정리할 수 있게 했다.

- Claude와 Codex용 skill 경로를 모두 지원했다.
  - `.claude/skills/...`와 `.agents/skills/...`에 동일한 skill 및 template 구성을 추가했다.
  - Codex용 skill description은 현재 세션에서 바로 라우팅될 수 있도록 유지하고, Claude 쪽도 같은 작성 기준을 공유하도록 맞췄다.

- 잘못 덮어쓴 conflict 커밋을 복구했다.
  - 중간 커밋에서 `.claude` 쪽 일부 skill 파일이 한국어 버전으로 덮였으나, revert 커밋으로 영어 기준 문서를 복구했다.
  - 최종 diff 기준으로는 skill 문서와 템플릿 10개가 신규 추가된 상태다.

### 주의할 점

- 이번 PR은 코드 실행 로직이나 데이터 파이프라인을 변경하지 않는다. 변경 범위는 Claude/Codex skill 문서와 템플릿에 한정된다.
- skill 기본 출력 언어는 영어로 정의되어 있으며, 사용자가 한국어를 요청할 경우에만 한국어로 작성하도록 되어 있다.
- `.claude`와 `.agents`에 같은 내용을 중복 배치하므로, 이후 skill 수정 시 두 경로를 함께 갱신해야 한다.
- 커밋 히스토리에는 conflict 커밋과 revert 커밋이 함께 남아 있다. 최종 파일 내용은 복구됐지만, 머지 전 히스토리 정리가 필요하면 squash/rebase 대상이다.

### 영향 범위

- Claude/Codex가 ML 학습 전략 요청과 실험 결과 요약 요청을 더 일관된 구조로 처리할 수 있다.
- 26번 브랜치의 모델 결과 산출물에는 직접 영향이 없고, 이후 H별/variant별 실험 계획과 보고서 작성 품질에만 영향을 준다.
- 런타임 의존성, 데이터 파일, 모델 학습 CLI, 전처리 산출물은 변경되지 않는다.

<details>
<summary>커밋 히스토리</summary>

| hash | date | author | message |
|---|---|---|---|
| `2b127b4` | 2026-05-12 | hann | Revert "docs : error (conflict) #27" |
| `ffb31ce` | 2026-05-12 | hann | docs : error (conflict) #27 |
| `d67d8d0` | 2026-05-12 | hann | docs : add same skills at .agents and translation skill.md to english #27 |
| `26b482f` | 2026-05-12 | hann | docs : add skill of ml-result-report-summary #27 |
| `ad9da12` | 2026-05-12 | hann | docs : add skill of ml-training-strategy #27 |

</details>

<details>
<summary>변경 파일 상세</summary>

**other/**
  - `.agents/skills/ml-result-report-summary/SKILL.md` (추가)
  - `.agents/skills/ml-result-report-summary/templates/comparison-report-template.md` (추가)
  - `.agents/skills/ml-result-report-summary/templates/single-report-template.md` (추가)
  - `.agents/skills/ml-training-strategy/SKILL.md` (추가)
  - `.agents/skills/ml-training-strategy/templates/training-strategy-template.md` (추가)
  - `.claude/skills/ml-result-report-summary/SKILL.md` (추가)
  - `.claude/skills/ml-result-report-summary/templates/comparison-report-template.md` (추가)
  - `.claude/skills/ml-result-report-summary/templates/single-report-template.md` (추가)
  - `.claude/skills/ml-training-strategy/SKILL.md` (추가)
  - `.claude/skills/ml-training-strategy/templates/training-strategy-template.md` (추가)

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

## 앞으로 진행할 내용
- PR 리뷰 후 `.claude` / `.agents` skill 내용이 동일하게 유지되는지 확인
- 필요 시 conflict/revert 커밋을 squash해 skill 추가 커밋 중심으로 히스토리 정리
