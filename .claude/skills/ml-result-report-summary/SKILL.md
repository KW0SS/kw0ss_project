---
name: ml-result-report-summary
description: Use this skill when the user asks to generate, improve, or compare machine learning result reports such as summary.md for delisting prediction models, including single experiment reports and comparison reports across horizons, preprocessing versions, model variants, or dataset variants.
---

# ML Result Report Summary Skill

## 목적

이 Skill은 상장폐지 예측 모델의 학습 결과를 `summary.md` 형태로 정리하기 위해 사용한다.

보고서는 단순히 metric을 나열하는 것이 아니라, 실험 설정, 데이터 구성, 모델별 결과, 핵심 해석, 한계점, 다음 실험 방향을 명확히 정리해야 한다.

## 보고서 유형

이 Skill은 두 가지 유형의 보고서를 작성한다.

1. 현 모델 학습 보고서
2. 비교 보고서

단일 실험 결과만 제공되면 "현 모델 학습 보고서"로 작성한다.  
여러 실험, 여러 horizon, 여러 데이터 버전, 여러 전처리 버전, 여러 모델 결과가 함께 제공되면 "비교 보고서"로 작성한다.

## 참고 템플릿

보고서 유형에 따라 다음 템플릿을 사용한다.

- 현 모델 학습 보고서: `templates/single-report-template.md`
- 비교 보고서: `templates/comparison-report-template.md`

템플릿을 그대로 복사하는 것이 아니라, 사용자가 제공한 실험 결과에 맞게 필요한 항목을 채운다.

## 공통 작성 원칙

- 정보가 없으면 추측하지 말고 "확인 필요"라고 표시한다.
- metric 수치가 제공되면 표로 정리한다.
- 표 이후에는 반드시 해석을 작성한다.
- PR-AUC를 핵심 지표로 우선 해석한다.
- ROC-AUC는 보조 지표로 사용한다.
- Accuracy는 핵심 지표로 강조하지 않는다.
- Precision과 Recall은 threshold와 함께 해석한다.
- false negative와 false positive의 비용을 모두 언급한다.
- 실험 비교 시 비교 조건이 동일한지 먼저 확인한다.
- feature importance는 인과관계로 표현하지 않는다.
- 성능 개선이 있더라도 데이터 수, positive 수, split 차이를 고려한다.
- 결론은 항상 다음 실험 방향으로 이어지게 작성한다.

## 현 모델 학습 보고서 작성 원칙

- 특정 설정의 단일 실험 결과를 설명한다.
- 이전 실험과의 비교가 일부 포함될 수는 있지만, 보고서의 중심은 현재 실험의 설정, 결과, 해석, 한계, 다음 실험 방향이다.
- 1~3번 섹션은 명확한 결과와 간략한 설명 중심으로 작성한다.
- 해석, 판단, 원인 분석은 6번 이후 섹션에서 수행한다.

## 비교 보고서 작성 원칙

- 여러 실험 결과를 같은 기준으로 비교한다.
- base 실험을 기준으로 개선점, 악화점, trade-off를 판단한다.
- base 실험이 명시되지 않은 경우, 가장 먼저 제공된 실험을 임시 base로 두되 "base 실험 확인 필요"라고 표시한다.
- 성능 비교를 시작하기 전에 반드시 비교 가능성 점검을 수행한다.
- 비교 조건이 다르면 성능 차이를 단정하지 않는다.
- 동일 기준으로 비교할 수 없는 항목은 "직접 비교 주의"라고 명시한다.

## 금지 사항

- "성능이 좋다"라고만 말하고 어떤 지표 기준인지 말하지 않는다.
- Accuracy를 핵심 성능으로 강조하지 않는다.
- ROC-AUC만 보고 모델이 우수하다고 판단하지 않는다.
- 비교 조건이 다른 실험을 단정적으로 비교하지 않는다.
- 양성 클래스 수가 적은데 성능 개선을 과도하게 일반화하지 않는다.
- feature importance를 상장폐지의 직접 원인처럼 표현하지 않는다.
- threshold 기준 없이 Precision, Recall, F1을 단정적으로 비교하지 않는다.
- base 실험이 불명확한 상태에서 비교 결론을 강하게 쓰지 않는다.