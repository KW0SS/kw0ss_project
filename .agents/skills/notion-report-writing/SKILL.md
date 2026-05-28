---
name: notion-report-writing
description: Write experiment or analysis reports into Notion's 보고서 page and 보고서 데이터베이스 from a specified summary.md. Use when the user asks to create a new Notion report, baseline report, comparison test report, model result report, EDA report, dataset cleaning report, experiment design report, or model comparison report in Notion.
---

Always respond in Korean.
Use concise, technical Korean.

Use this skill when the user asks to write a report into Notion from a local `summary.md` or equivalent result artifact.

## Inputs

Required:

1. Source report file, usually a user-specified `summary.md`.

Optional:

1. `사람` property value, e.g. `이정한`.
2. Explicit report type.
3. Parent report title or existing parent page/database item.

If `summary.md` is not specified, find the most relevant `summary.md` from the user's context or ask for the path only when multiple plausible files exist.

## Notion Target

1. Find the Notion page named `보고서`.
2. Inside it, find `보고서 데이터베이스`.
3. Create the report in `보고서 데이터베이스`.
4. When referencing existing report formatting, use only entries whose `사람` property is `이정한`.
5. Do not use pages with any other `사람` value as formatting or content references.

## Fixed Properties

When creating a report item:

1. `멘션`: leave empty.
2. `진행도`: always set to `작성중`.
3. `사람`: set to the user-specified value. If omitted, leave absent/empty.
4. `타입`: infer automatically from the content unless the user explicitly specifies it.

Allowed `타입` values:

1. `todo`
2. `모델 학습 결과`
3. `EDA`
4. `데이터셋 정제`
5. `실험 설계`
6. `모델 비교`

## Type Inference

Infer `타입` using the dominant purpose of the source content:

1. `모델 비교`: multiple models, variants, horizons, baselines, or candidate-vs-baseline comparisons.
2. `모델 학습 결과`: a single training run, model metrics, threshold, feature setup, or test/valid performance.
3. `EDA`: exploratory analysis, data profiling, distributions, missingness, correlation, or visualization findings.
4. `데이터셋 정제`: cleaning, filtering, deduplication, labeling, schema normalization, or dataset build reports.
5. `실험 설계`: planned experiments, strategy, protocol, validation plan, or hypothesis design.
6. `todo`: task list or action-oriented planning without completed experiment results.

If multiple types apply, choose the most specific result category. Prefer `모델 비교` over `모델 학습 결과` when the report compares variants.

## Hierarchy Rule

Always create or identify the parent item first, then place child report items under that parent.

1. If the user specifies a parent, fetch or create that parent item first.
2. If no parent is specified, create a parent item that represents the experiment/report group before creating detailed child items.
3. Put the new result report as a child/sub-item of that parent when the database supports hierarchy or relation properties.
4. Do not create only a child item without first ensuring the parent exists.

## Writing Workflow

1. Read the specified `summary.md` and any nearby JSON/CSV metrics referenced by it if needed for exact values.
2. Search Notion for `보고서`, fetch it, and identify `보고서 데이터베이스`.
3. Fetch the database schema before creating pages.
4. Query or search for sample entries where `사람 = 이정한`; use only those pages' section layout, title style, and level of detail as the formatting reference.
5. Create or identify the parent item first.
6. Create the child result report in the database with fixed properties and inferred `타입`.
7. Keep the Notion body factual:
   - experiment purpose
   - input data and preprocessing
   - split and validation setup
   - key metrics
   - interpretation
   - risks or caveats
   - next actions

## Content Rules

1. Preserve metric values exactly from source files.
2. Do not invent missing metrics.
3. Use Korean headings and concise bullet points.
4. Prefer tables for metric comparisons.
5. Separate observations from recommendations.
6. If writing from a comparison `summary.md`, state the baseline and candidate clearly.
7. If writing from a baseline-only `summary.md`, avoid overstating comparative conclusions.

## Completion Response

After creating the Notion report, respond with:

1. Created report title.
2. Notion page URL.
3. Parent item used or created.
4. Any fields left empty by rule, especially `멘션` and omitted `사람`.
