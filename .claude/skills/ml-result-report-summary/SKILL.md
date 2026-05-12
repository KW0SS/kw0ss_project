---
name: ml-result-report-summary
description: Create concise English reports for machine-learning experiment results, including single-run reports and comparison reports across horizons, preprocessing variants, or model families.
---

Use this skill when the user asks to summarize ML training results, write an experiment report, compare model runs, compare horizons such as H10/H12/H14/H16, or turn result artifacts into a structured report.

Default language: English.
If the user asks for Korean, write the final report in Korean but keep template section semantics unchanged.

## Report Types

Choose one of two report modes before writing:

1. Single experiment report
   - Focuses on the current training result.
   - Comparisons to previous runs may be included, but they are secondary.
   - Use `templates/single-report-template.md`.

2. Comparison report
   - Focuses on comparing multiple experiments under a shared evaluation frame.
   - Use this for horizon comparisons, preprocessing comparisons, model-family comparisons, or base-vs-candidate analysis.
   - Use `templates/comparison-report-template.md`.

## Required Workflow

1. Identify the report mode from the user request and available files.
2. Read the relevant `summary.md`, per-horizon summaries, metrics CSV/JSON files, and test outputs before writing.
3. Preserve factual metric values exactly. Do not invent missing metrics.
4. Separate observations from recommendations.
5. Make base-vs-candidate comparisons only when both sides are present.
6. If a metric is unavailable, write `Not available` rather than estimating it.

## Single Report Guidance

Emphasize:

- What was trained
- Data and target definition
- Preprocessing and feature setup
- Model performance
- Best model and threshold behavior
- Main interpretation
- Failure modes and next experiments

Do not let historical comparison dominate this report.

## Comparison Report Guidance

Emphasize:

- The comparison question
- Baseline definition
- Controlled variables
- Changed variables
- Metric deltas
- Horizon or preprocessing trade-offs
- Operational recommendation

Always state whether a candidate improves, matches, or worsens against the baseline.

## Style

- Be concise and technical.
- Prefer tables for metrics.
- Use bullet points for findings.
- Avoid promotional language.
- Avoid causal claims unless the experiment design supports them.
