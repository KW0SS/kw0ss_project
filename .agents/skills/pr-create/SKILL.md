---
name: pr-create
description: Generate PR markdown files and write Korean change summaries from git diff context using scripts/pr_pipeline.py. Use when the user asks to create a PR file, draft PR notes, save PR analysis under prs/, or summarize changes for a PR.
---

Always respond in Korean.
Use concise, technical Korean.
Do not switch to English unless the user explicitly asks.

When creating or updating PR markdown:

1. Default to `main` as base and `HEAD` as head.
2. Run `python3 scripts/pr_pipeline.py --type auto --base <base> --output-json prs/context.json`.
3. If the user specifies another branch, add `--head-ref <head>`.
4. If the user wants uncommitted changes included and the current branch is checked out, add `--include-worktree`.
5. Read `prs/context.json` and inspect `git diff <base>...<head>`.
6. Read the main changed files directly before writing the summary.
7. Fill the `## 변경 요약` section in Korean.

Write the change summary with these sections:

1. **변경 배경/동기**: why the change was needed
2. **주요 변경 사항**: what changed and how
3. **주의할 점**: breaking changes, dependencies, design impacts
4. **영향 범위**: effect on existing behavior

Follow these rules:

1. Focus on intent, not raw file listings.
2. Keep the summary readable and technical.
3. If the pipeline reports warnings or failures, include the PASS/WARN/FAIL outcome and one rerun command.
4. If there is no diff, say that clearly instead of forcing a PR summary.
