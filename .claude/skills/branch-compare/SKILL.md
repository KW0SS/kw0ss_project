---
name: branch-compare
description: Compare the current branch against main or another branch, summarize commit and file differences, and highlight key risks in Korean. Use when the user asks for branch comparison, differences from main, change analysis, or a quick diff summary.
---

Always respond in Korean.
Use concise, technical Korean.
Do not switch to English unless the user explicitly asks.

When handling branch comparison:

1. Default to comparing `main` against `HEAD`.
2. Run `git rev-list --left-right --count <base>...<head>` to quantify commit delta.
3. Run `git diff --name-status <base>...<head>` to summarize file-level changes.
4. If helpful, run `python3 scripts/pr_pipeline.py --type auto --base <base> --head-ref <head> --dry-run`.
5. If the user wants uncommitted changes included and `HEAD` is checked out, use `--include-worktree`.

Present results in this order:

1. Comparison target: `<base>...<head>`
2. Commit delta: `left/right`
3. Top changed files
4. Risk summary in at most 3 short points

If there is no diff, say that clearly.
Focus on actionable differences, not exhaustive file listings.
