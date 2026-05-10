---
name: running-valkyrie
description: Playbook for running, monitoring, and validating Valkyrie benchmarks (e.g. harvey-legal-agent) end-to-end — launching runs, secrets, monitoring cadence, retrying errors, error classification (Bug-A through Bug-L), Sentry queries, cost attribution (agent + judge), subset testing, dataset token analysis, webhook notifs, and post-run validation. Use when launching, monitoring, or analyzing Valkyrie benchmark runs.
---

# running-valkyrie

A playbook for running, monitoring, and validating Valkyrie benchmarks against the `harvey-legal-agent` (and similar) benchmarks. Captured from a multi-day running session: launching runs, classifying errors, retrying, attributing cost, and the gotchas that bit us along the way.

> **Repo:** [`vals-ai/Valkyrie`](https://github.com/vals-ai/Valkyrie) — base branch is `dev` (not `main`).

## Files in this playbook

1. [`RunningTheBenchmark.md`](./RunningTheBenchmark.md) — `valk run start` end-to-end syntax: agent, benchmark, model, slice, secrets, kwargs, headers, intervals.
2. [`Secrets.md`](./Secrets.md) — AWS Secrets Manager (`localEvalInfra*Key`), the `-s ENV_VAR aws_secret_name` flag, default secret bundle, what each provider needs.
3. [`MonitoringRuns.md`](./MonitoringRuns.md) — `valk run fetch`, status states (`PENDING/IN_PROGRESS/STOPPING/STOPPED/FINISHED/ERROR`), the 5 → 25 → 45 → 90 → 120 min cadence.
4. [`RetryingErrors.md`](./RetryingErrors.md) — `valk run retry --retry`, retry semantics, end-of-pass workflow, the `URL-too-long` (Bug-J) wedge and the workaround.
5. [`ErrorClassification.md`](./ErrorClassification.md) — taxonomy of every error class we hit (Bug-A through Bug-L) with retriable / chronic disposition + Sentry links.
6. [`SentryQueries.md`](./SentryQueries.md) — how to slice issues by `benchmark_id` / `task_id`, the dataset gotcha, useful URL templates.
7. [`CostAnalysis.md`](./CostAnalysis.md) — token metrics on S3, per-task cost reconstruction, attributing the *judge* cost via the Anthropic Admin Console.
8. [`SubsetTesting.md`](./SubsetTesting.md) — the `--slice :10` smoke-test pattern before launching a full 1251-task run.
9. [`DatasetTokenAnalysis.md`](./DatasetTokenAnalysis.md) — running a tokenizer over the `harvey-legal-agent` dataset to find tasks that bust a model's context window.
10. [`WebhookNotifs.md`](./WebhookNotifs.md) — Slack webhook setup + the `-i 25 -i 50 -i 75` interval flag (max 3, divisible by 5, range 5–100).
11. [`PostRunValidation.md`](./PostRunValidation.md) — pulling `final_evaluation`, criteria-pass-rate semantics, what "reasonable" looks like, when to call a run done.

## Mental model

A "run" (a.k.a. benchmark) is a single execution of an `(agent, benchmark, model)` triple over a dataset. The agent runs in a Daytona sandbox, produces deliverables under `/workspace/results`, and a judge LLM (Anthropic Sonnet, billed against `local-api-key`) scores the deliverables against per-task criteria. The criteria-pass-rate is the headline metric; the all-or-nothing per-task score is almost always 0 because *one* failed criterion tanks the whole task.

## End-to-end happy path (what the work loop looks like)

1. **Subset smoke test** — `--slice :10`, no `-i`, default concurrency. Confirms model/secret/registry plumbing.
2. **Full run** — concurrency 35–40 (any higher and the tracker → benchmark-service hop starts to wedge), `-i 25 -i 50 -i 75` for Slack pings.
3. **Monitor** — `valk run fetch <id>` at 5 → 25 → 45 → 90 → 120 min cadence (cap at 120). Log errors to a running file.
4. **Retry pass(es)** — once status is `Finished`, `valk run retry <id> --retry --concurrency 40`. Repeat until errors are chronic (same task IDs every retry).
5. **Final evaluation** — `aws s3 cp s3://agentic-harness/benchmarks/<id>/harvey-legal-agent.json` → look at `final_evaluation.properties.criteria_pass_rate`.
6. **Cost** — see `CostAnalysis.md`. Aggregate from S3 `metrics.json` per task + Anthropic admin console for judge cost.

## What "reasonable" looks like (harvey-legal-agent, 2026-05)

| Metric | Range we saw across 6+ models | Notes |
|---|---|---|
| `criteria_pass_rate` | 0.55 – 0.65 | Sonnet 4.6 ~0.62, gpt-5.4-mini ~0.59, qwen3.6-plus ~0.58, haiku 4.5 ~0.59 |
| `passed_tasks` | 0 / 1251 | All-or-nothing; rounding noise gets you 0 every time |
| Errors | 3 – 10 chronic per run | Bug-L (judge `IndexError`) on long-deliverable tasks |
| Wall-clock | 4 – 8 h at conc 35–40 | Sonnet is slowest, gemini-flash fastest |
| Cost (1251 tasks) | $80 (gpt-5.4-mini) – $1300+ (opus 4.7) | See `CostAnalysis.md` |
