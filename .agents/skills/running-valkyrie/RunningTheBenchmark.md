# RunningTheBenchmark

End-to-end syntax for `valk run start`, plus the lifecycle commands (`fetch`, `retry`, `resume`, `stop`).

## Launching a run

```bash
valk run start \
  --agent harvey-labs \
  --benchmark harvey-legal-agent \
  --model openai/gpt-5.4-mini-2026-03-17 \
  --concurrency 40 \
  -s GOOGLE_API_KEY localEvalInfraGoogleKey \
  -i 25 -i 50 -i 75 \
  --slice :10
```

Required flags:

- `--agent <name>` — S3 agent name (e.g. `harvey-labs`) or local path (`agents/claude_code`)
- `--benchmark <name>` — registered benchmark, e.g. `harvey-legal-agent`. Must match a custom benchmark service the harness knows about.
- `--model <key>` — provider/model key, e.g. `openai/gpt-5.4-mini-2026-03-17`, `anthropic/claude-sonnet-4-6`, `google/gemini-3-flash-preview`. The model must exist in `model_library` *and* the matching API key must be reachable (see `Secrets.md`).

Useful optional flags:

- `--concurrency N` — parallel tasks. **Sweet spot is 35–40.** 50+ wedged the tracker → benchmark-service hop in our runs (timeouts, dropped sandboxes). Default = 5 if you forget.
- `--slice :N` / `--slice A-B` — subset (e.g. `:10` for first 10 tasks). See `SubsetTesting.md`.
- `--task-ids id1,id2,id3` *or* `--task-ids-file path.txt` — explicit task list. **Caveat:** does not chunk under the hood; the OR filter still pulls all stopped tasks. Don't rely on this to dodge URL limits.
- `-s ENV_VAR aws_secret_name` — secret injection. Repeatable. See `Secrets.md`.
- `-k key value` — kwargs forwarded to the agent (e.g. `-k temperature 0`). Repeatable.
- `-H HeaderName HeaderValue` — extra HTTP header on benchmark-service calls. Repeatable.
- `-i N` — Slack notification at progress threshold N%. Max 3, divisible by 5, range 5–100. Typical: `-i 25 -i 50 -i 75`. *Skip on subsets* (we don't want notifs for 10-task smoke tests).
- `--dataset NAME` — alternate dataset, defaults to `default`.
- `--lambda NAME` — Lambda to invoke at end of run.
- `--ignore-custom-services` / `--ics` — skip custom benchmark services.

Output gives you the run UUID, plus shortcuts for tracking/results/stop/resume/retry. **Save the run ID.** Everything downstream keys on it.

## The lifecycle commands

```bash
# Live progress (formatted, single-shot)
valk run fetch <run_id>

# Live streaming (tails the run; Ctrl+C to exit)
valk run fetch <run_id> --connect

# Pull final results JSON
valk run results <run_id> --path ./results.json

# Stop a running benchmark gracefully (lets in-progress tasks drain)
valk run stop <run_id>

# Resume a STOPPED run from where it left off
valk run resume <run_id>

# Reset ERROR tasks → PENDING and resume
valk run retry <run_id> --retry --concurrency 40

# Pull all agent outputs from S3 (per-task subdirectories)
valk agent outputs <run_id> --output-dir ./outputs
```

## Run statuses you'll actually see

- `IN_PROGRESS` — sandboxes spinning, tasks moving
- `STOPPING` — `stop` request landed, in-progress tasks draining
- `STOPPED` — manually halted; resumable
- `FINISHED` — ran to completion (with or without errors)
- `ERROR` — a task-level state, not a run-level one. `Error: N` in the fetch output is the count of tasks in `ERROR` state.

A run can be `FINISHED` with `Error: 22 / Finished: 1229`. That's normal — retry to clear retriable errors (see `RetryingErrors.md`).

## Where the run lives in S3 / CloudWatch

- S3: `s3://agentic-harness/benchmarks/<run_id>/` — `harvey-labs.zip` (the bundled agent), per-task output directories, `harvey-legal-agent.json` (the final evaluation summary)
- CloudWatch: `benchmarks/<run_id>` log group (us-east-1). Per-task log streams keyed by task ID.

The `harvey-legal-agent.json` is your one-stop file for `final_evaluation`, `benchmark_arguments` (what flags the run launched with — check this if you've forgotten which secrets you used), and the `task_errors` map (task_id → error message).
