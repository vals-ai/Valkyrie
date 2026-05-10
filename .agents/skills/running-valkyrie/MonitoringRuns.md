# MonitoringRuns

The cadence and the tools.

## Default polling cadence

```
+5 min  →  +25 min  →  +45 min  →  +90 min  →  +120 min  →  +120 min  →  ... (capped at 120)
```

Why this cadence:

- **+5 min** catches the obvious blowups: missing secret, model name typo, model registry miss, sandbox refused to start. If the run was going to die in the first minute, you've already spent the cycles to know.
- **+25 / +45 min** catch slow leaks (rate limit loops, sandbox creation failures, stuck-at-N-tasks).
- **+90 / +120** are the steady-state poll. Runs with concurrency 35–40 wrap in 4–8h, so 120-min cadence is enough granularity to catch new error patterns.

For subset (`--slice :10`) tests, you usually only need +5 and +25 — they wrap fast.

## Single-shot fetch

```bash
valk run fetch <run_id>
```

Output (formatted):

```
[████████████████░░░░░░░░░░░░░░] 425/1251 (34.0%) • In Progress
Pending: 786 │ In Progress: 40 │ Evaluating: 2 │ Error: 19 │ Finished: 404
```

The headline counts:

- **Pending** — task hasn't been picked up yet
- **In Progress** — agent sandbox is running (max == `--concurrency`)
- **Evaluating** — agent finished, judge is scoring deliverables
- **Error** — task hit a terminal error in agent or evaluator
- **Finished** — agent + evaluator both completed (regardless of pass/fail)

`Pending + In Progress + Evaluating + Error + Finished == total_tasks`. If they don't add up, the tracker is wedged (rare).

Status line:

- `In Progress` — run is live
- `Stopping` — `valk run stop` landed, draining
- `Stopped` — manually halted; resumable
- `Finished` — run done (status doesn't say "successfully done"; check `Error: N`)

## Streaming fetch

```bash
valk run fetch <run_id> --connect
```

Tails forever. Useful when you want to babysit a `--slice :10` to completion. Press Ctrl+C to detach.

## Polling all your runs at once

```bash
for id in 4aa3ff7c-... a6ca4d1e-... 71527464-...; do
  echo "=== $id ==="
  valk run fetch $id 2>&1 | tail -4
done
```

This is the default move during overnight runs. Pipe to `tail -4` because `fetch` prints a header banner you don't need every time.

## When to escalate

- **Stuck "in progress" for 30+ min on a single task** — likely a sandbox stuck. Check Sentry: there'll usually be a heartbeat-timeout log around the time the task should have wrapped. Stopping/retrying clears it.
- **Pending > 0 with In-Progress at 0** — workers are dead or the tracker isn't dispatching. Check tracker / benchmark-service health.
- **Error count climbing every poll** — new bug class. Pull Sentry, classify (`ErrorClassification.md`), decide retriable vs chronic.
- **`In Progress` capped below `--concurrency`** — you're rate-limited or a provider is throttling. Check provider's status page; consider lowering concurrency.

## Run-status REST hit (when CLI is too slow)

The tracker's `/benchmarks/{id}` endpoint returns the same data as `valk run fetch` but without the formatted output. Useful for scripting:

```bash
curl -s -H "X-API-Key: $TRACKER_API_KEY" \
  "https://tracker.<env>/benchmarks/<run_id>" | jq '.tasks_by_status'
```

(Replace `<env>` with the deployed tracker host; check `~/.config/valk/config.toml` for the URL the CLI uses.)

## Run-state from S3 (for runs that have already finished)

For a finished run, just pull the JSON:

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/harvey-legal-agent.json /tmp/run.json
python3 -c "
import json
d = json.load(open('/tmp/run.json'))
print('status:', d['status'])
print('finished_at:', d['finished_at'])
print('errors:', len(d.get('task_errors', {})))
print('criteria_pass_rate:', d['final_evaluation']['properties']['criteria_pass_rate'])
"
```

This is also the only source of truth for the `criteria_pass_rate` — `valk run fetch` doesn't show it.
