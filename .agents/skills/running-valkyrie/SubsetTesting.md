# SubsetTesting

The `--slice :10` smoke test before launching a full 1251-task run.

## Why subset first

A full run is 4–8 h and $50–$1300. A subset is ~10 min and ~$0.50–$10. **Always smoke-test new models or new harness configs with a subset before committing to a full run.**

The subset catches:

- Missing API keys (`_get_default_api_key` traceback) — see `Secrets.md`
- Wrong model name (`_get_model_from_registry` traceback)
- Account suspended / 429-loop (Bug-G)
- Sandbox creation failures
- Custom benchmark-service unreachable / wrong port
- Webhook misconfiguration (only if you bother passing `-i` on the subset, which you usually shouldn't)

It does *not* catch:

- Long-deliverable Bug-L (judge `IndexError`) — only fires on tasks with multi-page docx
- Scaling issues (concurrency 40 wedging the tracker) — subset runs at default concurrency 5
- Bug-J (`URL component 'query' too long` on retry) — only fires on retry of large runs

## How to launch

```bash
valk run start \
  --agent harvey-labs \
  --benchmark harvey-legal-agent \
  --model <provider/model_name> \
  --slice :10 \
  -s <ENV_VAR> <secret_name>
  # NO -i flags on subsets (the user doesn't want notifs for smoke tests)
```

Default concurrency for a subset is fine — 5 parallel × 10 tasks → wraps in 2–10 min.

## Polling cadence for subsets

Subsets wrap fast; poll often:

- +2 min — confirm tasks moved off pending
- +5 min — first deep look (typical "is this thing alive" check)
- +10 min — usually wrapped or near-wrapped
- +25 min — final fail-safe; if not done, something's wrong

```bash
for id in <subset_run_ids>; do
  echo "=== $id ==="
  valk run fetch $id 2>&1 | tail -4
done
```

## Reading subset results

Three cases:

### 1. All 10 finished, 0 errors → green-light the full run

```
Finished: 10
```

You're good. Launch the full run now:

```bash
valk run start \
  --agent harvey-labs \
  --benchmark harvey-legal-agent \
  --model <provider/model_name> \
  --concurrency 40 \
  -s <ENV_VAR> <secret_name> \
  -i 25 -i 50 -i 75
```

### 2. All 10 errored → secrets/model-name issue

Pull `task_errors`:

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/harvey-legal-agent.json /tmp/sub.json
python3 -c "
import json
d = json.load(open('/tmp/sub.json'))
for tid, msg in list(d['task_errors'].items())[:1]:
    print(f'{tid}:')
    print(msg[:1500])
"
```

If the trace ends at `_get_default_api_key` → missing `-s ENV_VAR secret`.
If it ends at `_get_model_from_registry` → wrong model name *or* missing `-s`.
If it's a 429 or "insufficient balance" → provider account out of funds (Bug-G). Tell the user to refill, then re-run.

### 3. Some pass, some error → mixed bag, decide case-by-case

If 9/10 finished and 1 errored on a known transient (Bug-I, Bug-K), green-light. If 5/10 errored, dig into the errors before scaling up.

## Cost rough estimate

```
subset agent cost ≈ (full_estimate / 1251) * 10 + ε
```

So a $50 full run ≈ $0.40 subset; a $1300 full run ≈ $10.40 subset. Cheap insurance.

## When to skip the subset

- You're re-running a model that's already passed a subset and you're just iterating on a harness config change. Even then, do a `--slice :3` for a sanity check.
- You're running a known-good model + benchmark combo that hit 0 errors yesterday. Probably fine.

But generally: 10 min of subset > 4 h of "why is this all errored?". Just do the subset.
