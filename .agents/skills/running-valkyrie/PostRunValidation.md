# PostRunValidation

How to confirm a run is *actually* done and read the score.

## "Done" is not the same as "Finished"

A run with `status: Finished` and `Error: 22 / Finished: 1229` is *not* done in the practical sense — the 22 errored tasks contributed 0 criteria to the rubric, dragging the headline `criteria_pass_rate` down. Always retry-pass before reading the score (see `RetryingErrors.md`).

## Pulling the final evaluation

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/harvey-legal-agent.json /tmp/run.json

python3 -c "
import json
d = json.load(open('/tmp/run.json'))
fe = d['final_evaluation']
print(f'status:          {d[\"status\"]}')
print(f'started_at:      {d[\"started_at\"]}')
print(f'finished_at:     {d[\"finished_at\"]}')
print(f'total_tasks:     {fe[\"properties\"][\"total_tasks\"]}')
print(f'passed_tasks:    {fe[\"properties\"][\"passed_tasks\"]}')
print(f'failed_tasks:    {fe[\"properties\"][\"failed_tasks\"]}')
print(f'total_criteria:  {fe[\"properties\"][\"total_criteria\"]}')
print(f'passed_criteria: {fe[\"properties\"][\"passed_criteria\"]}')
print(f'criteria_pass_rate: {fe[\"properties\"][\"criteria_pass_rate\"]:.4f}')
print(f'errors:          {len(d.get(\"task_errors\", {}))}')
"
```

## Understanding `passed_tasks`

`passed_tasks` is *all-or-nothing per task* — the task only counts as passed if every single rubric criterion passes. On harvey-legal-agent, this is almost always **0** for every model because rubrics typically have 30–80 criteria per task and one miss zeros the task out.

> **Use `criteria_pass_rate`, not `passed_tasks`. The pass-rate is the only score that meaningfully discriminates between models on this benchmark.**

## What "reasonable" looks like

The criteria pass rates we've seen across models on harvey-legal-agent (post-retry, ~3 chronic Bug-L errors per run):

| Model | criteria_pass_rate |
|---|---|
| `anthropic/claude-sonnet-4-6` | ~0.62 |
| `anthropic/claude-haiku-4-5` | ~0.59 |
| `openai/gpt-5.4-mini-2026-03-17` | ~0.59 |
| `openai/gpt-5.4-2026-03-05-high` | ~0.65 |
| `alibaba/qwen3.6-plus` | ~0.58 |
| `deepseek/deepseek-v4-pro` | ~0.55 |
| `google/gemini-3.1-flash-lite-preview` | ~0.50 |
| `xai/grok-4.20-0309-reasoning` | ~0.61 |
| `zai/glm-5.1-thinking` | ~0.50 (run aborted at 67%) |

**A pass rate below 0.45 or above 0.75 is anomalous** and worth investigating: model is degenerate or there's a scoring glitch.

## Checklist before declaring a run done

- [ ] `status == 'Finished'`
- [ ] At least one `valk run retry --retry` pass has been run
- [ ] Retry passes have plateaued (last pass produced same error count as previous)
- [ ] All remaining errors are classified (`ErrorClassification.md`):
  - Chronic Bug-L (judge `IndexError` on long deliverables) → accept
  - Bug-G (insufficient funds) → refill provider, re-retry, *then* declare done
  - Anything else → investigate before declaring done
- [ ] `criteria_pass_rate` is within the 0.45 – 0.75 sanity band (or you've confirmed why it's outside)
- [ ] Cost is recorded (agent + judge — see `CostAnalysis.md`)
- [ ] Run ID is in the leaderboard / errors_summary doc with timestamps

## What to send the user when you're done

```
*<model> full run FINISHED.*

Run ID:           <run_id>
Status:           Finished
Total tasks:      1251
Finished:         <N>
Errored:          <M>  (chronic Bug-L: <task_ids>; <other categories>)
Criteria passed:  <P> / <T>
criteria_pass_rate: <P/T>
Wall-clock:       <H>h <M>m

Cost (agent):     $<X>  (<input_tok>/<output_tok>)
Cost (judge):     $<Y>  (Anthropic Sonnet via local-api-key)
Total:            $<X+Y>
```

Plus links to:
- The errors_summary.md doc
- The Sentry issues page filtered by `benchmark_id:<run_id>`
- The CloudWatch log group

## When a run *cannot* be validated

- **Sandboxes were killed externally during the run** — finished tasks are still scored, but the run will sit in `IN_PROGRESS` because the tracker can't tell the sandboxes died. `valk run stop` returns 500. Workaround: `kill_sandboxes.py` (saved at `/tmp/kill_sandboxes.py`) directly hits the tracker DB to flip stuck `IN_PROGRESS` tasks to `STOPPED`.
- **Tracker DB is wedged** — same symptom. Force-stop via the dedicated tracker endpoint, or wait for the tracker's heartbeat reaper.
- **Harvey-legal-agent benchmark service redeployed mid-run** — sandboxes that were running at the time will fail to reconnect. Retry the run.
