# CostAnalysis

How to attribute the dollar cost of a run — agent side and judge side.

## The two cost streams

A run costs you money on *two* providers:

1. **Agent side** — the model under test (`gpt-5.4-mini`, `claude-sonnet-4-6`, `qwen3.6-plus`, etc). Recorded per-task in `metrics.json` on S3.
2. **Judge side** — Anthropic Sonnet 4.6 used as the rubric judge for *every* run (regardless of which agent ran). Billed via the `local-api-key` Anthropic API key. Visible in the Anthropic Admin Console only.

The judge cost is often *larger than the agent cost*. We saw $1700 of Sonnet judge cost during a single 4-run overnight wave (haiku + kimi + glm + grok), all attributed to `local-api-key`. Don't forget to count it.

## Pulling agent-side cost from S3

For a single task:

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/<task_id>/metrics.json /tmp/m.json
python3 -c "
import json
m = json.load(open('/tmp/m.json'))
print(json.dumps(m, indent=2))
"
```

`metrics.json` typically looks like:

```json
{
  "cost": {
    "total": 0.0432,
    "input": 0.0181,
    "output": 0.0211,
    "reasoning": 0.0040,
    "cache_read": 0.0000
  },
  "tokens": {
    "input": 75320,
    "output": 4218,
    "cache_read": 0,
    "reasoning": 1024
  },
  "turns": 14,
  "duration_seconds": 187.4
}
```

Aggregate across all tasks:

```bash
TASK_DIRS=$(aws s3 ls s3://agentic-harness/benchmarks/<run_id>/ \
  | awk '{print $NF}' | grep '/$' | grep -v 'harvey-labs')
total_cost=0
for dir in $TASK_DIRS; do
  metrics=$(aws s3 cp s3://agentic-harness/benchmarks/<run_id>/${dir}metrics.json - 2>/dev/null)
  cost=$(echo "$metrics" | jq -r '.cost.total // 0')
  total_cost=$(echo "$total_cost + $cost" | bc -l)
done
echo "Total agent cost: \$$total_cost"
```

Or use the bundle file if it exists:

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/metrics_total.json /tmp/total.json 2>/dev/null \
  && python3 -c "import json; print(json.dumps(json.load(open('/tmp/total.json'))['cost'], indent=2))"
```

The script `/tmp/aggregate_metrics.sh` (saved during the running session) also does this.

## Per-task cost CSV

For a model breakdown by task:

```bash
python3 - <<'PY'
import json, csv, subprocess
run_id = "<run_id>"
out = []
for line in subprocess.check_output(["aws","s3","ls",f"s3://agentic-harness/benchmarks/{run_id}/","--recursive"]).decode().split("\n"):
    if line.endswith("metrics.json"):
        path = line.split()[-1]
        task_id = path.split("/")[-2]
        body = subprocess.check_output(["aws","s3","cp",f"s3://agentic-harness/{path}","-"]).decode()
        m = json.loads(body)
        out.append({
            "task_id": task_id,
            "cost_total": m.get("cost", {}).get("total", 0),
            "input_tokens": m.get("tokens", {}).get("input", 0),
            "output_tokens": m.get("tokens", {}).get("output", 0),
            "turns": m.get("turns", 0),
        })
with open(f"/tmp/{run_id}_per_task_cost.csv","w") as f:
    w = csv.DictWriter(f, fieldnames=out[0].keys())
    w.writeheader()
    w.writerows(out)
print(f"Wrote /tmp/{run_id}_per_task_cost.csv ({len(out)} rows)")
PY
```

## Estimating full-run cost from a partial run

Stopping at ~100 finished tasks and extrapolating is the standard move when the per-task cost is unknown for a new model. Do this:

1. Run `valk run start ...` (no `--slice`) and let it hit 100 finished tasks.
2. Stop with `valk run stop <run_id>` (or `kill_sandboxes.py` if the stop endpoint is wedged — see *Daytona* note below).
3. Aggregate cost across the 100 finished tasks.
4. Multiply by 1251/100 = 12.51 for the full-benchmark estimate.

Real numbers we recorded (per ~100-task sample):

| Model | Per-100 sample cost | Estimated full 1251 |
|---|---|---|
| `openai/gpt-5.4-mini-2026-03-17` | $5.92 | $74.06 |
| `anthropic/claude-sonnet-4-6` | $42.70 | $534.21 |
| `anthropic/claude-opus-4-7` | $103.40 | **$1293.51** |
| `alibaba/qwen3.6-plus` | $7.81 | $97.71 |

(Numbers are agent-side only — judge cost is on top, see below.)

## Judge-side cost (the part everyone forgets)

Every run scores deliverables via Anthropic Sonnet 4.6 against per-task rubrics. The judge calls go through the `local-api-key` Anthropic API key. They are *not* in any per-run S3 file — they live only in the Anthropic Admin Console.

### Pulling judge cost from the Anthropic Admin Console

You need an *Admin* API key (not the regular `local-api-key` itself). Generate one in the Anthropic Console at *Settings → API Keys → Create Admin Key* (workspace-wide). Then:

```bash
curl -s "https://api.anthropic.com/v1/organizations/usage_report/messages?starting_at=2026-05-09T00:00:00Z&ending_at=2026-05-10T00:00:00Z&group_by[]=api_key_id&bucket_width=1h" \
  -H "x-api-key: $ANTHROPIC_ADMIN_KEY" \
  -H "anthropic-version: 2023-06-01" \
  | jq '.data[] | select(.api_key_id == "<local-api-key-id>")'
```

Or pull a CSV from the Admin Console UI (Usage tab → Export CSV) and grep for the `local-api-key` rows.

### Attributing judge cost to a specific run

The Anthropic API doesn't tag requests by Valkyrie run ID. So you have to align by *time window*: each run's `started_at` and `finished_at` are in `harvey-legal-agent.json`; bucket the judge usage to those windows.

### Real numbers from our overnight run (2026-05-09)

```
hour (UTC)    input tokens     output tokens    cost ($)
18:00         9,540,217        2,103,481        $36.42
19:00         12,418,602       2,872,140        $48.71
20:00         18,772,933       4,210,807        $73.12
21:00         24,103,114       5,614,288        $94.06
22:00         29,847,512       6,723,914        $114.63
total                                           $366.94 (in 5h)
```

This was during the haiku/kimi/glm/grok wave. The single highest *hour* of judge spend was during the kimi run wrap; the dataset evidently has a few tasks with heavy criteria.

### Cache-controlled judge

The judge prompt has heavy structural overlap (per-task rubric template). Adding `cache_control: ephemeral` on the rubric prefix would slash the judge cost. We did not implement this — flagged for future work.

## Caveats

- `metrics.json` is written by the agent; if the agent dies before writing it, the task has no cost record. Manually estimate by averaging.
- Cache-read cost may be reported separately; sum `input + output + reasoning + cache_read` for total agent spend.
- If a model lies about its token usage (some providers under-report `output_tokens`), the cost numbers will be off. Cross-check with provider's billing dashboard once a quarter.
