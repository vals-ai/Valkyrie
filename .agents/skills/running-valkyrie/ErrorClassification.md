# ErrorClassification

The full taxonomy of errors we hit running `harvey-legal-agent` against ~10 models, with retriable / chronic disposition and Sentry links.

> Maintain this list as you go. Every new bug class gets a Bug-X letter, a one-line symptom, root cause, and disposition. Keep `errors_summary.md` in sync.

## Quick reference

| Bug | Symptom | Cause | Retriable? |
|---|---|---|---|
| **A** | `scores.json` truncation in S3 download | Daytona `download_files` cap on large outputs | Yes (transient) |
| **B** | Daytona container IP refresh 502 | Daytona load-balancer flake | Yes |
| **C** | Sandbox heartbeat-timeout / health-check 500 | Daytona PTY layer flake | Yes |
| **D** | `agent_exit_code: 1` mid-run on parsing | Agent CLI bug parsing certain markdown deliverables | Sometimes |
| **E** | Hanging evaluation past 60 min | Judge LLM stuck or sandbox PTY wedged | Sometimes (cap eval allowance) |
| **F** | Context-window exceeded (explicit error class as of 2026-05) | Model hit prompt-token cap | No (model-side) |
| **G** | `429: insufficient balance` loop | Provider account out of funds (we hit it on Moonshot/kimi) | After refill |
| **H** | (reserved) | (reserved) | — |
| **I** | `ConnectionClosedError 1011 keepalive ping timeout` | benchmark-service `--ws-ping-timeout 60` too short for heavy I/O | Yes |
| **J** | `valk run retry` 400 — `URL component 'query' too long` | tracker → benchmark-service `verify_task_ids` GET busts URL limit | Workaround only |
| **K** | `AgentRunFailedError exit code 1` on immigration / employment-labor tasks | Agent sandbox flake on specific task types | Yes |
| **L** | Judge `IndexError: list index out of range` on `response.content[0].text` | Anthropic Sonnet judge returns empty content on long deliverables | **No (chronic)** |

## Bug-A — `scores.json` truncation

**Where it lands:** `evaluation/scoring.py` raises `JSONDecodeError` because the downloaded `scores.json` from Daytona is truncated mid-byte.
**Root cause:** Daytona's `download_files` had an output-buffer cap; large `scores.json` (66 KB+) lost the tail.
**Sentry pattern:** look for `JSONDecodeError` with line numbers near the end of the file.
**Disposition:** retriable; chronic on the same task means the `scores.json` keeps blowing the cap. We could not 100% repro in isolation.
**Recommendation:** stream the file or chunk-download. See `/home/ubuntu/run_logs/repro_scores_truncation.py` for the test harness.

## Bug-I — keepalive 1011

```
ConnectionClosedError: sent 1011 (internal error) keepalive ping timeout;
no close frame received
```

**Site:** `benchmark-service`'s WebSocket → tracker connection closes mid-evaluation when the agent is doing heavy I/O (`cp -r`, `upload_files`).
**Root cause:** server-side `--ws-ping-timeout 60` in `harvey-legal-agent-benchmark-service/Dockerfile:29`. Heavy I/O blocks the asyncio event loop for ≥60 s, the ping ack misses, and the connection drops.
**Tasks regularly hit:**
- `funds-asset-management_draft-lpa_scenario-03`
- `funds-asset-management_draft-lpa_scenario-07`
- a couple intellectual-property tasks (large PDFs)

**Disposition:** retriable. Most fail once and clear on retry pass.
**Recommendation:** bump `--ws-ping-timeout` to `300`, or yield progress chunks during heavy I/O so the event loop isn't blocked.

## Bug-K — `AgentRunFailedError exit code 1`

**Sentry issue:** [VALKYRIE-2N](https://vals-ai.sentry.io/issues/VALKYRIE-2N)
**Symptom:** `AgentRunFailedError: Sandbox error: Failed to run command cd /workspace && PYTHONSAFEPATH=1 harvey-legal-agent ... exit code: 1`
**Tasks hit:**
- `immigration_compare-uscis-filing-receipt-against-original-petition-submission`
- `immigration_draft-appeal-brief`
- `employment-labor_draft-investigation-plan-for-workplace-harassment-and-retaliation-complaint`

**Site:** raised at `services/tracker/src/tracker/sandbox.py` in `stream_command_output`. Pre-existing issue group; first seen weeks before our runs, fires on multiple benchmarks.
**Disposition:** retriable. Most clear on retry pass.

## Bug-L — Judge `IndexError` on `response.content[0]`

**Symptom (in `task_errors`):**

```
Sandbox error: ... exit code: 1
Last output:
  ...
  No fuzzy match for deliverable 'joint-development-agreement.docx': joint-development-agreement.docx
  ...
  File "/workspace/evaluation/judge.py", line 86, in evaluate
    text = response.content[0].text
           ~~~~~~~~~~~~~~~~^^^
IndexError: list index out of range
```

**The "No fuzzy match" lines are red herrings** — they're warnings logged before the trace. The actual failure is the judge LLM (Anthropic Sonnet 4.6) returning an empty `content` array.

**Why empty?** The deliverable docx is huge → the judge prompt blows past Sonnet's effective output budget → response stops with `stop_reason: "max_tokens"` and *no* text in `content`. The harness then crashes on `response.content[0]`.

**Tasks hit (chronic — same task IDs every retry pass):**
- `intellectual-property_draft-joint-development-agreement`
- `energy-natural-resources_analyze-counterparty-markup-of-intercreditor-agreement`
- `intellectual-property_rnw-ip-license-renewal`
- `arbitration-international-dispute-resolution_extract-findings-from-arbitral-award`
- `arbitration-international-dispute-resolution_analyze-counterparty-markup-of-arbitration-agreement`
- `employment-labor_offer-letter-to-employment-agreement`
- `funds-asset-management_draft-lpa_scenario-XX` (subset)

**Disposition: not retriable.** Three retry passes all preserved the same task IDs.

**Fix recommendations:**

1. **Defensive (smallest):** in `evaluation/judge.py:86`, check `if not response.content: ...` and treat the criterion as "ungraded" / 0 instead of bubbling `IndexError`.
2. **Better:** detect `stop_reason == "max_tokens"` and retry with smaller chunk or higher `max_tokens`.
3. **Best:** chunk the deliverable input in `evaluate_from_file` so the judge sees `~50K input + ~4K output` instead of `~150K input + max-tokens-truncated empty output`.

## Logging an error in `errors_summary.md`

When you find a new error pattern, add a section with:

```markdown
## Bug-X — <one-line symptom>

**Date:** YYYY-MM-DD HH:MM:SS UTC
**Run:** `<run_id>` (model, concurrency, +T min into run)
**Sentry issue:** [VALKYRIE-XX](https://vals-ai.sentry.io/issues/VALKYRIE-XX) — <error class>

**Tasks hit (errored, with eventID):**
- `task-id-1` (eventID `abc123...`, HH:MM:SS UTC)
- `task-id-2` (eventID `def456...`, HH:MM:SS UTC)

**Site:** raised at `path/to/file.py:LINE` in `<func_name>`.

**Disposition:** retriable | chronic | needs-fix

**Recommendation:** <fix>
```

The user later wants to find CloudWatch logs for these — *always include the run_id and timestamps* so the deep-link in the run-status email is reproducible.
