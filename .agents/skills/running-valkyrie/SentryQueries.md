# SentryQueries

How to slice Sentry by `benchmark_id` / `task_id`, the dataset gotcha, and the URL templates.

> Source: `services/tracker/src/tracker/sentry.py` (`_before_send`) reads context vars from `tracker/logging/context.py` and writes them as Sentry tags. Plus explicit `sentry_sdk.set_tag` calls in `tracker/sandbox.py` and `tracker/utils.py`.

## Org / project

- Org slug: `vals-ai`
- Region URL: `https://us.sentry.io`
- Project slug: `valkyrie`

## The tag keys you'll actually use

| Tag | What it is |
|---|---|
| `benchmark_id` | The run UUID (a.k.a. "run id" in conversation) |
| `task_id` | Per-task identifier, e.g. `funds-asset-management_draft-lpa_scenario-03` |
| `request_id` | Correlates events from the same `/start-benchmark` HTTP request |
| `agent_name` / `benchmark_name` | High-level filters |
| `sandbox_id` / `sandbox_name` / `sandbox_state` | Sandbox-level context |
| `error_class` / `agent_exit_code` | Error classification |
| `daytona.op` / `daytona.sdk_version` | Daytona-side context |

> A plain UUID search (`<uuid>`) returns nothing. Sentry only indexes them as tag values. Always query by tag key.

## MCP recipes (using the `sentry` MCP server)

1. **List issue groups for a benchmark/run:**
   ```
   search_issues(
     organizationSlug="vals-ai",
     regionUrl="https://us.sentry.io",
     query="benchmark_id:<uuid>"
   )
   ```

2. **Read the error logs (often more useful than the exception view — includes the agent's tail output):**
   ```
   search_events(
     organizationSlug="vals-ai",
     regionUrl="https://us.sentry.io",
     dataset="logs",
     query="benchmark_id:<uuid> severity:error",
     fields=["timestamp", "message", "task_id", "sandbox_state"]
   )
   ```

3. **Per-benchmark slice within an issue (issues are grouped across many benchmarks; you almost always need this):**
   ```
   get_issue_tag_values(
     organizationSlug="vals-ai",
     regionUrl="https://us.sentry.io",
     issueId="VALKYRIE-XX",
     tagKey="benchmark_id"
   )
   ```

4. **Full issue detail / stacktrace / event tags:**
   ```
   get_sentry_resource(url="https://vals-ai.sentry.io/issues/VALKYRIE-XX")
   ```

5. **Events within one issue, scoped to a benchmark:**
   ```
   search_issue_events(
     organizationSlug="vals-ai",
     regionUrl="https://us.sentry.io",
     issueId="VALKYRIE-XX",
     query="benchmark_id:<uuid>"
   )
   ```

## Dataset gotcha

`search_events` with `dataset="errors"` *sometimes* silently auto-routes to `dataset="spans"` and returns trace rows instead of error events. If the result looks like trace spans (no `message`, no `severity`), retry with:

- `search_issues` + `search_issue_events`, or
- `dataset="logs"` + `severity:error` (this routes reliably)

Same idea for spans/traces: `dataset="spans"` query `benchmark_id:<uuid>` finds the trace id (`019d...`) for the `POST /start-benchmark` request → drill into `https://vals-ai.sentry.io/explore/traces/trace/<trace_id>` for the timeline.

## Useful URL templates

```
Issues:  https://vals-ai.sentry.io/issues/?query=benchmark_id%3A<uuid>
Logs:    https://vals-ai.sentry.io/explore/logs/?query=benchmark_id%3A<uuid>%20severity%3Aerror&statsPeriod=14d
Trace:   https://vals-ai.sentry.io/explore/traces/trace/<trace_id>
Issue:   https://vals-ai.sentry.io/issues/<issue_id>/
```

## Workflow when an error fires during a run

1. `valk run fetch <run_id>` — confirm the error count climbed.
2. `search_events dataset="logs" benchmark_id:<run_id> severity:error` — pull the full traceback.
3. If the traceback is unfamiliar → new bug class → log to `errors_summary.md` (`ErrorClassification.md`).
4. If the traceback matches an existing Bug-X letter → add the new task IDs + eventIDs under that bug section.
5. End-of-pass, decide: retry (`valk run retry <run_id> --retry`) or accept (chronic).

## When Sentry data isn't enough

Sentry doesn't capture the full agent stdout/stderr — only the tail (~10–50 lines). For the *full* output:

- S3: `s3://agentic-harness/benchmarks/<run_id>/<task_id>/agent_stdout.log`
- CloudWatch: `benchmarks/<run_id>` log group, log stream named after the task ID

CloudWatch is faster for live runs; S3 is the source of truth after the run finishes.
