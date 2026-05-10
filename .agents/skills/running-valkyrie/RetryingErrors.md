# RetryingErrors

How `valk run retry` works, when to use it, and the gotcha that bit us hard (Bug-J).

## Two flavors of retry

```bash
# 1. Resume STOPPED tasks (no error reset). Picks up from where the run was halted.
valk run retry <run_id> --concurrency 40

# 2. Reset ERROR tasks → PENDING then resume. Use this end-of-pass to clear retriables.
valk run retry <run_id> --retry --concurrency 40
```

The `--retry` flag is the difference. Without it, only `STOPPED` tasks are restarted; `ERROR` tasks stay errored. With it, both `STOPPED` and `ERROR` are reset to `PENDING`.

`--concurrency` on retry overrides the original. Use this to dial down if the run is hammering a rate-limited provider.

## End-of-pass workflow

After a full run wraps:

```bash
# 1. Confirm Finished
valk run fetch <run_id>
# → 1229 finished │ 22 errors │ status: Finished

# 2. Reset errors and re-run them
valk run retry <run_id> --retry --concurrency 40

# 3. Wait ~10–15 min for retries to wrap (depends on error count)
valk run fetch <run_id>
# → 1247 finished │ 4 errors │ status: Finished

# 4. If errors dropped, retry again. Repeat until count plateaus
valk run retry <run_id> --retry --concurrency 40
# → 1248 finished │ 3 errors │ status: Finished

# 5. If retry pass #N produces same error count as #N-1 → those are chronic. Stop.
```

We typically see 22 → 4 → 3 → 3 (plateau) on gpt-5.4-mini. The plateau means Bug-L (judge `IndexError`); see `ErrorClassification.md`.

## Retry semantics under the hood

`/retry-or-resume-benchmark/{benchmark_id}` in `services/tracker/src/tracker/main.py`:

1. Pulls all tasks where status `IN ('STOPPED', 'ERROR')` (and `'ERROR'` only if `--retry` passed).
2. Calls `reset_to_in_progress_status(task_rows)` (`tracker/utils.py:1270-1272`):
   - Updates DB: `status='PENDING', error=NULL, attempts=0`
   - Calls `benchmark_service.verify_task_ids(task_ids=...)` to confirm task IDs are valid (this is where Bug-J lives — see below).
3. Re-dispatches PENDING tasks to workers.

## Bug-J — `URL component 'query' too long` on retry

**Symptom:**

```
$ valk run retry <run_id> --retry --concurrency 40
✗ HTTP 400: URL component 'query' too long
```

**Root cause:**

`benchmark_service.verify_task_ids()` calls `GET /verify-task-ids?task_ids=A&task_ids=B&...` with one query param per task ID (`benchmark_service/client.py:175-199`). With ~1100 stopped tasks × ~50 chars per task ID, the query string busts httpx's ~64 KB URL limit.

**Reproducer:**

Stop a run mid-flight that has ≥800 finished tasks (so ≥400 stopped). `valk run retry --retry` will fail.

**Workaround when you hit it:**

Until the underlying fix lands (one of):

1. Drop the `verify_task_ids` call in `reset_to_in_progress_status` — `task_rows` came from the tracker's own DB so the IDs are trivially valid (3-line patch).
2. Chunk `verify_task_ids` into batches of ~200 inside `reset_to_in_progress_status`.
3. Switch `/verify-task-ids` to POST + JSON body.

**Workaround we used:** restart the run from scratch with `valk run start` at concurrency 40. *Lost the ~150 finished tasks but it was the only path forward.* The `--task-ids` flag does *not* help — the SQL filter ORs `status IN ('STOPPED', 'ERROR')` with `task_ids IN (...)`, so all stopped tasks land in the result set regardless. Confirmed empirically with a 100-id batch.

## When to skip retry

- **Errors are all chronic Bug-L (judge `IndexError` on long deliverables).** Three retry passes won't change a thing — same task IDs every time. Accept them as failed and move on.
- **Errors are model behavior issues** (rate limit, content filter, the model just refuses). Retry won't fix it.
- **Errors are insufficient-funds** (Bug-G — Moonshot kimi suspended account). Refill the provider account, *then* retry.

## Retry doesn't preserve the original `-i` flags

If you launched with `-i 25 -i 50 -i 75`, the retry pass *won't* re-fire those notifications. `-i` is only honored by the original `start`. If you need a notification when the retry wraps, watch the run yourself.

## Retry doesn't preserve `--concurrency` either

Always pass `--concurrency` explicitly on retry. The default from the original run is *not* honored — without an explicit concurrency, you'll get the global default (5 last we checked).
