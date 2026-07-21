# Machine-readable CLI output

The agent-facing run workflows below accept `--json`. Human-readable output
remains the default.

`start`, `fetch`, `status`, `list`, `errors`, `resume`, `retry`, `update`,
`analyze`, `results`, and `outputs`

```bash
valk run start --agent sweagent --benchmark swebench --json
valk run fetch <run-id> --connect --json
valk run list --status IN_PROGRESS --json --all
```

## Contract

- Successful finite invocations write exactly one compact JSON document to
  stdout. Connected `start`, `fetch`, `resume`, and `retry` commands write JSON
  Lines, as does `run analyze`.
- Every record has `schema_version: 1` and a `kind`; stream records also have an
  `event`.
- Progress, prompts, warnings, and CLI errors use stderr. Existing overwrite
  confirmation remains required, and cancellation produces a receipt.
- New receipts contain explicit allowlisted fields rather than raw configuration
  or response models. Secret mappings, credentials, header values, and presigned
  URLs are omitted. Existing fetch fields retain their current semantics and may
  contain service URLs. Free-form diagnostic strings can contain sensitive
  upstream text.
- Exit status remains authoritative. A nonzero command may first emit a useful
  partial receipt or valid JSONL prefix, while a zero exit does not mean that a
  benchmark run itself succeeded.

`run start --json` reports requested, attempted, and confirmed counts. A failed
later request preserves confirmed run IDs and marks the overall outcome
`partial`; an ambiguous first request reports `uncertain`. Reconcile an
uncertain launch with `run list --json --all` before retrying.

Connected start/resume/retry commands emit an action or launch receipt before
`run_snapshot` events; connected fetch begins with a snapshot. Treat
`disconnect` and `interrupted` as client-stream termination, not benchmark
completion. Analysis returns `reading_plan_url: null` when the source URL
contains credentials, a query, or a fragment; use a fresh fetch when access to
that service URL is required.

## Compatibility

`--json` selects the existing JSON format for commands that already expose
`--format`; with `--connect`, it selects JSONL. Do not combine the two spellings.

Both `run list --json` and `run list --format json` require `--all`.
