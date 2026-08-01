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

- Parse stdout as JSON Lines. Most successful invocations write exactly one
  document; connected `start`, `fetch`, `resume`, and `retry`, `run analyze`, and
  `run start --count` above 1 write several.
- Every record has `schema_version: 1` and a `kind`. An `event` appears on every
  snapshot and analysis record, on a `run start` launch record, and on a connected
  `run_action`. Never read a missing `event` as a stream terminator: a snapshot
  stream closes with `complete`, `error`, `stopped`, `disconnect`, or `interrupted`,
  while `run start` closes with its event-less terminal record.
- Progress, prompts, warnings, and CLI errors use stderr.
- New receipts contain explicit allowlisted fields rather than raw configuration
  or response models. Secret mappings, credentials, header values, and presigned
  URLs are omitted. Existing fetch fields retain their current semantics and may
  contain service URLs. Free-form diagnostic strings can contain sensitive upstream
  text; the terminal `error` document and `run analyze`'s `error` event scrub a
  quoted URL that carries credentials, a query, or a fragment to `<redacted-url>`,
  but treat any other free-form field as untrusted.
- Exit status remains authoritative. A nonzero command may first emit a useful
  partial receipt or valid JSONL prefix, while a zero exit does not mean that a
  benchmark run itself succeeded.

## Failures

A failed invocation ends with one terminal document:

```json
{"command":"run fetch","error_message":"...","kind":"error","schema_version":1}
```

`command` names the leaf as invoked, so the `run retry` alias reports itself.
`error_message` is free-form upstream text: report it, do not branch on it.

A receipt emitted before the failure is kept, so `run status` with missing IDs
writes its `run_status` document and then the `error` document. Two failures have
no `error` document: rejected usage exits `2` with empty stdout because validation
always precedes the first tracker request, and `run analyze` reporting `unavailable`
is already a complete terminal receipt.

## Overwrite decisions

`run results` prompts on stderr before replacing an existing local file or S3
result, and distinguishes three outcomes:

| `status` | Meaning | Exit |
| --- | --- | --- |
| `completed` | The target was written. | 0 |
| `cancelled` | The operator declined; nothing was written. | 0 |
| `blocked` | No answer was obtainable, so nothing was written. | nonzero |

A `blocked` receipt carries `reason: "target_exists"` and is followed by an
`error` document naming `--force`, rather than a bare abort. Pass `--force` to
overwrite without prompting. Neither a cancelled nor a blocked receipt is fresh
retrieval evidence.

## Starting runs

`run start --json` reports requested, attempted, and confirmed counts. Whenever
more than one run is requested or `--connect` is used, each confirmed run is
published immediately as a `launch` event carrying only that run, so a later
failure cannot strand a run ID stdout never showed. `confirmed_count` stays
cumulative, and the terminal document carries no `event` and lists every
confirmed run.

`outcome` describes the launch operation only, never benchmark progress. It is
`in_progress` while runs remain to request, then `completed`, `partial`,
`uncertain`, or `failed` — so a connected launch, which requests one run, reports
`completed` immediately. A failed later request marks the overall outcome
`partial`; an ambiguous first request reports `uncertain`. Reconcile an uncertain
launch with `run list --json --all` before retrying.

## Streams

Connected start/resume/retry emit an action or launch receipt before
`run_snapshot` events; connected fetch begins with a snapshot. A connected start
whose stream ends cleanly then writes its terminal `run_start` document; a stream
that fails ends at the `error` document instead. Treat `disconnect` and
`interrupted` as client-stream termination, not benchmark completion.

`run analyze` emits nonterminal `started` and `heartbeat` events. On `complete`,
`reading_plan_url_status` says whether the URL is `present`, `absent` because the
analyzer returned none, or `withheld` because the source URL was not already
credential-free HTTPS. `reading_plan_url` is null unless the status is `present`.
A withheld URL is not recoverable through this contract: `run fetch`'s
`docent_reading_url` is an existing field with existing semantics and returns the
same source URL unfiltered, so treat it exactly like the other fetch fields that
"may contain service URLs" above.

## Compatibility

`--json` selects the existing JSON format for commands that already expose
`--format`; with `--connect`, it selects JSONL. Do not combine the two spellings.

Both `run list --json` and `run list --format json` require `--all`.
