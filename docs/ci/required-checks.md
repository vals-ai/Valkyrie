# Required PR checks

Valkyrie's `dev` and `prod` branch rulesets require exactly two GitHub Actions contexts
(integration id `15368`):

- **`required-ci`** — the single always-emitted PR-validation gate.
- **`maintenance-classification`** — the trusted deployment-policy gate.

No other workflow may emit either context, and neither is produced by a `push` workflow, so
the latest PR run is always the authoritative result.

## `required-ci`

`required-ci` runs on `pull_request` (targeting `dev` or `prod`) only. A relevance selector
(`.github/scripts/required_ci_select.py`) computes, from the exact
`pull_request.base.sha`..`pull_request.head.sha` diff, which subsystem validations the
revision requires. Each leaf job runs only when selected. The `required-ci` aggregate job
(`if: always()`, `.github/scripts/required_ci_aggregate.py`) inspects every leaf through
`needs` and fails when:

- the relevance selector failed or was cancelled, or
- a required leaf failed, was cancelled, hit a setup failure, or was unexpectedly skipped
  (e.g. a downstream matrix job skipped because its prerequisite failed), or
- a non-required leaf ended in failure/cancellation.

A skipped leaf passes only when the selector explicitly marked it not required.

### Selected validation per area

| Area | Trigger paths | Leaf validation |
| --- | --- | --- |
| core / CLI | `src/valkyrie/**`, root `pyproject.toml`/`uv.lock`, `Makefile`, `tests/**` | ruff, basedpyright, cli-tests, cli-tool-smoke-test |
| tracker | `services/tracker/**` | tracker unit tests + tracker live tests (policy below) |
| executor | executor/tracker/infra executor paths | executor build |
| infra | `infra/**`, deploy/executor/infra workflows | infra checks |
| sdk | SDK path list | sdk package + all compatibility versions (3.13, 3.14) |
| lockfile (root / tracker / infra) | each scope's `uv.lock`/`pyproject.toml` | validate-lockfile (that scope) |
| cbs | prod PRs only | validate-cbs-latest-tag |

If the SDK `package` prerequisite fails, `compatibility` is skipped and the aggregate fails
(a skipped-but-required leaf), so a failed prerequisite never becomes a passing gate.

### Self-modification protection

Changing `required-ci.yaml`, the selector, the aggregate, or `required-contexts.json` forces
full validation here **and** is independently classified `maintenance-required` by the trusted
`maintenance-classification` gate (defined on the base branch, never executing candidate
code). Because both rulesets require both contexts, a candidate cannot weaken its own
`required-ci` gate and self-satisfy the ruleset: even a neutered `required-ci` still faces
`maintenance-classification`, which blocks ordinary merging and requires an authorized force
merge.

## Tracker live tests

`.github/workflows/tracker-live-tests.yaml` has two modes, with no attempt-number-based
intentional failure:

- **Required live validation** (`workflow_call` from `required-ci`) for policy-selected,
  same-repository tracker changes. Runs in the protected `tracker-live-tests` GitHub
  Environment (reviewer approval) with the least-privileged
  `github-actions-valkyrie-tests` OIDC role.
- **Manual diagnostics** (`workflow_dispatch`) as a distinct, non-required context.

### Fork policy

`required-ci` runs on `pull_request`, so fork PRs receive no secrets or OIDC credentials. A
fork PR that changes `services/tracker/**` is routed to the `tracker-live-fork-blocked` job,
which fails with an explicit message. To validate such a change, a maintainer reviews and
imports the candidate commit onto a same-repository branch and runs required validation on
that new PR revision. Candidate code is never executed through `pull_request_target` or any
privileged context.

## Reusable-workflow pinning

Reusable workflows that contribute to `required-ci`
(`vals-ai/.github/.github/workflows/validate-lockfile.yaml` and `validate-cbs-latest-tag.yaml`)
are pinned to immutable commit SHAs, recorded in `.github/required-contexts.json`.

## Merge queues

Out of scope. Merge queues are not enabled by these rulesets. If they are enabled later,
every required gate must support `merge_group` and evaluate the synthesized candidate's
base/head SHAs.

## Ruleset drift

`.github/required-contexts.json` is the source of truth for the required contexts.
`.github/workflows/required-context-drift.yaml` is a scheduled, read-only check that fails on
drift between the live rulesets and the manifest. Ruleset changes are always applied by a
human; CI never mutates rulesets.

## Rollout (coordinated, one ruleset at a time)

1. Land this workflow on `dev` (non-required); exercise representative PRs (docs-only, infra,
   tracker, sdk, executor, workflow-policy, safe-maintenance, maintenance-required) plus
   deliberate-failure fixtures, verifying emitted names and SHA association via the
   check-runs API.
2. Dev ruleset (`14301922`): in one update, add `required-ci`, remove the obsolete leaf /
   matrix / reusable-child contexts, keep `maintenance-classification`.
3. Promote to `prod`; repeat representative PRs.
4. Prod ruleset (`12028382`): in one update, require `required-ci` and add
   `maintenance-classification`; remove leaf contexts.
5. Retire superseded PR triggers from the leaf workflows (keep their `push` variants under
   distinct non-required job names).

Pre-change ruleset JSON and rollback payloads are captured before each ruleset edit.
