# Executor releases

Valkyrie keeps executor releases immutable. PostgreSQL owns the admission pointer,
each benchmark stores its initial release identity, and each executor dispatch
stores the release and artifact selected for that invocation.

## Lifecycle

1. Register a candidate with an `s3://` artifact, a SHA-256 digest, and protocol
   version `1`.
2. Verify readiness, then promote it. New benchmarks and retry/resume dispatches
   select the active release.
3. Tracker commits a queued dispatch record before broker enqueue. The stable
   ExecutorHost atomically claims it before launching the subprocess and marks it
   terminal after that subprocess exits.
4. The previous active release becomes `draining`. Existing processes keep their
   pinned artifact; the draining release receives no new admissions.
5. Retire a draining release only after it has no queued or running dispatches.
6. Roll back by promoting a verified previous release. Running dispatches and
   benchmark initial-admission metadata are not rewritten.

Inspect release health and ownership through the authenticated
`GET /executor-releases` Tracker endpoint. It reports the active admission
pointer, readiness metadata, active execution counts, and retirement blockers.
During the additive rollout, pre-ledger benchmarks remain covered by their
benchmark status until they finish.

## Operator commands

Run these commands from the Tracker environment with its database settings and
AWS credentials:

```bash
uv run python -m tracker.release_cli register RELEASE_ID s3://bucket/key SHA256_DIGEST
uv run python -m tracker.release_cli verify RELEASE_ID
uv run python -m tracker.release_cli promote RELEASE_ID
uv run python -m tracker.release_cli retire RELEASE_ID
```

`verify` streams the S3 object and checks its SHA-256 digest before promotion.

## Artifact retention

Retirement records a 30-day `artifact_retention_until` window. Artifact removal
is allowed only after that window expires and no queued or running dispatch
references the release. The current code exposes the deletion guard; it does not
run an automatic cleanup job.

An uncertain dispatch fails closed. A broker acknowledgement loss or ExecutorHost
crash can leave a nonterminal dispatch that blocks retirement until an operator
investigates it. There is deliberately no scheduler, heartbeat, reaper, or
automatic orphan repair in this release-safety layer.

Successive promotions are independent: A, B, and C may all drain concurrently,
and each retires when its own active execution count reaches zero. There is no
two-release limit.

Do not delete a release artifact manually while the status endpoint reports an
active-execution or retention blocker.

## Release-test

Use the isolated dev-sized stage before a normal release:

```bash
make plan STAGE=release-test SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

The stage uses namespaced resources and SSM outputs, exposes the tracker through
an internal ALB DNS name reachable from the VPC, and connects to
`benchmarks.vals.ai`. Local clients outside the VPC cannot call this endpoint
directly; use an approved VPC-local execution path for live HTTP smoke tests.

Deploy the backward-compatible ExecutorHost before Tracker starts emitting
dispatch IDs. Old queue messages omit that ID and continue through the existing
launch path; new messages use the PostgreSQL dispatch lifecycle without changing
the versioned executor artifact payload.
