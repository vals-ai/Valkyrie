# Executor releases

Maintainer runbook for deploying, draining, recovering, and retiring Valkyrie executor releases.

Valkyrie keeps executor releases immutable. PostgreSQL owns the admission pointer,
each benchmark stores its initial release identity, and each executor dispatch
stores the release and artifact selected for that invocation.

## Lifecycle

1. Register a candidate with an `s3://` artifact, a SHA-256 digest, and protocol
   version `1`.
2. Verify readiness, then promote it. New benchmarks and whole-run terminal
   recovery select the `ACTIVE` release.
3. Benchmark admission stores immutable initial ownership and mutable current
   execution ownership before Tracker creates the immutable queued dispatch.
   Tracker launches one runner task for that dispatch. The task atomically claims
   the dispatch and consumes its encrypted payload before starting the subprocess.
4. The previous active release becomes `DRAINING`. It receives no new benchmark
   starts or terminal restarts, but active executions that it owns continue using
   it.
5. Whole-run terminal recovery moves current execution ownership to the then-
   `ACTIVE` release. In-progress retry never changes that ownership.
6. Retirement remains blocked by queued or running dispatches, active current
   owners, or any active benchmark whose current owner is unknown.
7. Tracker automatically retires each `DRAINING` release after all of those
   blockers clear. Retired releases cannot become active again.

![Executor release lifecycle](diagrams/valkyrie-release-lifecycle.png)

## Execution-pinned recovery

Release routing changes only when an execution crosses a whole-run terminal
boundary. It does not change which tasks retry or resume selects.

![Release coexistence and execution ownership](diagrams/valkyrie-release-coexistence.png)

| Operation and benchmark state | Executor release |
| --- | --- |
| New benchmark start | The `ACTIVE` release |
| Original work while `IN_PROGRESS` | The benchmark's current execution release |
| Retry dispatch while `IN_PROGRESS` | The current execution release, including when it is `DRAINING` |
| Resume while `IN_PROGRESS` | Existing behavior is unchanged; it does not hand off release ownership |
| Partial task stop while the benchmark remains `IN_PROGRESS` | No handoff; the benchmark keeps its current execution release |
| Retry or resume while `STOPPING` | Rejected, as today |
| Retry or resume from `STOPPED`, `FINISHED`, or `ERROR` | The `ACTIVE` release, which becomes the benchmark's current execution release |

A benchmark keeps two distinct ownership facts:

- **Initial release:** immutable provenance for the benchmark's first admission.
- **Current execution release:** the release used by continuation retries while
  the benchmark is active. A whole-run terminal retry or resume replaces this
  pointer with the then-`ACTIVE` release.

Every executor dispatch keeps its own immutable release and artifact snapshot.
A release becoming `DRAINING` never rewrites queued or running dispatches.

![Dispatch ownership and pinned artifact flow](diagrams/valkyrie-dispatch-ownership.png)

Start admission atomically persists benchmark ownership, its queued `START`
dispatch, and its AES-256-GCM encrypted payload. Tracker launches one ECS runner
task per dispatch with only the dispatch ID in its command. The transaction sets
both immutable initial ownership and current execution ownership to the locked `ACTIVE` release and
snapshots that release into the dispatch. If A becomes `DRAINING` after the
transaction commits, the admitted benchmark and dispatch remain on A and block
its retirement until their active work becomes terminal.

### Deployment during an active run

Given release A running a benchmark at 40/100 when B is promoted:

1. The 40 running tasks remain on A.
2. Tasks 41-100 from the existing execution also remain on A.
3. A mid-run retry remains on A.
4. A new benchmark start uses B.
5. Promotion alone never migrates tasks from A to B.

### Whole-run stop and recovery

A non-forced whole-run Stop moves `PENDING`, `BUILDING`, and `EVALUATING`
tasks to `STOPPED`. When it changes whole-run work, the benchmark enters
`STOPPING`; already `IN_PROGRESS` tasks and the current dispatch remain active
until normal finalization makes the run terminal. Resume then runs selected work
on the `ACTIVE` release and establishes that release as the new current
execution release.

A forced whole-run Stop also marks `IN_PROGRESS` tasks `STOPPED` and tears down
remaining sandboxes. Once no runnable work remains, it makes the benchmark
`STOPPED` and revokes active dispatches under the benchmark lock. A task-scoped
Stop preserves active dispatches while runnable work remains. If a forced
task-scoped Stop exhausts runnable work, it performs the same terminal transition
so an immediate Resume follows terminal recovery.

For example, after A reaches a whole-run terminal state and recovery starts on
B, later mid-run retries stay on B even if C has been promoted. A later
whole-run terminal retry or resume may then establish C as the current execution
release.

### Draining and retirement

A `DRAINING` release accepts no new benchmark starts or terminal restarts. It may
accept continuation retries for an `IN_PROGRESS` benchmark whose current
execution release is already that release.

Retirement remains blocked while a release owns an active execution or has a
queued or running dispatch. Tracker checks draining releases immediately at
startup and once per minute, then automatically retires every blocker-free
release. Once the execution and its dispatches are terminal, a later retry or
resume uses the `ACTIVE` release instead of retaining the retired release.

If the required current execution release or an `ACTIVE` release is unavailable,
recovery fails explicitly. It never silently switches releases.

### Non-goals

This release-affinity change does not alter:

- task selection for retry or resume;
- scoring, result history, or run IDs;
- partial-task stop behavior;
- concurrency limits or sandbox queue scheduling;
- task-level release assignment or automatic migration during promotion.

### Benchmark release provenance

Benchmark responses expose these release fields:

| Field | Meaning |
| --- | --- |
| `executor_release_id` | Immutable release selected when the benchmark was first admitted. |
| `current_execution_release_id` | Release currently owning execution. It changes only after a whole-run terminal retry or resume. |
| `executor_artifact_digest` | Immutable digest of the initial release artifact; it may differ from the artifact used after a terminal handoff. |
| `executor_protocol_version` | Immutable protocol version of the initial release. |

Pre-migration benchmarks can return null for these fields. The metadata endpoint
also exposes the initial `executor_artifact_uri`. The immutable per-dispatch
snapshot is authoritative for the exact artifact used by each invocation.

An `IN_PROGRESS` benchmark without a current execution release cannot continue.
Any `IN_PROGRESS` or `STOPPING` benchmark without one blocks all release
retirement. After it becomes terminal, retry or resume may establish the
`ACTIVE` release as its current owner.

### Failure and forward-recovery policy

An invalid or missing persisted owner for in-progress recovery is a `409`
conflict. Terminal recovery without a valid `ACTIVE` release is a `503` service
availability failure. After bounded retries of a failed task launch, Tracker
keeps a dispatch that was already claimed, rejects one superseded by newer work,
or marks a still-unclaimed dispatch `FAILED`. That failure errors only eligible
task attempts selected for that launch, errors the benchmark only when no active
sibling remains, and returns a `503` with the benchmark and dispatch IDs so Retry
can continue the run.

Tracker keeps its normal ECS deployment circuit breaker; failed infrastructure
updates retain normal CloudFormation rollback. Running runner tasks keep their pinned
task-definition revisions when a new revision is deployed. Executor
activation runs only after that deployment succeeds. Once an executor release is
active, it is never rolled back or reactivated after draining; fix executor
failures by deploying a new release.

Migration `e9f0a1b2c3d4` is forward-only because dropping current ownership would
destroy required execution state. Normal rollback must never run `alembic
downgrade` across it. After this migration is applied, do not deploy a pre-
Package-R Tracker image: its migration history cannot resolve `e9f0a1b2c3d4` and
its runtime does not maintain current ownership. Fix Tracker failures forward;
database restoration is a separately approved disaster-recovery operation.

### One-time runner cutover

This PR follows the existing CI-gated maintenance path: close admission, stop
active runs, wait until all old executor tasks have stopped, deploy the stacks and
activate the release, then reopen admission. There is no transition window or
parallel dispatch lifecycle. The removal of the historical executor service
triggers maintenance for this cutover; subsequent runner revisions do not stop
running tasks. The physical `WorkerStack` still owns release control.

The unused Redis cluster and its security group remain in SharedStack for this
one deploy, along with their endpoint address, endpoint port, security group ID,
and cluster-reference CloudFormation exports. TrackerStack also retains the
Tracker service security group ID export while WorkerStack drops its old import.
Release-test additionally retains the unused executor-host ECR repository and
its ARN and name exports. Core stacks deploy before WorkerStack; delete these
resources and exports in the follow-up cleanup PR together with the host-removal
classifier rule, not during this cutover. Monitoring no longer has Redis widgets.

## Automated deployment

Core and executor deployment use separate jobs and one non-cancelling deployment
mutex per stage. Every `dev` or `prod` push may deploy the Shared, Tracker, and
Monitoring stacks, but a core-only change never builds an executor artifact,
updates the physical `WorkerStack`, or activates a release. Executor work runs
only when the trusted classifier reports an executor release, an `ExecutorStack`
change, or an incompatible migration. After
acquiring the mutex, an executor job compares its SHA with the current branch
head and exits before AWS credentials or mutations when it is stale.

`ExecutorStack` is the Python owner and `executor` is the deployment scope. Its
physical CloudFormation identity remains `WorkerStack` to update the deployed
stack and retained resources in place.

An executor release builds one ARM64/Python 3.12 PEX from the exact Tracker source
and the dedicated `services/executor_artifact/uv.lock`. The release launcher uses
`infra/executor_release/uv.lock`; ordinary Tracker and CDK lock changes therefore
do not schedule executor work. The artifact digest is part of the release ID and
S3 key, so different bytes cannot reuse an existing release identity. The
executor lane uploads with create-only semantics, then runs one sealed
release-control task. Its `activate` transaction creates or matches the immutable
release, verifies the S3 digest, promotes it, and confirms it is the active
admission target before committing. PostgreSQL serializes overlapping activations
on the singleton admission row before either task creates or matches the release.

Dev executor operations follow a successful core deployment unless an incompatible
migration must run inside maintenance. Bench executor operations use the existing
protected `prod` GitHub Environment; production uses `prod-external`. Both are
wired directly to their mutating jobs, like the `dev` Environment. Each mutating executor job starts only after its
same-revision core dependency succeeds. The AWS accounts must already contain the
account-owned GitHub OIDC provider used by the environment-bound release roles.

The `maintenance-classification` job runs the same classifier used by deployment.
New tables, explicitly nullable default-free columns, changes that make an existing
column nullable, and explicitly non-unique indexes are safe. ExecutorStack and
release-control changes require executor maintenance. Other migration operations
require database maintenance. Safe changes pass immediately. Maintenance changes
wait for a required reviewer on the secretless `maintenance-dev` or
`maintenance-prod` GitHub Environment; approval makes the required check pass so
the pull request uses the normal merge path. Rejection keeps the check failed, and
synthesis, artifact-validation, or classifier infrastructure errors cannot be
approved.

For an approved maintenance deployment, the sealed release control task closes
admission, marks active benchmarks and tasks `STOPPED`, fails queued or running
dispatches, sends `StopTask` to active executor tasks, and waits until every
executor task is `STOPPED` before updating the stacks. Tracker is stopped before
the stack update, and admission reopens only after the required updates and
executor activation succeed. A failure leaves the fence closed for retry of
the same commit. Unsafe database migrations continue to require maintenance.
Later `ExecutorStack` changes use the normal maintenance flow. Manual workflow
dispatch remains limited to credential validation and planning; deployments come
from branch pushes.

Start, Retry, Resume, and concurrency changes return `503` while the fence is
held. Nothing is replayed automatically. Alembic startup upgrades use one
PostgreSQL advisory lock so rolling Tracker tasks cannot race migrations.
Each dispatch gets one Fargate runner task using a pinned task-definition revision.
The runner claims its dispatch, decrypts the transactionally deleted payload, and
exits after its child process completes. Ordinary runner deployments register a
new revision without stopping tasks already running. AWS Fargate platform
retirement is a separate interruption, not a deployment drain.

Tracker retires blocker-free draining releases automatically; artifact deletion
remains separate.

## Lifecycle interfaces

There is no tenant release or maintenance HTTP endpoint and no manual release
lifecycle CLI. The sealed deployment task calls release and maintenance control
directly; GitHub can launch that task but cannot read database or tenant sandbox
credentials. New benchmark starts return `503` until executor activation commits
an `ACTIVE` admission target and any maintenance fence opens.

## Artifact retention

Retirement records a 30-day `artifact_retention_until` window. Artifact removal
is allowed only when the release is `RETIRED`, the window has expired, the
release has no active current owner or queued/running dispatch, and no
unattributed active benchmark exists. The current code exposes the deletion
guard; it does not run an automatic cleanup job.

An uncertain dispatch fails closed. A launch acknowledgement loss or runner
interruption may leave a nonterminal dispatch that blocks retirement until an
operator investigates it. Tracker returns and logs the immutable dispatch ID.

### Investigate or stop an authoritative runner

1. Query `executor_dispatch` by immutable dispatch ID, checking `status`,
   `ecs_task_arn`, claim deadline, lease, and pinned release identity. For an
   admitted dispatch, its `executor_dispatch_payload` row exists only while
   queued; the runner deletes it as part of its guarded claim transaction.
2. Use the recorded `ecs_task_arn` to inspect the task in the stage cluster.
   Its task-definition family is stage-specific `ExecutorRunner`; find correlated
   logs in the retained stage-specific `/valkyrie/executor-runner` log group.
   Correlate the dispatch ID and task ARN, never
   dump payload ciphertext, data keys, service headers, or credentials. A missing
   ARN after a launch timeout does not prove no task was launched; inspect ECS
   tasks and dispatch-correlated logs before taking action.
3. If `RUNNING` and hung but still authoritative, authorize interruption and
   call `aws ecs stop-task --cluster <stage-cluster> --task <ecs-task-arn>`.
   Confirm ECS reports `STOPPED`, then let lease expiry and normal reconciliation
   resolve the dispatch; do not launch a duplicate task or directly rewrite its
   status. For a `QUEUED` dispatch, do not replay without resolving whether
   the original launch was accepted.
4. Leave unresolved work fail-closed. Replay or terminalization requires a
   separately approved and audited operator action that resolves the execution
   outcome first.

The retirement reconciler changes release metadata only. It does not schedule,
replay, requeue, repair, or delete executor work or artifacts.

Successive promotions are independent: A, B, and C may all drain concurrently,
and each retires automatically when its own active execution count reaches zero.
There is no two-release limit.

## Release-test

The release-test stage is dev-sized and targets the account selected by
`DEV_ACCOUNT_ID`. The coexistence procedure below runs it in the bench account
by setting `DEV_ACCOUNT_ID` to `BENCH_ACCOUNT_ID`; the target guard still keeps
the release-test resources inside the explicitly selected account.

Release-test also publishes `/valkyrie/release-test/executor-release/launch-config`
and the same sealed activation task used by deployment. It reuses the existing
release-test bucket and creates no GitHub OIDC release role; an explicitly
authorized release-test operator may use it for live deployment proof.

The Package R driver is a static Fargate task definition, not a service. It has a
no-ingress security group, explicit VPC/database/DNS/HTTPS egress, retained
logs, named secret references, and separate execution, task, and operator roles.
The operator role can run only that task definition and pass only its two roles.
Public IP assignment is a launch-time requirement because the stage has public
subnets and no NAT gateway; it does not expose Tracker, whose ALB remains
internal.

Before running the release-test driver, set:

```bash
export BENCH_ACCOUNT_ID="<bench-account-id>"
export PRODUCTION_ACCOUNT_ID="<production-account-id>"
export RELEASE_TEST_DRIVER_SECRET_ARN="arn:aws:secretsmanager:us-east-1:${BENCH_ACCOUNT_ID}:secret:YOUR_DRIVER_SECRET-SUFFIX"
export RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN="arn:aws:secretsmanager:us-east-1:${BENCH_ACCOUNT_ID}:secret:SANDBOX_PROVIDER_SECRET-SUFFIX"
export RELEASE_TEST_OPERATOR_PRINCIPAL_ARN="arn:aws:iam::${BENCH_ACCOUNT_ID}:role/ROLE_NAME"
export RELEASE_TEST_IMAGE_TAG=package-r-RUN_ID
```

Package R staging is create-only. With credentials for the authorized operator
role, upload the executor artifact under its reserved prefix and require that the
key does not already exist:

```bash
export RELEASE_TEST_ARTIFACT_BUCKET="agentic-harness-release-test-${BENCH_ACCOUNT_ID}"
export PACKAGE_R_EXECUTOR_ARTIFACT=/path/to/executor.pex
aws s3api put-object \
  --bucket "$RELEASE_TEST_ARTIFACT_BUCKET" \
  --key "releases/package-r/$(basename "$PACKAGE_R_EXECUTOR_ARTIFACT")" \
  --body "$PACKAGE_R_EXECUTOR_ARTIFACT" \
  --if-none-match '*'
```

A repeated key fails rather than replacing immutable release bytes.

The principal must be an IAM role ARN, not an STS assumed-role session ARN. Both
secret references must be complete generated ARNs, including their suffixes; a
name or partial ARN is not valid. The sandbox-provider ARN identifies the secret
that the Driver task role may read. The driver secret must contain exactly
`tracker_api_key` and `benchmark_authorization`; ECS injects those values and the
database credentials from Secrets Manager. Never put secret values in task
command or environment overrides.

Release-test owns an immutable `valkyrie/release-test/tracker` ECR repository;
the unused executor-host image repository and its CloudFormation exports remain for this deployment so WorkerStack can drop its imports. Remove them in the follow-up cleanup PR with the host-removal classifier rule. This avoids mutating the
account-wide CDK bootstrap repository. Deploy Shared first when creating the
repository, build and push the ARM64 Tracker image with a new immutable tag,
then synthesize and deploy dependent stacks with that tag. The runner uses the
Tracker image. Dev, bench, and prod keep the existing CDK asset path.

Review all stacks and the driver separately before deployment. Release-test
forces authentication on, so synthesis also needs the Descope project ID and
the account-local management-key secret name:

```bash
export DESCOPE_PROJECT_ID="release-test-descope-project-id"
export DESCOPE_MANAGEMENT_KEY_SECRET_NAME="release-test-descope-management-key-secret"

make plan STAGE=release-test SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$BENCH_ACCOUNT_ID" PROFILE=admin
make plan STAGE=release-test SCOPE=driver AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$BENCH_ACCOUNT_ID" PROFILE=admin
```

The driver launch contract is published under
`/valkyrie/release-test/driver/`: task-definition ARN, security-group ID, log
group name, and operator-role ARN. Shared outputs already provide the cluster
and public subnet IDs. Launches must use those exact values,
`assignPublicIp=ENABLED`, and only reviewed non-secret command/manifest
overrides. Without one, the default command writes that requirement to stderr
and exits with status 64. Deploying the Driver stack publishes a new standalone
task-definition revision through the SSM launch-contract ARN; it does not restart
a task or service. Only future launches that resolve the new ARN receive the new
default.

The stage connects to `benchmarks.vals.ai`. Local clients outside the VPC cannot
call the internal Tracker directly; use the driver for HTTP and database proof.

Each admitted dispatch launches a standalone runner task with the dispatch ID
only in its command; the artifact identity comes from the PostgreSQL dispatch
row and execution inputs from the encrypted payload row. No queue drain or
service replacement occurs on subsequent runner task-definition revisions.
