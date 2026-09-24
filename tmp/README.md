# Local deployment comparison

Run the same continuity probe against a commit before the executor changes and a revision after them. This folder is standalone test tooling, not application code.

## Run it

Use an ARM64 macOS or Linux machine with Docker running, `uv`, and an authenticated AWS profile. Both revisions must be available in the local Git repository. Dependency installation also needs access to their locked Git dependencies.

Set `COMPARISON_ENV_FILE` to the absolute path of your existing Tracker test env file. It must contain `TEST_AWS_S3_BUCKET`, `TEST_LOG_GROUP`, and `TEST_DAYTONA_SECRET_NAME`. `AWS_DEFAULT_REGION` and `TEST_LOG_RETENTION` are optional. The harness gets fresh credentials from the selected AWS profile; it does not use static credentials from that file.

From the repository root, run this command after setting that variable. Change `vals` to your profile name if needed:

```sh
uv run --python 3.12 --no-project tmp/compare.py --before a3d0ab17dbdbd164e214acd2d75466b9b5603844 --after working-tree --env-file "$COMPARISON_ENV_FILE" --aws-profile vals
```

This creates billable Daytona sandboxes and temporary S3 objects and CloudWatch log groups under unique test names. It never deploys ECS or contacts a deployed Tracker. Each case cleans up its remote resources, local processes, and Docker containers.

Replace `working-tree` with a commit or branch to compare two committed revisions. Use `--repo /absolute/path/to/repo` when keeping this folder outside the checkout under test. The supported baseline is the protocol-3 executor at the commit above; the changed implementation uses protocol 4. Missing support in an older revision is a setup error, not a reproduced interruption.

`working-tree` snapshots tracked files, including their local edits and deletions. Untracked files are excluded. Stage new source files first if they must participate. The harness never checks out a branch or changes the source repository.

## What it checks

Each revision gets its own source snapshot, lockfile-matched environment, built executor, PostgreSQL database, Redis, Tracker, and benchmark service. A frozen copy of this folder supplies the identical workload and assertions to both revisions.

The agent waits for the probe to release it. The probe records its sandbox, process identity, task ID, and attempt timestamp before replacing services. A second run must finish on the replacement host. The original attempt must then finish exactly once with the expected uploaded answer and score.

| Case | Controlled change | Expected before | Expected after |
| --- | --- | --- | --- |
| `replacement` | Host image changes | Interrupted | Original attempt survives |
| `maintenance` | Host task role changes | Interrupted | Interrupted; negative control |

The harness passes controlled task-definition templates to each revision's **own template classifier**. The inputs advertise draining only when that revision's executor stack does. The classifier selects the real maintenance functions or the real draining receiver; the test does not choose a green path based on whether a revision is labeled `after`.

These are local replacement tests. The templates isolate the change under test; they are not synthesized CloudFormation stacks. The harness does not execute GitHub Actions, ECS task protection, IAM changes, or database migrations. The maintenance control intentionally demonstrates that unsafe changes still stop runs. A passing comparison does not mean every deployment is downtime-free.

## Read the result

The command prints its evidence directory under `tmp/runs/`. `comparison.json` records outcomes, source and harness digests, revision IDs, classifier decisions, run IDs, and before/after task state. Each case also saves service logs. Treat those logs as private; do not publish the whole output folder.

Exit codes:

- `0`: the interruption reproduced before, replacement survived after, and any selected negative controls behaved as expected.
- `1`: the changed revision failed its expected behavior.
- `2`: setup, probe, or cleanup failed; no continuity conclusion is established.
- `3`: the old revision did not reproduce the interruption.

Start with the failing case's `result.json`, then its local logs. Setup failures must be fixed before interpreting the comparison. A `cleanup_error` requires checking the reported test agent and run IDs for remaining remote resources.

After the command exits and cleanup succeeds, the entire `tmp/` folder can be deleted. Only local evidence, source copies, and test environments live here; no application file imports it. Tool download caches may remain in their normal locations.

## Harness checks

```sh
uv run --project services/tracker pytest tmp/test_compare.py -q
```
