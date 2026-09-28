# Changelog

## Unreleased

- Run each admitted executor dispatch in its own pinned ECS task; local development uses a runner subprocess.
- Seal dispatch inputs with a per-dispatch AES-256-GCM key wrapped by KMS and consume the payload transactionally at claim time.
- Remove the Redis queue and long-lived executor service. The unused Redis
  cluster, security group, and their CloudFormation exports, the Tracker service
  security group export, and the release-test executor-host ECR repository and
  exports remain for this one deployment so later stacks can drop their imports;
  remove them in the follow-up cleanup PR together with the host-removal
  classifier rule. The one-time cutover uses gated maintenance; later runner
  deployments do not stop active tasks.