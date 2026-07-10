# Managed runtime rollout

Managed runtime is activated in two deployments because protected workers can outlive an ECS rollout.

1. Deploy the model-gateway capability and its `valkyrie_signing_key` secret field.
2. Deploy the worker/wire readiness change. The API still rejects task-role starts.
3. Wait until every pre-readiness worker has drained or been replaced.
4. Deploy the API activation change.

Do not activate managed starts while a pre-readiness worker can consume the queue. After activation, never roll back below the worker/wire readiness release.
