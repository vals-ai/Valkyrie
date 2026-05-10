# Secrets

How API keys make it from AWS Secrets Manager → the agent's runtime env.

## The flow

1. Caller invokes `valk run start ... -s ENV_VAR aws_secret_name`.
2. The CLI ships the `(env_var, secret_name)` pair into the run's `benchmark_arguments.contract.secrets` map.
3. The tracker fetches the secret value from AWS Secrets Manager (us-east-1) and exports it into the agent sandbox's environment as `ENV_VAR`.
4. Inside the sandbox, `harvey-legal-agent` boots, `model_library` reads `ENV_VAR`, the provider's `_get_default_api_key()` returns it, and the model client is happy.

If `ENV_VAR` isn't set, the provider's `_get_default_api_key()` returns empty / raises, and you get the classic `Sandbox error: ... exit code 1` with a traceback ending at `_get_default_api_key` or `_get_model_from_registry`. *That's a missing-secret signature, not a model bug.*

## Default secret bundle (always injected)

These are baked into the harness contract — no `-s` needed:

```
MODEL_PROXY_SSH_KEY  ← model_proxy_ssh
ANTHROPIC_API_KEY    ← localEvalInfraAnthropicKey
OPENAI_API_KEY       ← localEvalInfraOpenAIKey
```

That's why `openai/*` and `anthropic/*` models "just work" with no `-s` flag.

## Provider → env var → secret name cheat sheet

```
google/gemini-*               GOOGLE_API_KEY     localEvalInfraGoogleKey
minimax/MiniMax-*             MINIMAX_API_KEY    localEvalInfraMiniMaxKey
moonshot/kimi-*               MOONSHOT_API_KEY   localEvalInfraKimiKey
zai/glm-*                     ZAI_API_KEY        localEvalInfraZaiKey
alibaba/qwen-*                ALIBABA_API_KEY    localEvalInfraAlibabaKey
deepseek/*                    DEEPSEEK_API_KEY   localEvalInfraDeepSeekKey
xai/grok-*                    XAI_API_KEY        localEvalInfraXaiKey
mistral/*                     MISTRAL_API_KEY    localEvalInfraMistralKey
fireworks/*                   FIREWORKS_API_KEY  localEvalInfraFireworksKey
together/*                    TOGETHER_API_KEY   localEvalInfraTogetherApiKey
perplexity/*                  PERPLEXITY_API_KEY localEvalInfraPerplexityKey
cohere/*                      COHERE_API_KEY     localEvalInfraCohereKey
```

Many providers (`alibaba/qwen-*`, others) route through a Cloudflare proxy and *don't* need an explicit `-s` because they're keyless from the sandbox's POV. When in doubt: launch a `--slice :10` and read the traceback. If it points at `_get_default_api_key` you need an `-s`. If it points at `_get_model_from_registry` you usually need an `-s` *or* the model name is wrong.

## Discovering the actual secret name

```bash
aws secretsmanager list-secrets --region us-east-1 --max-items 200 \
  --query 'SecretList[?contains(Name,`localEvalInfra`)].Name' --output text \
  | tr '\t' '\n' | sort
```

Hits we've used: `localEvalInfraAnthropicKey`, `localEvalInfraOpenAIKey`, `localEvalInfraGoogleKey`, `localEvalInfraMiniMaxKey`, `localEvalInfraKimiKey`, `localEvalInfraZaiKey`, `localEvalInfraAlibabaKey`, `localEvalInfraDeepSeekKey`, `localEvalInfraXaiKey`.

## What the env var name actually has to be

Look at the provider class in `model_library/providers/<vendor>/*.py`, find `_get_default_api_key()`, follow it back to `model_library_settings.<NAME>`. That `<NAME>` is exactly what the env var must be called. Examples:

```bash
# google/google.py:149
return model_library_settings.GOOGLE_API_KEY

# delegates/minimax.py:32
return model_library_settings.MINIMAX_API_KEY
```

So `-s GOOGLE_API_KEY localEvalInfraGoogleKey` and `-s MINIMAX_API_KEY localEvalInfraMiniMaxKey`.

## Confirming after a run

The `harvey-legal-agent.json` on S3 records exactly which secrets the run was launched with:

```bash
aws s3 cp s3://agentic-harness/benchmarks/<run_id>/harvey-legal-agent.json /tmp/run.json
python3 -c "import json; print(json.dumps(json.load(open('/tmp/run.json'))['benchmark_arguments']['contract']['secrets'], indent=2))"
```

If you launched a 10-task subset and got 10/10 errors with `_get_default_api_key` in the trace, *check this output first* — confirms the `-s` flag was missing.

## Webhook secret (separate from model API keys)

The Slack webhook used by `-i` lives in `localEvalInfraValkyrieWebhook` (or whatever you've set as the webhook secret name). The tracker reads it directly via `valk run start --webhook-secret-name ValkyrieWebhook` (or the configured default). You don't pass it via `-s`.
