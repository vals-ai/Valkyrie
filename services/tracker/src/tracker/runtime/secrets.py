"""Provider-neutral secret reads and reference resolution."""

from collections.abc import Collection, Mapping
from typing import Protocol, TypeAlias, TypeVar

from tracker.exceptions import SecretsError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SecretValue: TypeAlias = JsonValue
T = TypeVar("T")


# Credentials that let a sandbox bypass Model Gateway and call a model provider
# directly. This is deliberately an allowlist so credentials for model-adjacent
# tools such as Tavily are not removed.
DIRECT_PROVIDER_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "AI21LABS_API_KEY",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_API_KEY_2",
        "ANT_API_KEY",
        "ARCEE_API_KEY",
        "AZURE_API_KEY",
        "BASETEN_API_KEY",
        "COHERE_API_KEY",
        "DASHSCOPE_API_KEY",
        "DASHSCOPE_CN_API_KEY",
        "DEEPSEEK_API_KEY",
        "DEEPSEEK_KEY",
        "FIREWORKS_API_KEY",
        "GCP_CREDS",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GOOGLE_GENERATIVE_AI_API_KEY",
        "KIMI_API_KEY",
        "KIMI_PUBLIC_API_KEY",
        "MERCURY_API_KEY",
        "MERCURY_KEY",
        "META_API_KEY",
        "MINIMAX_API_KEY",
        "MISTRAL_API_KEY",
        "NVIDIA_API_KEY",
        "OPENAI_API_KEY",
        "OPENROUTER_API_KEY",
        "PERPLEXITY_API_KEY",
        "PERPLEXITY_KEY",
        "POOLSIDE_API_KEY",
        "THOMSONREUTERS_API_KEY",
        "TINKER_API_KEY",
        "TOGETHER_API_KEY",
        "VERCEL_API_KEY",
        "XAI_API_KEY",
        "XIAOMI_API_KEY",
        "ZAI_API_KEY",
    }
)

_MODEL_GATEWAY_CLIENT_ENV_NAMES = frozenset({"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"})


def gateway_routing_enabled(secret_names: Collection[str], kwargs: Mapping[str, str]) -> bool:
    """Return whether a run has selected a complete Model Gateway route."""
    return kwargs.get("no_model_gateway") != "True" and _MODEL_GATEWAY_CLIENT_ENV_NAMES <= set(secret_names)


def without_direct_provider_credentials(values: Mapping[str, T]) -> dict[str, T]:
    """Copy an environment-like mapping without direct model credentials."""
    return {name: value for name, value in values.items() if name not in DIRECT_PROVIDER_CREDENTIAL_ENV_NAMES}


class SecretStore(Protocol):
    """Synchronous access to named secret values."""

    def get(self, name: str) -> SecretValue:
        """Return decoded JSON, or the raw string when the value is not JSON."""
        raise NotImplementedError


def resolve_secrets(secrets: dict[str, str], secret_store: SecretStore) -> dict[str, str]:
    """Resolve environment-variable secret references to their current values."""
    if not secrets:
        return {}

    resolved: dict[str, str] = {}
    for env_name, secret_name in secrets.items():
        secret_value = secret_store.get(secret_name)
        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_name}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = str(secret_value)
    return resolved
