"""Labels that identify Valkyrie-managed sandbox resources."""

from collections.abc import Mapping

MANAGED_BY_LABEL = "ManagedBy"
MANAGED_BY_VALKYRIE = "Valkyrie"
ENVIRONMENT_LABEL = "Environment"
CLEANUP_LABEL = "clean-up"
CLEANUP_ENABLED = "true"
CLEANUP_DISABLED = "false"


def valkyrie_sandbox_labels(environment: str) -> dict[str, str]:
    """Return ownership labels applied to every newly-created Valkyrie sandbox."""
    return {
        MANAGED_BY_LABEL: MANAGED_BY_VALKYRIE,
        ENVIRONMENT_LABEL: environment,
        CLEANUP_LABEL: CLEANUP_ENABLED,
    }


def is_valkyrie_managed(labels: Mapping[str, str], *, environment: str) -> bool:
    """Require explicit Valkyrie ownership in the expected deployment environment."""
    return labels.get(MANAGED_BY_LABEL) == MANAGED_BY_VALKYRIE and labels.get(ENVIRONMENT_LABEL) == environment


def cleanup_is_enabled(labels: Mapping[str, str]) -> bool:
    """Return whether a sandbox explicitly opts into scheduled cleanup."""
    value = labels.get(CLEANUP_LABEL)
    return value is not None and value.strip().casefold() == CLEANUP_ENABLED


def cleanup_is_disabled(labels: Mapping[str, str]) -> bool:
    """Return whether the issue #120 cleanup escape hatch is set."""
    value = labels.get(CLEANUP_LABEL)
    return value is not None and value.strip().casefold() == CLEANUP_DISABLED
