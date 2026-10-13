"""Sandbox provider mappings and run settings stored in Tracker."""

from valkyrie.sdk.models._base import ResponseModel


class ProvidersResponse(ResponseModel):
    """The organization's secret name for each sandbox provider; values are never returned."""

    providers: dict[str, str]


class SavedBenchmarkService(ResponseModel):
    """A benchmark service URL saved for your runs, and the secret Tracker sends to it as a header."""

    url: str
    auth_header_name: str | None
    auth_secret_name: str | None


class RunSettingsResponse(ResponseModel):
    """Your default sandbox provider and saved benchmark services; empty for access keys without a user."""

    default_provider: str | None
    benchmark_services: dict[str, SavedBenchmarkService]
