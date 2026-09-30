# pyright: reportPrivateUsage=false

"""Tests for the tracker identity-provider seam.

Run: uv run pytest tests/unit/test_identity_provider.py
"""

from unittest.mock import MagicMock

import pytest
from descope.exceptions import AuthException

from tracker.identity_provider import (
    CredentialRejectedError,
    DescopeIdentityProvider,
    UserProfile,
    build_identity_provider,
)


class TestBuildIdentityProvider:
    def test_auth_disabled_returns_none(self) -> None:
        assert (
            build_identity_provider(
                auth_required=False, provider_name="descope", descope_project_id="", descope_management_key=""
            )
            is None
        )

    def test_auth_disabled_ignores_provider_config(self) -> None:
        assert (
            build_identity_provider(
                auth_required=False,
                provider_name="anything",
                descope_project_id="P_fake",
                descope_management_key="",
            )
            is None
        )

    def test_unknown_provider_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported IDENTITY_PROVIDER"):
            build_identity_provider(
                auth_required=True, provider_name="okta", descope_project_id="P_fake", descope_management_key=""
            )

    def test_descope_without_project_id_returns_none(self) -> None:
        """AUTH_REQUIRED without project credentials stays lazily failing, matching previous behavior."""
        assert (
            build_identity_provider(
                auth_required=True, provider_name="descope", descope_project_id="", descope_management_key=""
            )
            is None
        )

    def test_descope_provider_builds_with_project_id(self) -> None:
        provider = build_identity_provider(
            auth_required=True, provider_name="descope", descope_project_id="P_fake", descope_management_key=""
        )
        assert isinstance(provider, DescopeIdentityProvider)


class TestDescopeIdentityProvider:
    def _provider_with_mock_client(self) -> tuple[DescopeIdentityProvider, MagicMock]:
        provider = DescopeIdentityProvider(project_id="P_fake", management_key=None)
        mock_client = MagicMock()
        provider._client = mock_client
        return provider, mock_client

    def test_exchange_access_key_returns_claims(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        claims = {"tenants": {"t": {}}, "keyId": "K2abc"}
        mock_client.exchange_access_key.return_value = claims

        assert provider.exchange_access_key("key") is claims
        mock_client.exchange_access_key.assert_called_once_with("key")

    def test_exchange_access_key_maps_auth_exception(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        mock_client.exchange_access_key.side_effect = AuthException(status_code=401, error_message="bad key")

        with pytest.raises(CredentialRejectedError):
            provider.exchange_access_key("bad")

    def test_exchange_access_key_propagates_transport_errors(self) -> None:
        """Non-credential failures propagate so callers can apply retry/availability policy."""
        provider, mock_client = self._provider_with_mock_client()
        mock_client.exchange_access_key.side_effect = TimeoutError("timed out")

        with pytest.raises(TimeoutError):
            provider.exchange_access_key("key")

    def test_validate_session_returns_claims(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        claims = {"tenants": {"t": {}}}
        mock_client.validate_session.return_value = claims

        assert provider.validate_session("jwt") is claims

    def test_validate_session_maps_auth_exception(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        mock_client.validate_session.side_effect = AuthException(status_code=401, error_message="bad session")

        with pytest.raises(CredentialRejectedError):
            provider.validate_session("jwt")

    def test_load_user_profile_normalizes_fields(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        mock_client.mgmt.user.load_by_user_id.return_value = {
            "user": {"email": "Alice@Vals.AI", "displayName": "Alice Smith"},
        }

        profile = provider.load_user_profile("U2abc")

        assert profile == UserProfile(email="alice@vals.ai", name="Alice Smith")
        mock_client.mgmt.user.load_by_user_id.assert_called_once_with("U2abc")

    def test_load_user_profile_failure_returns_empty(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        mock_client.mgmt.user.load_by_user_id.side_effect = RuntimeError("mgmt unavailable")

        assert provider.load_user_profile("U2abc") == UserProfile(email=None, name=None)

    def test_load_user_profile_missing_user_returns_empty(self) -> None:
        provider, mock_client = self._provider_with_mock_client()
        mock_client.mgmt.user.load_by_user_id.return_value = {}

        assert provider.load_user_profile("U2abc") == UserProfile(email=None, name=None)
