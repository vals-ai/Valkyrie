"""Tests for Tracker authentication and outbound header forwarding.

Run: uv run pytest tests/unit/test_auth.py
"""

import pytest
from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine

from tests.conftest import TEST_ORG_ID
from tracker.auth import forward_tracker_api_key
from tracker.database.models import DEFAULT_ORG_NAME, Org


@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def test_get_default_org_returns_vals_org(session: Session):
    from tracker.auth import get_default_org

    vals_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    session.add(vals_org)
    session.commit()

    result = get_default_org(session)
    assert result.id == TEST_ORG_ID
    assert result.name == DEFAULT_ORG_NAME


def test_get_default_org_raises_if_missing(session: Session):
    import tracker.auth as auth_module

    auth_module._cached_default_org = None  # clear cache from prior test

    with pytest.raises(RuntimeError, match="Default org not found"):
        auth_module.get_default_org(session)


def test_forward_tracker_api_key_preserves_explicit_override_case_insensitive():
    original_headers = {"x-descope-api-key": "override-key"}

    headers = forward_tracker_api_key(original_headers, "tracker-api-key")

    assert headers == original_headers
    assert headers is not original_headers


@pytest.mark.parametrize(
    "header_name",
    [
        "Host",
        "Connection",
        "Upgrade",
        "Transfer-Encoding",
        "Forwarded",
        "X-Forwarded-For",
        "Sec-WebSocket-Key",
        "X-Api-Key",
    ],
)
def test_forward_tracker_api_key_rejects_protocol_and_routing_headers(header_name: str) -> None:
    """Reject caller headers that can alter HTTP routing or protocol handling."""
    with pytest.raises(HTTPException) as exc_info:
        forward_tracker_api_key({header_name: "attacker-controlled"}, "tracker-api-key")

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "Unsupported benchmark service header"


@pytest.mark.parametrize(
    ("header_name", "header_value"),
    [
        (":authority", "attacker.example"),
        ("bad header", "attacker-controlled"),
        ("Authorization", "Bearer token\r\nHost: attacker.example"),
        ("Authorization", "Bearer token\x00suffix"),
        ("Authorization", "non-ascii-é"),
    ],
)
def test_forward_tracker_api_key_rejects_malformed_headers(header_name: str, header_value: str) -> None:
    """Reject headers that the outbound HTTP transport cannot encode or send."""
    with pytest.raises(HTTPException) as exc_info:
        forward_tracker_api_key({header_name: header_value}, "tracker-api-key")

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "Unsupported benchmark service header"
