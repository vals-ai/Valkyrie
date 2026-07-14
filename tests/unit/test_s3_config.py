import click
import pytest
from tracker.types import AWSCredentials

from valkyrie.cli import s3_config as cli_s3


def test_s3_client_forwards_explicit_session_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    credentials: list[AWSCredentials] = []
    client = object()

    monkeypatch.setattr(
        cli_s3,
        "load_config",
        lambda: {
            "AWS_ACCESS_KEY_ID": "aws-key",
            "AWS_SECRET_ACCESS_KEY": "aws-secret",
            "AWS_SESSION_TOKEN": "aws-token",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )

    def fake_s3_client(aws: AWSCredentials) -> object:
        credentials.append(aws)
        return client

    monkeypatch.setattr(cli_s3, "tracker_s3_client", fake_s3_client)

    assert cli_s3.s3_client() is client
    assert credentials == [
        AWSCredentials(
            aws_access_key_id="aws-key",
            aws_secret_access_key="aws-secret",
            aws_default_region="us-west-2",
            aws_session_token="aws-token",
        )
    ]


def test_s3_client_uses_sdk_credential_chain_without_configured_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    session_arguments: list[dict[str, object]] = []
    client = object()

    class FakeSession:
        def __init__(self, **kwargs: object) -> None:
            session_arguments.append(kwargs)

        def client(self, service_name: str) -> object:
            assert service_name == "s3"
            return client

    monkeypatch.setattr(cli_s3, "load_config", lambda: {"AWS_DEFAULT_REGION": "us-west-2"})
    monkeypatch.setattr(cli_s3.aioboto3, "Session", FakeSession)

    assert cli_s3.s3_client() is client
    assert session_arguments == [{"region_name": "us-west-2"}]


@pytest.mark.parametrize(
    ("config", "error_message"),
    [
        (
            {"AWS_ACCESS_KEY_ID": "aws-key", "AWS_DEFAULT_REGION": "us-west-2"},
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together",
        ),
        (
            {"AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_DEFAULT_REGION": "us-west-2"},
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together",
        ),
        (
            {"AWS_SESSION_TOKEN": "aws-token", "AWS_DEFAULT_REGION": "us-west-2"},
            "AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
        ),
    ],
)
def test_s3_client_rejects_partial_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch, config: dict[str, str], error_message: str
) -> None:
    monkeypatch.setattr(cli_s3, "load_config", lambda: config)

    with pytest.raises(click.ClickException, match=error_message):
        cli_s3.s3_client()
