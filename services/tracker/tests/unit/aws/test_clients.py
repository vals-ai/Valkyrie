"""Unit tests for tracker AWS client boundaries.

Run: uv run pytest tests/unit/aws/test_clients.py
"""

import re
from typing import cast
from unittest.mock import ANY, MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws import clients as aws_clients
from tracker.aws import cloudwatch_logs
from tracker.aws.clients import (
    AWSClientProvider,
    DefaultChainAWSClientProvider,
    ExplicitCredentialsAWSClientProvider,
)
from tracker.aws.cloudwatch_logs import (
    get_benchmark_log_url,
    handle_cloudwatch_error,
    write_benchmark_log_event,
)
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.s3 import handle_s3_error
from tracker.exceptions import CloudWatchError, S3Error
from tracker.types import AWSCredentials

_sanitize_log_stream_name = getattr(cloudwatch_logs, "_sanitize_log_stream_name")

_AWS = AWSCredentials(
    aws_access_key_id="test-key",
    aws_secret_access_key="test-secret",
    aws_default_region="us-east-1",
)

_AWS_RESOURCES = AWSResources(
    region="us-east-1",
    s3_bucket="test-bucket",
    log_group="/valkyrie/worker",
    log_retention_days=30,
)


class TestAWSClientProviders:
    """Credential selection and presigned URL lifetime behavior."""

    @pytest.mark.parametrize("session_token", [None, "test-session-token"])
    def test_explicit_provider_forwards_optional_session_token(
        self,
        monkeypatch: pytest.MonkeyPatch,
        session_token: str | None,
    ) -> None:
        session = MagicMock()
        session_factory = MagicMock(return_value=session)
        boto_client_factory = MagicMock()
        monkeypatch.setattr(aws_clients.aioboto3, "Session", session_factory)
        monkeypatch.setattr(aws_clients.boto3, "client", boto_client_factory)

        credentials = AWSCredentials(
            aws_access_key_id=f"test-key-{session_token or 'none'}",
            aws_secret_access_key="test-secret",
            aws_default_region="us-east-1",
            aws_session_token=session_token,
        )
        provider = ExplicitCredentialsAWSClientProvider(credentials)

        provider.s3_client()
        provider.cloudwatch_logs_client()
        provider.secretsmanager_client()
        provider.lambda_client()

        session_factory.assert_called_once_with(
            aws_access_key_id=credentials.aws_access_key_id,
            aws_secret_access_key=credentials.aws_secret_access_key,
            aws_session_token=session_token,
            region_name=credentials.aws_default_region,
        )
        session.client.assert_called_once_with("s3", config=ANY)
        assert {constructed.args[0] for constructed in boto_client_factory.call_args_list} == {
            "logs",
            "secretsmanager",
            "lambda",
        }
        for constructed in boto_client_factory.call_args_list:
            assert constructed.kwargs["aws_access_key_id"] == credentials.aws_access_key_id
            assert constructed.kwargs["aws_secret_access_key"] == credentials.aws_secret_access_key
            assert constructed.kwargs["aws_session_token"] == session_token
            assert constructed.kwargs["region_name"] == credentials.aws_default_region

    def test_default_chain_provider_omits_explicit_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session_factory = MagicMock(return_value=session)
        boto_client_factory = MagicMock()
        monkeypatch.setattr(aws_clients.aioboto3, "Session", session_factory)
        monkeypatch.setattr(aws_clients.boto3, "client", boto_client_factory)

        region = "test-default-chain-region"
        provider = DefaultChainAWSClientProvider(region=region)

        provider.s3_client()
        provider.cloudwatch_logs_client()
        provider.secretsmanager_client()
        provider.lambda_client()

        session_factory.assert_called_once_with(region_name=region)
        session.client.assert_called_once_with("s3", config=ANY)
        assert {constructed.args[0] for constructed in boto_client_factory.call_args_list} == {
            "logs",
            "secretsmanager",
            "lambda",
        }
        credential_arguments = {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}
        for constructed in boto_client_factory.call_args_list:
            assert constructed.kwargs["region_name"] == region
            assert credential_arguments.isdisjoint(constructed.kwargs)

    @pytest.mark.parametrize(
        ("provider", "requested_seconds", "expected_seconds"),
        [
            (ExplicitCredentialsAWSClientProvider(_AWS), 86_400, 86_400),
            (DefaultChainAWSClientProvider(region="us-east-1"), 86_400, 3_600),
            (DefaultChainAWSClientProvider(region="us-east-1"), 300, 300),
        ],
    )
    def test_maximum_presign_ttl(
        self,
        provider: AWSClientProvider,
        requested_seconds: int,
        expected_seconds: int,
    ) -> None:
        assert provider.maximum_presign_ttl(requested_seconds) == expected_seconds


class TestS3DecoratorClient:
    """S3 exception translation at the client boundary."""

    async def test_s3_error_with_client(self) -> None:
        """Test that ClientError is caught buy the decorator"""

        client_error = ClientError({"Error": {"Code": "500", "Message": "Error"}}, "GetObject")

        @handle_s3_error(message="Failed to get object")
        async def failing_function() -> None:
            raise client_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to get object" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    async def test_s3_error_with_botocore(self) -> None:
        """Test that BotoCoreError is caught by the decorator"""
        botocore_error = BotoCoreError()

        @handle_s3_error(message="Failed to connect to S3")
        async def failing_function() -> None:
            raise botocore_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to connect to S3" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


class TestS3ClientRetry:
    """S3 client retry configuration."""

    @pytest.mark.parametrize(
        "provider",
        [
            pytest.param(ExplicitCredentialsAWSClientProvider(_AWS), id="explicit"),
            pytest.param(DefaultChainAWSClientProvider("us-east-1"), id="default-chain"),
        ],
    )
    async def test_uses_standard_retry_mode(self, provider: AWSClientProvider) -> None:
        async with provider.s3_client() as client:
            assert client.meta.config.retries["mode"] == "standard"


class TestCloudWatchClient:
    """CloudWatch exception translation at the client boundary."""

    def test_cloudwatch_error_with_client(self) -> None:
        """Test that ClientError is caught buy the decorator"""
        client_error = ClientError({"Error": {"Code": "404", "Message": "Not found"}}, "CreateLogStream")

        @handle_cloudwatch_error(message="Failed to create log stream")
        def failing_function() -> None:
            raise client_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to create log stream" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    def test_cloudwatch_error_with_botocore(self) -> None:
        """Test that BotoCoreError is caught by the decorator"""
        botocore_error = BotoCoreError()

        @handle_cloudwatch_error(message="Failed to connect to CloudWatch")
        def failing_function() -> None:
            raise botocore_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to connect to CloudWatch" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


class TestSanitizeLogStreamName:
    """logStreamName must satisfy AWS constraint [^:*]* (no ':' or '*')."""

    def test_replaces_colon(self) -> None:
        assert _sanitize_log_stream_name("provider/model:fast") == "provider/model_fast"

    def test_replaces_asterisk(self) -> None:
        assert _sanitize_log_stream_name("task*glob") == "task_glob"

    def test_replaces_both_and_multiple(self) -> None:
        assert _sanitize_log_stream_name("a:b:c*d") == "a_b_c_d"

    def test_preserves_clean_name(self) -> None:
        # plain ids and the allowed '/' are left intact
        assert _sanitize_log_stream_name("water_intake_tracker") == "water_intake_tracker"
        assert _sanitize_log_stream_name("group/sub/name") == "group/sub/name"

    def test_result_matches_aws_constraint(self) -> None:
        for raw in ["openai/gpt-5.5", "laguna-xs.2:fast", "x*:y", "plain_id"]:
            assert re.fullmatch(r"[^:*]*", _sanitize_log_stream_name(raw))


class TestGetBenchmarkLogUrl:
    """CloudWatch benchmark log URL construction."""

    def test_sanitizes_task_id_in_url(self) -> None:
        url = get_benchmark_log_url("bench123", _AWS_RESOURCES, task_id="provider/model:fast")
        # task id is sanitized before being url-quoted into the log-events path
        assert "model_fast" in url
        assert "model:fast" not in url

    def test_no_task_id_omits_log_events(self) -> None:
        url = get_benchmark_log_url("bench123", _AWS_RESOURCES)
        assert "log-events" not in url


class TestWriteBenchmarkLogEvent:
    """CloudWatch stream creation and benchmark log writes."""

    def _runtime_with_mock_client(self, monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, AWSRuntime]:
        client = MagicMock()
        client_provider = MagicMock(spec=AWSClientProvider)
        client_provider.cloudwatch_logs_client.return_value = client
        monkeypatch.setattr(cloudwatch_logs, "_created_streams", set[str]())
        runtime = AWSRuntime(
            resources=_AWS_RESOURCES,
            clients=cast(AWSClientProvider, client_provider),
        )
        return client, runtime

    def test_creates_stream_and_puts_event_with_sanitized_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, runtime = self._runtime_with_mock_client(monkeypatch)

        # stream_key splits on the first ':' -> task_id keeps its own ':'
        write_benchmark_log_event("bench123:provider/model:fast", "hello", runtime)

        client.create_log_stream.assert_called_once_with(
            logGroupName="/valkyrie/worker/bench123", logStreamName="provider/model_fast"
        )
        assert client.put_log_events.call_args.kwargs["logStreamName"] == "provider/model_fast"

    def test_create_stream_botocore_error_reports_sanitized_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, runtime = self._runtime_with_mock_client(monkeypatch)
        client.create_log_stream.side_effect = BotoCoreError()

        with pytest.raises(CloudWatchError) as exc_info:
            write_benchmark_log_event("bench123:provider/model:fast", "hello", runtime)

        assert "provider/model_fast" in str(exc_info.value)
        client.put_log_events.assert_not_called()
