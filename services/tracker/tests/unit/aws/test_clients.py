"""Unit tests for tracker AWS client boundaries.

Run: uv run pytest tests/unit/aws/test_clients.py
"""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws import cloudwatch_logs
from tracker.aws.cloudwatch_logs import (
    get_benchmark_log_url,
    handle_cloudwatch_error,
    write_benchmark_log_event,
)
from tracker.aws.s3 import handle_s3_error, s3_client
from tracker.exceptions import CloudWatchError, S3Error
from tracker.types import AWSCredentials

_sanitize_log_stream_name = getattr(cloudwatch_logs, "_sanitize_log_stream_name")


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

    async def test_uses_standard_retry_mode(self, aws_credentials: AWSCredentials) -> None:
        async with s3_client(aws_credentials) as client:
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
        import re

        for raw in ["openai/gpt-5.5", "laguna-xs.2:fast", "x*:y", "plain_id"]:
            assert re.fullmatch(r"[^:*]*", _sanitize_log_stream_name(raw))


class TestGetBenchmarkLogUrl:
    """CloudWatch benchmark log URL construction."""

    def test_sanitizes_task_id_in_url(self) -> None:
        url = get_benchmark_log_url("bench123", "us-east-1", "/valkyrie/worker", task_id="provider/model:fast")
        # task id is sanitized before being url-quoted into the log-events path
        assert "model_fast" in url
        assert "model:fast" not in url

    def test_no_task_id_omits_log_events(self) -> None:
        url = get_benchmark_log_url("bench123", "us-east-1", "/valkyrie/worker")
        assert "log-events" not in url


class TestWriteBenchmarkLogEvent:
    """CloudWatch stream creation and benchmark log writes."""

    def _mock_client(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        client = MagicMock()

        def create_client(_aws: AWSCredentials) -> MagicMock:
            return client

        created_streams: set[str] = set()
        monkeypatch.setattr(cloudwatch_logs, "_cloudwatch_client", create_client)
        monkeypatch.setattr(cloudwatch_logs, "_created_streams", created_streams)
        return client

    def test_creates_stream_and_puts_event_with_sanitized_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_credentials: AWSCredentials,
    ) -> None:
        client = self._mock_client(monkeypatch)

        # stream_key splits on the first ':' -> task_id keeps its own ':'
        write_benchmark_log_event("bench123:provider/model:fast", "hello", aws_credentials, "/valkyrie/worker")

        client.create_log_stream.assert_called_once_with(
            logGroupName="/valkyrie/worker/bench123", logStreamName="provider/model_fast"
        )
        assert client.put_log_events.call_args.kwargs["logStreamName"] == "provider/model_fast"

    def test_create_stream_botocore_error_reports_sanitized_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_credentials: AWSCredentials,
    ) -> None:
        client = self._mock_client(monkeypatch)
        client.create_log_stream.side_effect = BotoCoreError()

        with pytest.raises(CloudWatchError) as exc_info:
            write_benchmark_log_event(
                "bench123:provider/model:fast",
                "hello",
                aws_credentials,
                "/valkyrie/worker",
            )

        assert "provider/model_fast" in str(exc_info.value)
        client.put_log_events.assert_not_called()
