from unittest.mock import MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws import cloudwatch_logs
from tracker.aws.cloudwatch_logs import (
    _sanitize_log_stream_name,
    get_benchmark_log_url,
    handle_cloudwatch_error,
    write_benchmark_log_event,
)
from tracker.aws.s3 import handle_s3_error, s3_client
from tracker.exceptions import CloudWatchError, S3Error
from tracker.types import AWSCredentials

_AWS = AWSCredentials(
    aws_access_key_id="test-key",
    aws_secret_access_key="test-secret",
    aws_default_region="us-east-1",
)


class TestS3DecoratorClient:
    async def test_s3_error_with_client(self):
        """Test that ClientError is caught buy the decorator"""

        client_error = ClientError({"Error": {"Code": "500", "Message": "Error"}}, "GetObject")

        @handle_s3_error(message="Failed to get object")
        async def failing_function():
            raise client_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to get object" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    async def test_s3_error_with_botocore(self):
        """Test that BotoCoreError is caught by the decorator"""
        botocore_error = BotoCoreError()

        @handle_s3_error(message="Failed to connect to S3")
        async def failing_function():
            raise botocore_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to connect to S3" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


class TestS3ClientRetry:
    async def test_uses_standard_retry_mode(self):
        async with s3_client(_AWS) as client:
            assert client.meta.config.retries["mode"] == "standard"


class TestCloudWatchClient:
    def test_cloudwatch_error_with_client(self):
        """Test that ClientError is caught buy the decorator"""
        client_error = ClientError({"Error": {"Code": "404", "Message": "Not found"}}, "CreateLogStream")

        @handle_cloudwatch_error(message="Failed to create log stream")
        def failing_function():
            raise client_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to create log stream" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    def test_cloudwatch_error_with_botocore(self):
        """Test that BotoCoreError is caught by the decorator"""
        botocore_error = BotoCoreError()

        @handle_cloudwatch_error(message="Failed to connect to CloudWatch")
        def failing_function():
            raise botocore_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to connect to CloudWatch" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


class TestSanitizeLogStreamName:
    """logStreamName must satisfy AWS constraint [^:*]* (no ':' or '*')."""

    def test_replaces_colon(self):
        assert _sanitize_log_stream_name("provider/model:fast") == "provider/model_fast"

    def test_replaces_asterisk(self):
        assert _sanitize_log_stream_name("task*glob") == "task_glob"

    def test_replaces_both_and_multiple(self):
        assert _sanitize_log_stream_name("a:b:c*d") == "a_b_c_d"

    def test_preserves_clean_name(self):
        # plain ids and the allowed '/' are left intact
        assert _sanitize_log_stream_name("water_intake_tracker") == "water_intake_tracker"
        assert _sanitize_log_stream_name("group/sub/name") == "group/sub/name"

    def test_result_matches_aws_constraint(self):
        import re

        for raw in ["openai/gpt-5.5", "laguna-xs.2:fast", "x*:y", "plain_id"]:
            assert re.fullmatch(r"[^:*]*", _sanitize_log_stream_name(raw))


class TestGetBenchmarkLogUrl:
    def test_sanitizes_task_id_in_url(self):
        url = get_benchmark_log_url("bench123", "us-east-1", "/valkyrie/worker", task_id="provider/model:fast")
        # task id is sanitized before being url-quoted into the log-events path
        assert "model_fast" in url
        assert "model:fast" not in url

    def test_no_task_id_omits_log_events(self):
        url = get_benchmark_log_url("bench123", "us-east-1", "/valkyrie/worker")
        assert "log-events" not in url


class TestWriteBenchmarkLogEvent:
    def _mock_client(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        client = MagicMock()
        monkeypatch.setattr(cloudwatch_logs, "_cloudwatch_client", lambda aws: client)
        monkeypatch.setattr(cloudwatch_logs, "_created_streams", set())
        return client

    def test_creates_stream_and_puts_event_with_sanitized_name(self, monkeypatch: pytest.MonkeyPatch):
        client = self._mock_client(monkeypatch)

        # stream_key splits on the first ':' -> task_id keeps its own ':'
        write_benchmark_log_event("bench123:provider/model:fast", "hello", _AWS, "/valkyrie/worker")

        client.create_log_stream.assert_called_once_with(
            logGroupName="/valkyrie/worker/bench123", logStreamName="provider/model_fast"
        )
        assert client.put_log_events.call_args.kwargs["logStreamName"] == "provider/model_fast"

    def test_create_stream_botocore_error_reports_sanitized_name(self, monkeypatch: pytest.MonkeyPatch):
        client = self._mock_client(monkeypatch)
        client.create_log_stream.side_effect = BotoCoreError()

        with pytest.raises(CloudWatchError) as exc_info:
            write_benchmark_log_event("bench123:provider/model:fast", "hello", _AWS, "/valkyrie/worker")

        assert "provider/model_fast" in str(exc_info.value)
        client.put_log_events.assert_not_called()
