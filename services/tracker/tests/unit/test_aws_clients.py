import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.cloudwatch_logs import _sanitize_log_stream_name, handle_cloudwatch_error
from tracker.exceptions import CloudWatchError, S3Error
from tracker.aws.s3 import handle_s3_error


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
