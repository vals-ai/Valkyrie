import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.cloudwatch import handle_cloudwatch_error
from tracker.exceptions import CloudWatchError, S3Error
from tracker.s3 import handle_s3_error


class TestS3DecoratorClient:
    def test_s3_error_with_client(self):
        """Test that ClientError is caught buy the decorator"""

        client_error = ClientError({"Error": {"Code": "500", "Message": "Error"}}, "GetObject")

        @handle_s3_error(message="Failed to get object")
        def failing_function():
            raise client_error

        with pytest.raises(S3Error) as exc_info:
            failing_function()

        assert "Failed to get object" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    def test_s3_error_with_botocore(self):
        """Test that BotoCoreError is caught by the decorator"""
        botocore_error = BotoCoreError()

        @handle_s3_error(message="Failed to connect to S3")
        def failing_function():
            raise botocore_error

        with pytest.raises(S3Error) as exc_info:
            failing_function()

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
