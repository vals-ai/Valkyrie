"""Unit tests for tracker Lambda invocation.

Run: uv run pytest tests/unit/aws/test_lambda.py
"""

import io
import json
from typing import cast
from unittest.mock import MagicMock

import pytest
from botocore.config import Config

from tracker._lambda import invoke_lambda
from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import LambdaError


def test_invoke_lambda_uses_provider_config_and_returns_parsed_payload() -> None:
    client = MagicMock()
    client.invoke.return_value = {
        "Payload": io.BytesIO(b'{"statusCode": 200, "reading_plan_url": "https://example.test/plan"}')
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client
    config = Config(read_timeout=905)
    payload = {"benchmark_id": "benchmark-1"}

    result = invoke_lambda(
        cast(AWSClientProvider, provider),
        "analyzer-function",
        payload,
        config=config,
    )

    provider.lambda_client.assert_called_once_with(config)
    client.invoke.assert_called_once_with(
        FunctionName="analyzer-function",
        Payload=json.dumps(payload),
    )
    assert result == {
        "statusCode": 200,
        "reading_plan_url": "https://example.test/plan",
    }


def test_invoke_lambda_raises_for_function_error() -> None:
    client = MagicMock()
    client.invoke.return_value = {
        "FunctionError": "Unhandled",
        "Payload": io.BytesIO(b'{"errorMessage": "analysis failed"}'),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    with pytest.raises(LambdaError, match="analysis failed"):
        invoke_lambda(
            cast(AWSClientProvider, provider),
            "analyzer-function",
            {"benchmark_id": "benchmark-1"},
        )


def test_invoke_lambda_raises_for_error_status() -> None:
    client = MagicMock()
    client.invoke.return_value = {
        "Payload": io.BytesIO(b'{"statusCode": 503, "errorMessage": "service unavailable"}'),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    with pytest.raises(LambdaError, match="returned status 503"):
        invoke_lambda(
            cast(AWSClientProvider, provider),
            "analyzer-function",
            {"benchmark_id": "benchmark-1"},
        )


def test_invoke_lambda_returns_non_object_payload() -> None:
    client = MagicMock()
    client.invoke.return_value = {
        "Payload": io.BytesIO(b'["result"]'),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    result = invoke_lambda(
        cast(AWSClientProvider, provider),
        "analyzer-function",
        {"benchmark_id": "benchmark-1"},
    )

    assert result == ["result"]
