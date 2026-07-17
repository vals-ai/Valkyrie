import io
import json
from typing import cast
from unittest.mock import MagicMock

from botocore.config import Config

from tracker._lambda import invoke_lambda
from tracker.aws.clients import AwsClientProvider


def test_invoke_lambda_uses_provider_config_and_returns_parsed_payload() -> None:
    client = MagicMock()
    client.invoke.return_value = {
        "Payload": io.BytesIO(b'{"statusCode": 200, "reading_plan_url": "https://example.test/plan"}')
    }
    provider = MagicMock(spec=AwsClientProvider)
    provider.lambda_client.return_value = client
    config = Config(read_timeout=905)
    payload = {"benchmark_id": "benchmark-1"}

    result = invoke_lambda(
        cast(AwsClientProvider, provider),
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
