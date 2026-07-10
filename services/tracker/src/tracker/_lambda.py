import json
from functools import lru_cache
from typing import Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from tracker.aws.credentials import aws_client_kwargs
from tracker.exceptions import LambdaError
from tracker.types import AWSConfig


@lru_cache(maxsize=32)
def lambda_client(aws: AWSConfig, config: Config | None = None) -> Any:
    """Build a boto3 Lambda client. The caller chooses the Config — defaults
    (60s read timeout, standard retries) are appropriate for short-running
    Lambdas; long-running invocations (e.g. analyzer Lambdas) should pass a
    Config with an extended read_timeout."""
    return cast(
        Any,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "lambda",
            **aws_client_kwargs(aws),
            config=config,
        ),
    )


def invoke_lambda(client: Any, function_name: str, payload: dict[str, Any]) -> Any:
    """Invoke a Lambda using the provided boto3 client and return its parsed payload.

    Raises LambdaError on AWS errors, Lambda-side FunctionError, or statusCode >= 400.
    """
    try:
        response = client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )

        function_error = response.get("FunctionError")
        response_payload = json.loads(response["Payload"].read())

        if function_error:
            raise LambdaError(
                f"Lambda function '{function_name}' returned error: {json.dumps(response_payload, indent=4)}"
            )

        payload_status = response_payload.get("statusCode") if isinstance(response_payload, dict) else None
        if payload_status and payload_status >= 400:
            raise LambdaError(
                f"Lambda function '{function_name}' returned status {payload_status}: {json.dumps(response_payload, indent=4)}"
            )

        return response_payload
    except ClientError as e:
        raise LambdaError(f"Failed to invoke lambda function '{function_name}': {e}") from e
