import json
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import LambdaError


def _response_status(payload: object) -> int | None:
    if not isinstance(payload, dict):
        return None

    status = payload.get("statusCode")
    return status if isinstance(status, int) else None


async def invoke_lambda(
    client_provider: AWSClientProvider,
    function_name: str,
    payload: dict[str, Any],
    config: Config | None = None,
) -> Any:
    """Invoke a Lambda using the provided client source and return its parsed payload.

    Raises LambdaError on AWS errors, Lambda-side FunctionError, or statusCode >= 400.
    """
    try:
        async with client_provider.lambda_client(config) as client:
            response: dict[str, Any] = await client.invoke(
                FunctionName=function_name,
                Payload=json.dumps(payload),
            )

            function_error = response.get("FunctionError")
            response_payload: Any = json.loads(await response["Payload"].read())

            if function_error:
                raise LambdaError(
                    f"Lambda function '{function_name}' returned error: {json.dumps(response_payload, indent=4)}"
                )

            payload_status = _response_status(response_payload)
            if payload_status and payload_status >= 400:
                raise LambdaError(
                    f"Lambda function '{function_name}' returned status {payload_status}: {json.dumps(response_payload, indent=4)}"
                )

            return response_payload
    except ClientError as e:
        raise LambdaError(f"Failed to invoke lambda function '{function_name}': {e}") from e


async def dry_run_lambda(client_provider: AWSClientProvider, function_name: str) -> None:
    """Verify that the selected AWS authority can invoke a Lambda function."""
    try:
        async with client_provider.lambda_client() as client:
            await client.invoke(FunctionName=function_name, InvocationType="DryRun")
    except ClientError as e:
        raise LambdaError(f"Lambda invoke preflight failed for '{function_name}': {e}") from e
