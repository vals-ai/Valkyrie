import json
from typing import Any, cast

from botocore.config import Config
from botocore.exceptions import ClientError

from tracker.aws.clients import AwsClientProvider
from tracker.exceptions import LambdaError


def _response_status(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return None
    return cast(dict[str, Any], payload).get("statusCode")


def invoke_lambda(
    client_provider: AwsClientProvider,
    function_name: str,
    payload: dict[str, Any],
    config: Config | None = None,
) -> Any:
    """Invoke a Lambda using the provided client source and return its parsed payload.

    Raises LambdaError on AWS errors, Lambda-side FunctionError, or statusCode >= 400.
    """
    client = client_provider.lambda_client(config)
    try:
        response: dict[str, Any] = client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )

        function_error = response.get("FunctionError")
        response_payload: Any = json.loads(response["Payload"].read())

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
