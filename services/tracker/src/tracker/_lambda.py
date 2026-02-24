import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tracker.exceptions import LambdaError


def invoke_lambda(function_name: str, payload: dict[str, Any]):
    """
    Invokes lambda method provided the function name and the payload.

    Raises LambdaError if the invocation fails or the Lambda returns an error.
    """

    try:
        lambda_client = boto3.client("lambda")

        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )

        function_error = response.get("FunctionError")
        response_payload = json.loads(response["Payload"].read())

        # Check if the Lambda function errored
        if function_error:
            raise LambdaError(
                f"Lambda function '{function_name}' returned error: {json.dumps(response_payload, indent=4)}"
            )

        # Check for errors returned from the lambda itself
        payload_status = response_payload.get("statusCode") if isinstance(response_payload, dict) else None
        if payload_status and payload_status >= 400:
            raise LambdaError(
                f"Lambda function '{function_name}' returned status {payload_status}: {json.dumps(response_payload, indent=4)}"
            )
    except ClientError as e:
        raise LambdaError(f"Failed to invoke lambda function '{function_name}': {e}") from e
