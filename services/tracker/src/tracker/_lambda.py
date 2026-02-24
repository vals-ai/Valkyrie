import json
from typing import Any

import boto3
from botocore.utils import ClientError

from tracker.exceptions import LambdaError


def invoke_lambda(function_name: str, payload: dict[str, Any]):
    """
    Invokes lambda method provided the function name and the payload

    # NOTE Will not return anything, merely background
    """

    try:
        lambda_client = boto3.client("lambda")

        _ = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload),
        )
    except ClientError as e:
        raise LambdaError(f"Failed to invoke lambda function '{function_name}': {e}") from e
