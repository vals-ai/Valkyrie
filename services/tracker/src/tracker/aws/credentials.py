"""AWS SDK arguments for explicit credentials and ECS task roles."""

from typing import Any

from tracker.types import AWSConfig, AWSCredentials


def aws_client_kwargs(aws: AWSConfig) -> dict[str, Any]:
    if isinstance(aws, AWSCredentials):
        return {
            "aws_access_key_id": aws.aws_access_key_id,
            "aws_secret_access_key": aws.aws_secret_access_key,
            "aws_session_token": aws.aws_session_token,
            "region_name": aws.aws_default_region,
        }
    return {"region_name": aws.aws_default_region}
