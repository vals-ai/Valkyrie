from aws_cdk import Tags
from constructs import Construct

DEPLOYMENT_STAGE_TAG_KEY = "stage"


def tag_with_deployment_stage(scope: Construct, stage_name: str) -> None:
    Tags.of(scope).add(DEPLOYMENT_STAGE_TAG_KEY, stage_name)
