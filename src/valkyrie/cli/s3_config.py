import click
from tracker.aws.s3 import s3_client as tracker_s3_client
from tracker.types import AWSCredentials

from valkyrie.cli.config.state import load_config


def fetch_bucket_name() -> str:
    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return bucket_name


def aws_credentials() -> AWSCredentials:
    """Build AWS credentials from the valkyrie config."""
    config = load_config()
    return AWSCredentials(
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        aws_default_region=config["AWS_DEFAULT_REGION"],
        aws_session_token=config.get("AWS_SESSION_TOKEN"),
    )


def s3_client():
    """Create an async S3 client using credentials from the valkyrie config."""
    return tracker_s3_client(aws_credentials())
