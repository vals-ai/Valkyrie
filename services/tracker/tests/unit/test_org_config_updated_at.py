import time

from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import OrgConfig


def test_updated_at_refreshes_on_write(database_session: Session) -> None:
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIA1",
        aws_secret_access_key="s1",
        aws_default_region="us-east-2",
        s3_bucket="b1",
        daytona_secret_name="d1",
    )
    database_session.add(config)
    database_session.commit()
    database_session.refresh(config)
    initial = config.updated_at

    # Make sure time advances enough to detect the change
    time.sleep(0.01)

    config.s3_bucket = "b2"
    database_session.add(config)
    database_session.commit()
    database_session.refresh(config)

    assert config.updated_at > initial, f"updated_at did not advance after UPDATE: {initial} -> {config.updated_at}"
