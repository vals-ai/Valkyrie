import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.database.models import OrgConfig


def test_create_org_config_persists(database_session: Session) -> None:
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIAEXAMPLE",
        aws_secret_access_key="secret",
        aws_default_region="us-east-2",
        s3_bucket="agentic-harness",
        daytona_secret_name="daytona/prod",
    )
    database_session.add(config)
    database_session.commit()

    fetched = database_session.exec(select(OrgConfig).where(OrgConfig.org_id == TEST_ORG_ID)).one()
    assert fetched.aws_access_key_id == "AKIAEXAMPLE"
    assert fetched.s3_bucket == "agentic-harness"
    assert fetched.log_group is None
    assert fetched.webhook is None


def test_org_config_one_per_org(database_session: Session) -> None:
    database_session.add(
        OrgConfig(
            org_id=TEST_ORG_ID,
            aws_access_key_id="k1",
            aws_secret_access_key="s1",
            aws_default_region="us-east-2",
            s3_bucket="b1",
            daytona_secret_name="d1",
        )
    )
    database_session.commit()

    database_session.add(
        OrgConfig(
            org_id=TEST_ORG_ID,
            aws_access_key_id="k2",
            aws_secret_access_key="s2",
            aws_default_region="us-east-2",
            s3_bucket="b2",
            daytona_secret_name="d2",
        )
    )
    with pytest.raises(IntegrityError):
        database_session.commit()


def test_org_config_persists_benchmark_services(database_session):
    from sqlmodel import select
    from tests.conftest import TEST_ORG_ID
    from tracker.database.models import OrgConfig

    cfg = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="s",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="d",
        benchmark_services=[
            {
                "name": "swebench",
                "url": "http://swebench:8001",
                "auth_header_name": None,
                "auth_secret_name": None,
            },
        ],
    )
    database_session.add(cfg)
    database_session.commit()

    fetched = database_session.exec(select(OrgConfig).where(OrgConfig.org_id == TEST_ORG_ID)).one()
    assert len(fetched.benchmark_services) == 1
    assert fetched.benchmark_services[0]["name"] == "swebench"
