from tests.conftest import TEST_ORG_ID
from tracker.database.models import OrgConfig
from tracker.types import MASKED_SECRET, OrgConfigResponse, OrgConfigUpdate


def test_response_masks_secrets():
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="AKIA",
        aws_secret_access_key="real-secret",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="daytona/prod",
        webhook="https://hooks.example.com/T/x/y",
    )
    response = OrgConfigResponse.from_org_config(config)
    assert response.aws_secret_access_key == MASKED_SECRET
    assert response.daytona_secret_name == MASKED_SECRET
    assert response.webhook == MASKED_SECRET
    assert response.aws_access_key_id == "AKIA"
    assert response.s3_bucket == "b"


def test_response_webhook_none_stays_none():
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="s",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="d",
        webhook=None,
    )
    response = OrgConfigResponse.from_org_config(config)
    assert response.webhook is None


def test_update_apply_replaces_non_secret_fields():
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="OLD",
        aws_secret_access_key="old-secret",
        aws_default_region="us-east-1",
        s3_bucket="old",
        daytona_secret_name="old-dt",
    )
    update = OrgConfigUpdate(
        aws_access_key_id="NEW",
        aws_secret_access_key="new-secret",
        aws_default_region="us-east-2",
        s3_bucket="new",
        daytona_secret_name="new-dt",
    )
    update.apply_to(config)
    assert config.aws_access_key_id == "NEW"
    assert config.aws_secret_access_key == "new-secret"
    assert config.aws_default_region == "us-east-2"
    assert config.s3_bucket == "new"
    assert config.daytona_secret_name == "new-dt"


def test_update_masked_sentinel_preserves_secret():
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="real-secret",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="real-daytona",
        webhook="real-webhook",
    )
    update = OrgConfigUpdate(
        aws_access_key_id="A",
        aws_secret_access_key=MASKED_SECRET,
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name=MASKED_SECRET,
        webhook=MASKED_SECRET,
    )
    update.apply_to(config)
    assert config.aws_secret_access_key == "real-secret"
    assert config.daytona_secret_name == "real-daytona"
    assert config.webhook == "real-webhook"


def test_update_explicit_none_clears_optional_secret():
    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="s",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="d",
        webhook="existing",
    )
    update = OrgConfigUpdate(
        aws_access_key_id="A",
        aws_secret_access_key=MASKED_SECRET,
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name=MASKED_SECRET,
        webhook=None,
    )
    update.apply_to(config)
    assert config.webhook is None


def test_org_config_response_includes_benchmark_services():
    from tracker.types import BenchmarkServiceEntry, OrgConfigResponse

    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="s",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="d",
        benchmark_services=[
            {"name": "swebench", "url": "http://x:8001", "auth_header_name": None, "auth_secret_name": None},
        ],
    )
    response = OrgConfigResponse.from_org_config(config)
    assert len(response.benchmark_services) == 1
    assert response.benchmark_services[0].name == "swebench"


def test_org_config_update_applies_benchmark_services():
    from tracker.types import BenchmarkServiceEntry, OrgConfigUpdate

    config = OrgConfig(
        org_id=TEST_ORG_ID,
        aws_access_key_id="A",
        aws_secret_access_key="s",
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name="d",
        benchmark_services=[],
    )
    update = OrgConfigUpdate(
        aws_access_key_id="A",
        aws_secret_access_key=MASKED_SECRET,
        aws_default_region="us-east-2",
        s3_bucket="b",
        daytona_secret_name=MASKED_SECRET,
        benchmark_services=[
            BenchmarkServiceEntry(
                name="swebench",
                url="http://x:8001",
                auth_header_name="X-Descope-Api-Key",
                auth_secret_name="prodSwebenchKey",
            ),
        ],
    )
    update.apply_to(config)
    assert len(config.benchmark_services) == 1
    assert config.benchmark_services[0]["name"] == "swebench"
    assert config.benchmark_services[0]["auth_header_name"] == "X-Descope-Api-Key"
