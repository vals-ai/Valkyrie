from uuid import UUID

import pytest

from tracker.database.models import Org
from tracker.types import HarnessConfig, TaskRoleAWSConfig


async def test_managed_agent_reads_use_org_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.agents as agents_api

    observed: dict[str, str] = {}
    harness_config = HarnessConfig(
        aws=TaskRoleAWSConfig(aws_default_region="us-east-1"),
        s3_bucket="managed-bucket",
        s3_prefix="orgs/org-id",
        log_group="managed-logs/orgs/org-id",
        log_retention_policy=30,
        sandbox_provider_secret_name="managed-provider",
    )

    async def list_agents(**kwargs: object) -> list[tuple[str, None]]:
        observed["list_prefix"] = str(kwargs["s3_prefix"])
        return []

    async def object_exists(key: str, **_kwargs: object) -> bool:
        observed["download_key"] = key
        return True

    async def object_size(*_args: object, **_kwargs: object) -> int:
        return 1

    async def presigned_url(*_args: object, **_kwargs: object) -> str:
        return "https://s3.test/agent.zip"

    monkeypatch.setattr(agents_api, "list_agents", list_agents)
    monkeypatch.setattr(agents_api, "s3_object_exists", object_exists)
    monkeypatch.setattr(agents_api, "get_s3_object_size", object_size)
    monkeypatch.setattr(agents_api, "create_presigned_url", presigned_url)
    org = Org(id=UUID(int=1), name="org")

    await agents_api.list_agents_endpoint(_org=org, harness_config=harness_config)
    await agents_api.get_agent_download_url("agent", _org=org, harness_config=harness_config)

    assert observed == {
        "list_prefix": "orgs/org-id",
        "download_key": "orgs/org-id/agents/agent.zip",
    }
