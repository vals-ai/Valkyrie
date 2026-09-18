"""Stored contract secret maps are references, never credential values."""

import copy
import hashlib
import json
from io import BytesIO
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from tests.transfer_support import SecretMetadataSession
from tracker.database.models import AgentContractRequest
from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.references import inventory_references, verify_portable_references
from tracker.run_transfer.rows import RowClosure


def contract_rows(contract: AgentContractRequest) -> RowClosure:
    return RowClosure(
        {
            "benchmark": [
                {
                    "id": UUID("10000000-0000-0000-0000-000000000001"),
                    "arguments": {"contract": contract.model_dump(mode="json")},
                    "webhook_secret_name": None,
                    "custom_benchmark_service": None,
                }
            ]
        },
        {},
    )


def test_empty_serialized_contract_secret_map_needs_no_metadata() -> None:
    rows = contract_rows(AgentContractRequest(name="test"))
    before = copy.deepcopy(rows)
    session = SecretMetadataSession({})

    verify_portable_references(rows, session, "222222222222", "us-west-2")

    assert rows == before
    assert inventory_references(rows) == ()
    assert session.requested == []


def test_contract_secret_inventory_keeps_map_context_and_escaped_pointers() -> None:
    rows = contract_rows(
        AgentContractRequest(name="test", secrets={"API_KEY": "agent-reference", "API~/KEY": "second-reference"})
    )
    before = copy.deepcopy(rows)

    inventory = inventory_references(rows)

    assert [item.model_dump(mode="json") for item in inventory] == [
        {
            "table": "benchmark",
            "row_id": "10000000-0000-0000-0000-000000000001",
            "pointer": pointer,
            "kind": "secret_locator",
            "value_sha256": hashlib.sha256(json.dumps(value).encode()).hexdigest(),
        }
        for pointer, value in (
            ("/arguments/contract/secrets/API_KEY", "agent-reference"),
            ("/arguments/contract/secrets/API~0~1KEY", "second-reference"),
        )
    ]
    assert rows == before


@pytest.mark.parametrize("invalid", [None, "missing", "account", "region", "current", "deleted"])
def test_contract_secret_map_requires_each_destination_metadata_record(invalid: str | None) -> None:
    rows = contract_rows(
        AgentContractRequest(name="test", secrets={"API_KEY": "agent-reference", "API~/KEY": "second-reference"})
    )
    before = copy.deepcopy(rows)
    metadata: dict[str, dict[str, Any]] = {
        name: {
            "ARN": f"arn:aws:secretsmanager:us-west-2:222222222222:secret:{name}-ABC123",
            "VersionIdsToStages": {"version-one": ["AWSCURRENT"]},
        }
        for name in ("agent-reference", "second-reference")
    }
    if invalid == "missing":
        del metadata["second-reference"]
    elif invalid == "account":
        metadata["second-reference"]["ARN"] = "arn:aws:secretsmanager:us-west-2:111111111111:secret:other-ABC123"
    elif invalid == "region":
        metadata["second-reference"]["ARN"] = "arn:aws:secretsmanager:us-east-1:222222222222:secret:other-ABC123"
    elif invalid == "current":
        metadata["second-reference"]["VersionIdsToStages"] = {"version-one": ["AWSPREVIOUS"]}
    elif invalid == "deleted":
        metadata["second-reference"]["DeletedDate"] = "2026-09-18T00:00:00Z"

    session = SecretMetadataSession(metadata)
    if invalid is not None:
        with pytest.raises(ClientError if invalid == "missing" else LifecycleConflict):
            verify_portable_references(rows, session, "222222222222", "us-west-2")
    else:
        verify_portable_references(rows, session, "222222222222", "us-west-2")

    assert session.requested == ["agent-reference", "second-reference"]
    assert rows == before


@pytest.mark.parametrize(
    "failure", [None, "version", "size", "unversioned", "null", "fragment", "unknown", "secret-map", "secret-list"]
)
def test_portable_object_requires_exact_version_bytes_and_closes_body(failure: str | None) -> None:
    rows = contract_rows(AgentContractRequest(name="test"))
    arguments = rows.rows["benchmark"][0]["arguments"]
    arguments["dataset"] = "s3://destination/data?versionId=immutable"
    if failure in {"unversioned", "null", "fragment"}:
        arguments["dataset"] = {
            "unversioned": "s3://destination/data",
            "null": "s3://destination/data?versionId=null",
            "fragment": "s3://destination/data?versionId=immutable#fragment",
        }[failure]
    elif failure == "unknown":
        arguments["contract"] = {"nested": [{"url": "https://unknown/data"}]}
    elif failure == "secret-map":
        arguments["contract"] = {"secrets": []}
    elif failure == "secret-list":
        arguments["contract"] = {"other_secrets": [""]}
    body = BytesIO(b"full body")
    calls: list[dict[str, Any]] = []

    class Destination:
        def client(self, service: str, *, region_name: str) -> "Destination":
            assert service == "s3" and region_name == "us-west-2"
            return self

        def get_object(self, **options: Any) -> dict[str, Any]:
            calls.append(options)
            return {
                "Body": body,
                "VersionId": "wrong" if failure == "version" else "immutable",
                "ContentLength": 999 if failure == "size" else 9,
            }

    if failure is None:
        proof = verify_portable_references(rows, Destination(), "222222222222", "us-west-2")
        assert len(proof) == 64
    else:
        with pytest.raises(LifecycleConflict):
            verify_portable_references(rows, Destination(), "222222222222", "us-west-2")
    if calls:
        assert calls == [
            {"Bucket": "destination", "Key": "data", "VersionId": "immutable", "ExpectedBucketOwner": "222222222222"}
        ]
        assert body.closed
