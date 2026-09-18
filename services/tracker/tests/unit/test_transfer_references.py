"""Stored contract secret maps are references, never credential values."""

import copy
import hashlib
import json
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
