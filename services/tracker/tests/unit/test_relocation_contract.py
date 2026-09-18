"""Published request boundaries reject ambiguous run scopes before operator access."""

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_schema
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

from pydantic import BaseModel

from tests.unit.test_relocation_providers import setup
from tracker.storage_migration_exchange import TrackerRequest, TrackerResponse

_DOCUMENTATION = Path(__file__).resolve().parents[4] / "docs" / "deployment"


@pytest.mark.parametrize(
    "model,published",
    [
        (TrackerRequest, "storage-migration-request-v1.schema.json"),
        (TrackerResponse, "storage-migration-response-v1.schema.json"),
    ],
)
def test_published_exchange_schema_equals_the_generated_model_schema(model: type[BaseModel], published: str) -> None:
    assert json.loads((_DOCUMENTATION / published).read_text()) == model.model_json_schema()


@pytest.mark.parametrize("scope", ["empty", "duplicate", "unsorted"])
def test_request_scope_is_rejected_before_any_operator_use(scope: str) -> None:
    _, _, payload = setup()
    run_id = payload["run_ids"][0]
    payload["run_ids"] = (
        []
        if scope == "empty"
        else [run_id, run_id]
        if scope == "duplicate"
        else sorted([run_id, str(uuid4())], reverse=True)
    )
    with pytest.raises(ValidationError):
        TrackerRequest.model_validate(payload)
    if scope != "unsorted":
        schema = json.loads((_DOCUMENTATION / "storage-migration-request-v1.schema.json").read_text())
        with pytest.raises(SchemaValidationError):
            validate_schema(payload, schema, cls=Draft202012Validator)


@pytest.mark.parametrize(
    "failure",
    [
        "operation-runs",
        "prefix",
        "history-predecessor",
        "pointer",
        "empty-edits",
        "cross-account",
        "missing-run",
        "region-change",
        "duplicate-transform",
        "copy-prefix",
        "marker-hash",
        "live-hash",
        "host-inventory",
    ],
)
def test_invalid_plan_or_copy_proof_cannot_enter_operator(failure: str) -> None:
    _, _, payload = setup()
    plan, copy = payload["plan"], payload["copied_objects"][0]
    run = plan["runs"][0]
    transformation: dict[str, Any] = {
        "source_bucket": "source",
        "key": copy["key"],
        "source_version_id": "s1",
        "original_size": 11,
        "original_sha256": "a" * 64,
        "rewritten_size": 11,
        "rewritten_sha256": "b" * 64,
        "edits": [{"pointer": "/uri", "original": "old", "replacement": "new"}],
    }
    if failure == "operation-runs":
        plan["identity"]["run_ids"] *= 2
    elif failure == "prefix":
        run["scope"]["object_prefix"] = "outside/"
    elif failure == "history-predecessor":
        run["predecessor"] = {
            "kind": "completed_history_only",
            "operation_id": str(uuid4()),
            "identity_sha256": "a" * 64,
            "scope_sha256": "b" * 64,
        }
    elif failure in {"pointer", "empty-edits", "duplicate-transform"}:
        run["transformations"] = [transformation]
        if failure == "pointer":
            transformation["edits"][0]["pointer"] = "/invalid~escape"
        elif failure == "empty-edits":
            transformation["edits"] = []
        else:
            run["transformations"].append(transformation)
    elif failure == "cross-account":
        plan["identity"]["destination_aws_account_id"] = "999999999999"
    elif failure == "missing-run":
        plan["runs"] = []
    elif failure == "region-change":
        run["destination_resources"]["region"] = "us-west-2"
    elif failure == "copy-prefix":
        copy["key"] = "outside/"
    elif failure == "marker-hash":
        copy["is_delete_marker"] = True
    elif failure == "live-hash":
        copy["source_sha256"] = None
    else:
        payload["host_contract"] = {
            "contract": "stable-host-lifecycle-v1",
            "deployment_sha256": "b" * 64,
            "host_inventory": ["same", "same"],
            "observed_at": "2026-09-18T00:00:00Z",
            "acknowledgement_required_since": "2026-09-18T00:00:00Z",
            "verifier": "test",
        }
    with pytest.raises(ValidationError):
        TrackerRequest.model_validate(payload)


def test_explicit_hold_only_and_default_relocation_have_stable_plan_digests() -> None:
    directory = Path(__file__).resolve().parents[4] / "docs/deployment/fixtures"
    expected = {
        "default": "53f268875b62181627ee2dbcd9c1021693a2e2121d6eb825394009c58b1f7bee",
        "hold-only": "a4d0e68bd9f9cf551f4cf24c559aaae397087f152576989b0d855b0cd4fc8bb1",
    }
    for name, digest in expected.items():
        payload = json.loads((directory / f"storage-migration-{name}-request-v1.json").read_text())
        request = TrackerRequest.model_validate(payload)
        assert request.plan is not None and request.plan.sha256 == digest
        if name == "default":
            payload["plan"]["runs"][0]["location_policy"] = "relocate"
            explicit = TrackerRequest.model_validate(payload)
            assert explicit.plan is not None and explicit.plan.sha256 == digest
        else:
            payload["plan"]["runs"][0].pop("location_policy")
            with pytest.raises(ValidationError, match="hold_only"):
                TrackerRequest.model_validate(payload)
