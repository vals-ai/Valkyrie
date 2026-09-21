"""Published JSON Schema must enforce the same resource contract as runtime parsing."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_schema
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import BaseModel, TypeAdapter, ValidationError

from tracker.aws.runtime import AWSResources
from tracker.lifecycle import OperationIdentity, RunScope
from tracker.lifecycle_evidence import ExternalHostDrain, HostContractObservation, LifecycleReport, RunReport
from tracker.run_purge.contracts import ProviderLocator, PurgeCheckpoint, PurgePlan, PurgeReport, PurgeRun
from tracker.run_purge.providers import FenceReceipt, OwnerDeletionFence

_DOCUMENTATION = Path(__file__).resolve().parents[4] / "docs" / "deployment"

_PURGE_DEFINITIONS: dict[str, Any] = {
    "PurgePlan": PurgePlan.model_json_schema(),
    "PurgeReport": PurgeReport.model_json_schema(),
    "FenceReceipts": TypeAdapter(tuple[FenceReceipt, ...]).json_schema(),
    "PurgeCheckpoint": PurgeCheckpoint.model_json_schema(),
    "OwnerDeletionFence": OwnerDeletionFence.model_json_schema(),
}


@pytest.mark.parametrize("kind", ["scope", "lifecycle_report", "purge_plan", "purge_report"])
@pytest.mark.parametrize("published", [False, True])
def test_unknown_resource_fields_fail_runtime_and_json_schema(kind: str, published: bool) -> None:
    run_id = uuid4()
    scope = RunScope(
        run_id=run_id,
        original_resources=AWSResources(
            region="us-west-2",
            s3_bucket="owner-bucket",
            log_group="runs",
            log_retention_days=7,
        ),
    )
    identity = OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=uuid4(),
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-west-2",
        environment="test",
        database_target="tracker-test",
        run_ids=(run_id,),
    )
    report = LifecycleReport(
        identity=identity,
        purpose="deletion",
        observed_at=datetime.now(UTC),
        runs=(RunReport(scope=scope, phase="held"),),
    )
    instance: BaseModel
    if kind == "scope":
        instance = scope
    elif kind == "lifecycle_report":
        instance = report
    elif kind == "purge_plan":
        instance = PurgePlan(
            identity=identity,
            runs=(PurgeRun(scope=scope, provider=ProviderLocator(kind="daytona", secret_name="provider")),),
        )
    else:
        instance = PurgeReport(**report.model_dump(), child_plan_sha256="b" * 64, outcome="incomplete")
    schema = type(instance).model_json_schema()
    if published:
        if kind in ("scope", "lifecycle_report"):
            schema = json.loads((_DOCUMENTATION / "tracker-lifecycle.schema.json").read_text())
            if kind == "scope":
                schema = {"$ref": "#/$defs/RunScope", "$defs": schema["$defs"]}
        else:
            schema = json.loads((_DOCUMENTATION / "tracker-purge.schema.json").read_text())[type(instance).__name__]
    payload: dict[str, Any] = instance.model_dump(mode="json")
    validate_schema(payload, schema, cls=Draft202012Validator)
    assert type(instance).model_validate(payload) == instance
    resources = payload["original_resources"] if kind == "scope" else payload["runs"][0]["scope"]["original_resources"]
    resources["unexpected_resource_field"] = "untrusted"
    with pytest.raises(ValidationError):
        type(instance).model_validate(payload)
    with pytest.raises(SchemaValidationError):
        validate_schema(payload, schema, cls=Draft202012Validator)


def _external_host_drain(identity: OperationIdentity) -> ExternalHostDrain:
    observed_at = datetime.now(UTC)

    return ExternalHostDrain(
        provenance="externally_confirmed_host_drain",
        identity=identity,
        run_id=identity.run_ids[0],
        hold_acquired_at=observed_at,
        dispatch_ids=(uuid4(),),
        host_inventory=("host-1",),
        deployed_host_contract="stable-host-lifecycle-v1",
        observed_at=observed_at,
        verifier="operator",
        evidence_sha256="c" * 64,
        confirmation="all_inventory_hosts_terminated_and_old_claims_disabled",
    )


@pytest.mark.parametrize("kind", ["identity", "host_observation", "external_drain_hosts", "external_drain_dispatches"])
@pytest.mark.parametrize("invalid_items", ["empty", "duplicate"])
@pytest.mark.parametrize("published", [False, True])
def test_identity_inventories_enforce_nonempty_unique_schema(
    kind: str,
    invalid_items: str,
    published: bool,
) -> None:
    example = LifecycleReport.model_validate_json((_DOCUMENTATION / "tracker-lifecycle-example.json").read_text())
    instance: BaseModel
    field: str
    if kind == "identity":
        instance, field = example.identity, "run_ids"
    elif kind == "host_observation":
        observed_at = datetime.now(UTC)
        instance = HostContractObservation(
            contract="stable-host-lifecycle-v1",
            deployment_sha256="a" * 64,
            host_inventory=("host-1",),
            observed_at=observed_at,
            acknowledgement_required_since=observed_at,
            verifier="operator",
        )
        field = "host_inventory"
    else:
        instance = _external_host_drain(example.identity)
        field = "host_inventory" if kind == "external_drain_hosts" else "dispatch_ids"
    schema = type(instance).model_json_schema()
    if published:
        definitions = json.loads((_DOCUMENTATION / "tracker-lifecycle.schema.json").read_text())["$defs"]
        schema = {"$ref": f"#/$defs/{type(instance).__name__}", "$defs": definitions}
    payload = instance.model_dump(mode="json")
    validate_schema(payload, schema, cls=Draft202012Validator)
    payload[field] = [] if invalid_items == "empty" else [payload[field][0], payload[field][0]]
    with pytest.raises(ValidationError):
        type(instance).model_validate(payload)
    with pytest.raises(SchemaValidationError):
        validate_schema(payload, schema, cls=Draft202012Validator)


@pytest.mark.parametrize("published", [False, True])
def test_report_run_inventory_is_nonempty_and_unique(published: bool) -> None:
    example = LifecycleReport.model_validate_json((_DOCUMENTATION / "tracker-lifecycle-example.json").read_text())
    schema: dict[str, Any] = (
        json.loads((_DOCUMENTATION / "tracker-lifecycle.schema.json").read_text())
        if published
        else LifecycleReport.model_json_schema()
    )
    payload = example.model_dump(mode="json")
    validate_schema(payload, schema, cls=Draft202012Validator)

    empty = {**payload, "runs": []}
    with pytest.raises(ValidationError):
        LifecycleReport.model_validate(empty)
    with pytest.raises(SchemaValidationError):
        validate_schema(empty, schema, cls=Draft202012Validator)

    duplicated = {**payload, "runs": [payload["runs"][0], payload["runs"][0]]}
    with pytest.raises(SchemaValidationError):
        validate_schema(duplicated, schema, cls=Draft202012Validator)


def test_published_lifecycle_schema_equals_runtime_model() -> None:
    published = json.loads((_DOCUMENTATION / "tracker-lifecycle.schema.json").read_text())

    assert published == LifecycleReport.model_json_schema()


@pytest.mark.parametrize("name", list(_PURGE_DEFINITIONS))
def test_published_purge_schema_equals_runtime_model(name: str) -> None:
    published = json.loads((_DOCUMENTATION / "tracker-purge.schema.json").read_text())

    assert published[name] == _PURGE_DEFINITIONS[name]


def test_published_purge_schema_publishes_exactly_the_generated_definitions() -> None:
    published = json.loads((_DOCUMENTATION / "tracker-purge.schema.json").read_text())

    assert list(published) == list(_PURGE_DEFINITIONS)
