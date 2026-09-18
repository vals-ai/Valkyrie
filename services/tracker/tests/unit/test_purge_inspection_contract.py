"""Published inspection bytes stay synchronized with strict private contracts."""

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_schema
from pydantic import ValidationError

from tracker.run_purge.contracts import PurgeInspection, PurgePlan

DOCS = Path(__file__).resolve().parents[4] / "docs" / "deployment"


def fixture(name: str) -> dict[str, Any]:
    return json.loads((DOCS / "fixtures" / name).read_text())


def test_published_inspection_and_plan_are_exact_and_consistent() -> None:
    for model, schema_name, fixture_name in (
        (PurgeInspection, "tracker-purge-inspection-v1.schema.json", "tracker-purge-inspection-v1.json"),
        (PurgePlan, "tracker-purge-plan-v1.schema.json", "tracker-purge-inspection-plan-v1.json"),
    ):
        schema = json.loads((DOCS / schema_name).read_text())
        assert schema == model.model_json_schema()
        Draft202012Validator.check_schema(schema)
        payload = fixture(fixture_name)
        validate_schema(payload, schema)
        model.model_validate(payload)
    plan = PurgePlan.model_validate(fixture("tracker-purge-inspection-plan-v1.json"))
    inspection = PurgeInspection.model_validate(fixture("tracker-purge-inspection-v1.json"))
    assert inspection.child_plan_sha256 == plan.digest()
    assert inspection.identity == plan.identity
    assert tuple(run.scope for run in inspection.runs) == tuple(run.scope for run in plan.runs)
    assert tuple(run.provider for run in inspection.runs) == tuple(run.provider for run in plan.runs)


def test_old_plan_digest_and_nullable_label_compatibility() -> None:
    payload = fixture("tracker-purge-inspection-plan-v1.json")
    for run in payload["runs"]:
        run.pop("expected_run_label")
    original = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    old_plan = PurgePlan.model_validate(payload)
    assert old_plan.digest() == original
    for run in payload["runs"]:
        run["expected_run_label"] = None
    assert PurgePlan.model_validate(payload).digest() == original
    payload["runs"][0]["expected_run_label"] = "required-label"
    assert PurgePlan.model_validate(payload).digest() != original


@pytest.mark.parametrize("corruption", ["extra", "label", "run_ids", "digest", "removed_phase", "current_label"])
def test_invalid_inspection_proof_is_rejected(corruption: str) -> None:
    payload = fixture("tracker-purge-inspection-v1.json")
    if corruption == "extra":
        payload["runs"][0]["raw_arguments"] = {}
    elif corruption == "label":
        payload["runs"][0]["current_label"] = "wrong"
    elif corruption == "run_ids":
        payload["runs"].reverse()
    elif corruption == "digest":
        payload["runs"][1]["checkpoint"]["child_plan_sha256"] = "0" * 64
    elif corruption == "removed_phase":
        payload["runs"][2]["checkpoint"]["phase"] = "held"
    else:
        payload["runs"][2]["current_label"] = "invented"
    with pytest.raises(ValidationError):
        PurgeInspection.model_validate(payload)
