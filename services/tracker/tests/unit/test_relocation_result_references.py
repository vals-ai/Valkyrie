"""Retired-bucket derivation and the saved task-result locators that block a portable release."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from tracker.aws.runtime import AWSResources
from tracker.database.models import RunLifecycle
from tracker.lifecycle import RunScope
from tracker.run_relocation import recorded_source_bucket, result_locator_references


def test_result_locators_into_a_retired_bucket_are_reported_without_rewriting_saved_json() -> None:
    result_id = uuid4()
    result: dict[str, Any] = {
        "eval_output_path": "s3://legacy-shared-storage/benchmarks/run/browser_use_output/result.json",
        "artifacts": [{"a/b~c": "s3://legacy-shared-storage/benchmarks/run/app_startup/error.txt"}],
        "score": 1,
    }
    unchanged = json.loads(json.dumps(result))

    references = result_locator_references([(result_id, result)], frozenset({"legacy-shared-storage"}))

    assert [item.pointer for item in references] == [
        f"/evaluation_result/{result_id}/eval_output_path",
        f"/evaluation_result/{result_id}/artifacts/0/a~1b~0c",
    ]
    assert {item.kind for item in references} == {"unknown"}
    assert all(item.kind not in {"builtin_dataset", "retained_s3_object"} for item in references)
    assert result == unchanged


def test_locators_outside_every_retired_bucket_are_not_reported() -> None:
    result: dict[str, Any] = {
        "retained": "s3://vs-dev-owner-42/benchmarks/run/result.json",
        "prefix_lookalike": "s3://legacy-shared-storage-two/benchmarks/run/result.json",
        "relative": "benchmarks/run/result.json",
    }

    assert result_locator_references([(uuid4(), result)], frozenset({"legacy-shared-storage"})) == ()
    assert result_locator_references([(uuid4(), result)], frozenset()) == ()


def test_a_relocated_run_keeps_its_recorded_source_bucket_as_the_retired_one() -> None:
    run_id = uuid4()
    scope = RunScope(
        run_id=run_id,
        original_resources=AWSResources("us-east-1", "legacy-shared-storage", "runs", 7),
    )
    relocated = {"properties": {"region": "us-east-1", "s3_bucket": "vs-dev-owner-42", "log_group": "runs"}}
    record = RunLifecycle(
        run_id=run_id,
        identity_json="{}",
        scope_json=scope.model_dump_json(),
        purpose="relocation",
        phase="relocated",
        acquired_at=datetime.now(UTC),
    )

    assert recorded_source_bucket(record, relocated) == "legacy-shared-storage"
    assert recorded_source_bucket(None, relocated) == "vs-dev-owner-42"
