"""Saved task-result locators into a retired bucket keep a plan out of portable release."""

import json
from typing import Any
from uuid import uuid4

from tracker.run_relocation import result_locator_references


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
