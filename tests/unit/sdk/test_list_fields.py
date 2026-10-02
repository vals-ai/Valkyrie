"""Read fields in SDK run and task lists."""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, create_model

from valkyrie.sdk.models._base import ResponseModel
from valkyrie.sdk.models.benchmarks import TaskSummary
from valkyrie.sdk.models.runs import BenchmarkTableRow

RUN_ROW = json.loads((Path(__file__).parents[2] / "fixtures/sdk_api/list.json").read_text())["response"]["benchmarks"][
    0
]
TASK_ROW = {
    "id": "11111111-1111-4111-8111-111111111111",
    "task_id": "task-1",
    "status": "FINISHED",
    "started_at": "2026-07-08T12:00:00+00:00",
    "finished_at": "2026-07-08T12:01:00+00:00",
    "error_message": None,
}


@pytest.mark.parametrize(
    ("model", "row", "field", "value"),
    [
        (BenchmarkTableRow, RUN_ROW, "sandbox_provider", "modal"),
        (BenchmarkTableRow, RUN_ROW, "priority", 0),
        (BenchmarkTableRow, RUN_ROW, "dataset_version", {"id": "release-2026-10-01", "label": "October"}),
        (BenchmarkTableRow, RUN_ROW, "dataset_version", {"id": "release-2026-10-01", "label": None}),
        (TaskSummary, TASK_ROW, "score", 0.0),
        (TaskSummary, TASK_ROW, "score", 0.75),
    ],
)
def test_list_fields_preserve_stored_values(
    model: type[BaseModel], row: dict[str, object], field: str, value: object
) -> None:
    payload = {**row, field: value}
    assert model.model_validate(payload).model_dump(mode="json")[field] == value


@pytest.mark.parametrize(
    ("model", "row", "field"),
    [
        (BenchmarkTableRow, RUN_ROW, "sandbox_provider"),
        (BenchmarkTableRow, RUN_ROW, "priority"),
        (BenchmarkTableRow, RUN_ROW, "dataset_version"),
        (TaskSummary, TASK_ROW, "score"),
    ],
)
@pytest.mark.parametrize("omitted", [False, True])
def test_list_fields_accept_null_and_old_responses(
    model: type[BaseModel], row: dict[str, object], field: str, omitted: bool
) -> None:
    payload = {**row, field: None}
    if omitted:
        del payload[field]
    assert model.model_validate(payload).model_dump(mode="json")[field] is None


@pytest.mark.parametrize(
    ("model", "row", "values"),
    [
        (
            BenchmarkTableRow,
            RUN_ROW,
            {"sandbox_provider": "modal", "priority": 0, "dataset_version": {"id": "release-1", "label": None}},
        ),
        (TaskSummary, TASK_ROW, {"score": 0.75}),
    ],
)
def test_old_clients_ignore_new_list_fields(
    model: type[BaseModel], row: dict[str, object], values: dict[str, object]
) -> None:
    old_fields: dict[str, Any] = {
        name: (field.annotation, field) for name, field in model.model_fields.items() if name not in values
    }
    old_model = create_model(
        "OldListRow",
        __base__=ResponseModel,
        **old_fields,
    )
    payload = model.model_validate({**row, **values}).model_dump()
    old_payload = {name: value for name, value in payload.items() if name not in values}
    old_row = old_model.model_validate(payload)
    assert old_row == old_model.model_validate(old_payload)
    assert old_row.model_dump().keys() == old_payload.keys()
