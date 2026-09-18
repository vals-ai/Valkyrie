"""Positive drain evidence excludes status and elapsed-time guesses."""

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from tracker.aws.runtime import AWSResources
from tracker.database.models import ExecutorDispatch, ExecutorDispatchKind, ExecutorDispatchStatus
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope
from tracker.lifecycle_evidence import (
    HostContractObservation,
    LifecycleReport,
    RunReport,
    classify_dispatch,
    write_report,
)


@pytest.mark.parametrize("status", [ExecutorDispatchStatus.FAILED, ExecutorDispatchStatus.FINISHED])
def test_status_is_not_exit_evidence_without_host_contract(status: ExecutorDispatchStatus) -> None:

    dispatch = ExecutorDispatch(
        id=uuid4(),
        benchmark_id=uuid4(),
        kind=ExecutorDispatchKind.START,
        executor_release_id="release",
        executor_artifact_uri="s3://bucket/a",
        executor_artifact_digest="a" * 64,
        executor_protocol_version="1",
        status=status,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
    )
    assert classify_dispatch(dispatch, host_contract=None).provenance == "pending"
    dispatch.process_exited_at = datetime.now(UTC)
    assert classify_dispatch(dispatch, host_contract=None).provenance == "host_process_exit"


def test_report_roundtrip_and_private_atomic_write(tmp_path: Path) -> None:

    run_id = uuid4()
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
    scope = RunScope(
        run_id=run_id,
        original_resources=AWSResources(region="us-west-2", s3_bucket="bucket", log_group="runs", log_retention_days=7),
    )
    report = LifecycleReport(
        identity=identity,
        purpose="deletion",
        observed_at=datetime.now(UTC),
        runs=(RunReport(scope=scope, phase="held"),),
    )
    path = tmp_path / "receipt.json"
    write_report(path, report)
    assert path.stat().st_mode & 0o777 == 0o600
    assert LifecycleReport.model_validate_json(path.read_text()) == report
    assert list(tmp_path.iterdir()) == [path]
    with pytest.raises(ValueError):
        RunScope.model_validate({**scope.model_dump(), "object_prefix": "benchmarks/other/"})


@pytest.mark.parametrize("invalid_time", ["future", "expired", "cutoff", "naive", "non_utc"])
@pytest.mark.parametrize("status", [ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.FINISHED])
def test_host_observation_must_be_current_at_use(invalid_time: str, status: ExecutorDispatchStatus) -> None:
    now = datetime.now(UTC)
    observation = HostContractObservation(
        contract="stable-host-lifecycle-v1",
        deployment_sha256="a" * 64,
        host_inventory=("host-1",),
        observed_at=now,
        acknowledgement_required_since=now - timedelta(days=1),
        verifier="operator",
    )
    if invalid_time == "future":
        observation = observation.model_copy(update={"observed_at": now + timedelta(days=1)})
    elif invalid_time == "expired":
        observation = observation.model_copy(update={"observed_at": now - timedelta(minutes=16)})
    elif invalid_time == "cutoff":
        observation = observation.model_copy(update={"acknowledgement_required_since": now + timedelta(seconds=1)})
    elif invalid_time == "naive":
        observation = observation.model_copy(update={"observed_at": now.replace(tzinfo=None)})
    else:
        observation = observation.model_copy(update={"observed_at": now.astimezone(timezone(timedelta(hours=1)))})
    dispatch = ExecutorDispatch(
        benchmark_id=uuid4(),
        kind=ExecutorDispatchKind.START,
        executor_release_id="release",
        executor_artifact_uri="s3://bucket/a",
        executor_artifact_digest="a" * 64,
        executor_protocol_version="1",
        status=status,
        started_at=now if status == ExecutorDispatchStatus.FINISHED else None,
    )
    with pytest.raises(LifecycleConflict, match="Host contract observation"):
        classify_dispatch(dispatch, host_contract=observation)
