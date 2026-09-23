"""Version-one adapters for the shared dispatch transactions."""

from uuid import UUID

from fastapi import APIRouter

from tracker.executor.dispatch_api import (
    DispatchIdentity,
    as_utc,
    claim_dispatch,
    complete_dispatch,
    dispatch_authority,
    heartbeat_dispatch,
    database_now,
)
from tracker.executor.run_api import RunState, initialize_run_tasks, read_run_state
from tracker.executor_api.v1.dependencies import DispatchSession
from tracker.executor_api.v1.task_router import router as task_router
from tracker.executor_api.v1.finalization_router import router as finalization_router
from tracker.executor_api.v1.queue_router import router as queue_router
from tracker.executor_api.v1.schemas import (
    AuthorityResponse,
    ClaimRequest,
    DispatchRequest,
    FailRequest,
    LeaseResponse,
    RunInfo,
    RunResources,
    RunStateResponse,
    RunStatus,
    RunTasksRequest,
    TaskState,
    TaskStatus,
    TerminalResponse,
)

router = APIRouter(prefix="/internal/executor/v1/dispatches", tags=["executor-v1"])
router.include_router(task_router)
router.include_router(finalization_router)
router.include_router(queue_router)


@router.post("/{dispatch_id}/claim")
def claim(dispatch_id: UUID, request: ClaimRequest, session: DispatchSession) -> LeaseResponse:
    dispatch = claim_dispatch(
        session,
        dispatch_id,
        request.claimant_id,
        DispatchIdentity(
            benchmark_id=request.benchmark_id,
            release_id=request.executor_release_id,
            artifact_uri=request.executor_artifact_uri,
            artifact_digest=request.executor_artifact_digest,
            protocol_version=request.executor_protocol_version,
        ),
    )
    assert dispatch.lease_expires_at is not None
    response = LeaseResponse(
        dispatch_id=dispatch.id,
        claimant_id=request.claimant_id,
        lease_expires_at=as_utc(dispatch.lease_expires_at),
        server_time=as_utc(database_now(session)),
    )
    session.commit()

    return response


@router.post("/{dispatch_id}/authority")
def authority(dispatch_id: UUID, request: DispatchRequest, session: DispatchSession) -> AuthorityResponse:
    return AuthorityResponse(current=dispatch_authority(session, dispatch_id, request.claimant_id))


@router.post("/{dispatch_id}/heartbeat")
def heartbeat(dispatch_id: UUID, request: DispatchRequest, session: DispatchSession) -> LeaseResponse:
    dispatch = heartbeat_dispatch(session, dispatch_id, request.claimant_id)
    assert dispatch.lease_expires_at is not None
    response = LeaseResponse(
        dispatch_id=dispatch.id,
        claimant_id=request.claimant_id,
        lease_expires_at=as_utc(dispatch.lease_expires_at),
        server_time=as_utc(database_now(session)),
    )
    session.commit()

    return response


@router.post("/{dispatch_id}/finish")
def finish(dispatch_id: UUID, request: DispatchRequest, session: DispatchSession) -> TerminalResponse:
    dispatch = complete_dispatch(session, dispatch_id, request.claimant_id)
    assert dispatch.finished_at is not None
    response = TerminalResponse(dispatch_id=dispatch.id, status="FINISHED", finished_at=as_utc(dispatch.finished_at))
    session.commit()

    return response


@router.post("/{dispatch_id}/fail")
def fail(dispatch_id: UUID, request: FailRequest, session: DispatchSession) -> TerminalResponse:
    dispatch = complete_dispatch(session, dispatch_id, request.claimant_id, error_message=request.error_message)
    assert dispatch.finished_at is not None
    response = TerminalResponse(dispatch_id=dispatch.id, status="FAILED", finished_at=as_utc(dispatch.finished_at))
    session.commit()

    return response


def _run_state_response(state: RunState, *, include_eval_resume_state: bool) -> RunStateResponse:
    benchmark = state.benchmark
    resources = benchmark.arguments.properties

    return RunStateResponse(
        current=state.current,
        run=RunInfo(
            benchmark_id=benchmark.id,
            org_id=state.org.id,
            org_name=state.org.name,
            benchmark_name=benchmark.name,
            agent_name=benchmark.arguments.contract.name,
            model=benchmark.arguments.contract.model,
            started_at=as_utc(benchmark.started_at),
            status=RunStatus(benchmark.status.value),
            aws_managed=benchmark.aws_managed,
            concurrency=benchmark.arguments.concurrency,
            queue_pool_id=benchmark.arguments.queue_pool_id,
            resources=RunResources(
                region=resources.region,
                s3_bucket=resources.s3_bucket,
                log_group=resources.log_group,
                log_retention_days=resources.log_retention_days,
            )
            if resources is not None
            else None,
        ),
        tasks=[
            TaskState(
                id=task.id,
                task_id=task_id,
                status=TaskStatus(task.status.value),
                started_at=as_utc(task.started_at),
                finished_at=as_utc(task.finished_at) if task.finished_at is not None else None,
                eval_resume_state=task.eval_resume_state if include_eval_resume_state else None,
            )
            for task_id, task in state.tasks
        ],
        task_counts={status: state.task_counts.get(status.value, 0) for status in TaskStatus},
    )


@router.post("/{dispatch_id}/run/initialize")
def initialize_run(dispatch_id: UUID, request: RunTasksRequest, session: DispatchSession) -> RunStateResponse:
    state = initialize_run_tasks(session, dispatch_id, request.claimant_id, request.task_ids)
    response = _run_state_response(state, include_eval_resume_state=request.include_eval_resume_state)
    session.commit()

    return response


@router.post("/{dispatch_id}/run/state")
def run_state(dispatch_id: UUID, request: RunTasksRequest, session: DispatchSession) -> RunStateResponse:
    state = read_run_state(session, dispatch_id, request.claimant_id, request.task_ids)

    return _run_state_response(state, include_eval_resume_state=request.include_eval_resume_state)
