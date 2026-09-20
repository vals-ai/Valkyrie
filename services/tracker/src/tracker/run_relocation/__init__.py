"""Same-account terminal run relocation. Provider calls never copy artifacts."""

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import text
from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorDispatch,
    RunLifecycle,
    Task,
    TaskStatus,
)
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, require_owned_hold
from tracker.lifecycle_completion import (
    RelocationCheckpoint,
    RelocationPredecessor,
    acquire_successor_hold,
    capture_predecessor,
)
from tracker.lifecycle_evidence import (
    ExternalHostDrain,
    HostContractObservation,
    classify_dispatch,
    validate_host_contract_observation,
    verify_drain,
)
from tracker.run_purge.contracts import ProviderLocator, PurgeRun
from tracker.run_purge.locking import OperationLock, database_target, exclusive_operation
from tracker.storage_migration_exchange import (
    AWSResources,
    CopiedObject,
    DestinationVersion,
    DispatchObservation,
    ExecutionReference,
    RelocationRun,
    RunObservation,
    TrackerRequest,
    TrackerResponse,
    canonical_digest,
)


class RelocationBoundary(Protocol):
    async def validate_source(self, identity: OperationIdentity, run: PurgeRun, /) -> None: ...
    async def validate(self, identity: OperationIdentity, run: PurgeRun, /) -> None: ...
    async def verify_absence(self, run: PurgeRun, /) -> None: ...
    async def cleanup_sandboxes(self, run: PurgeRun, /) -> None: ...
    async def verify_objects(
        self,
        request: TrackerRequest,
        run: RelocationRun,
        /,
        *,
        source_removed: bool = False,
        source_partial: bool = False,
        reuse_verified: bool = False,
    ) -> None: ...
    async def execution_references(
        self, arguments: dict[str, Any], request: TrackerRequest, retired_buckets: frozenset[str], /
    ) -> tuple[ExecutionReference, ...]: ...


def stored_arguments(session: Session, run_id: UUID) -> dict[str, Any]:
    value = (
        session.connection().execute(text("SELECT arguments FROM benchmark WHERE id=:id"), {"id": run_id}).scalar_one()
    )
    if not isinstance(value, dict):
        raise LifecycleConflict("Stored execution arguments are not an object")
    return cast(dict[str, Any], value)


def execution_digest(arguments: dict[str, Any]) -> str:
    value = json.loads(json.dumps(arguments))
    value["properties"].pop("s3_bucket", None)
    return canonical_digest(value)


def recorded_source_bucket(record: RunLifecycle | None, arguments: dict[str, Any]) -> str | None:
    """The bucket a relocation empties, which after the location commit is no longer the saved one.

    A hold-only run never changes its location, so its recorded source is still the live
    bucket and nothing is retired.
    """
    saved = cast(str, arguments["properties"]["s3_bucket"])
    if record is None:
        return saved

    recorded = RunScope.model_validate_json(record.scope_json).original_resources.s3_bucket

    return None if recorded == saved else recorded


def retired_source_buckets(request: TrackerRequest, source_buckets: Sequence[str] = ()) -> frozenset[str]:
    """Every bucket the parent empties for this operation, identical in every action."""
    if request.plan is not None:
        return frozenset(
            run.scope.original_resources.s3_bucket for run in request.plan.runs if run.location_policy != "hold_only"
        )

    return frozenset(source_buckets)


def result_locator_references(
    results: Sequence[tuple[UUID, Any]], retired_buckets: frozenset[str]
) -> tuple[ExecutionReference, ...]:
    """Report saved task-result locators into a retired bucket; the tool never rewrites them."""
    prefixes = tuple(f"s3://{bucket}/" for bucket in sorted(retired_buckets))
    references: list[ExecutionReference] = []

    def scan(value: Any, pointer: str) -> None:
        if isinstance(value, dict):
            for key, child in cast(dict[str, Any], value).items():
                scan(child, pointer + "/" + key.replace("~", "~0").replace("/", "~1"))
        elif isinstance(value, list):
            for index, child in enumerate(cast(list[Any], value)):
                scan(child, pointer + "/" + str(index))
        elif isinstance(value, str) and value.startswith(prefixes):
            references.append(ExecutionReference(pointer=pointer, value_sha256=canonical_digest(value), kind="unknown"))

    for result_id, result in results:
        scan(result, f"/evaluation_result/{result_id}")
    return tuple(references)


MAXIMUM_EVIDENCE_BYTES = 64 * 1024 * 1024


def evidence_digest(path: Path) -> str:
    """Hash a supplied evidence file without holding it, so unnamed files cost no memory."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def bounded_evidence(path: Path) -> bytes:
    """Read the one named attestation, refusing a size no operator attestation can have."""
    with path.open("rb") as handle:
        content = handle.read(MAXIMUM_EVIDENCE_BYTES + 1)
    if len(content) > MAXIMUM_EVIDENCE_BYTES:
        raise LifecycleConflict("External drain evidence exceeds the bounded operator input size")

    return content


class RelocationOperator:
    def __init__(self, session: Session, boundary: RelocationBoundary) -> None:
        self.session = session
        self.boundary = boundary
        self.evidence_digests: dict[str, str] = {}

    def _run(self, request: TrackerRequest, run_id: UUID) -> tuple[Benchmark, dict[str, Any]]:
        benchmark = self.session.exec(
            select(Benchmark)
            .where(col(Benchmark.id) == run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        if benchmark is None or benchmark.org_id != request.org_id:
            raise LifecycleConflict("Run is absent or outside requested organization")
        # Archive relocation needs an exact VersionId mapping from the separate production task.
        if getattr(benchmark, "log_history", None) is not None:
            raise LifecycleConflict("Version-pinned log history requires the production archive relocation adapter")
        return benchmark, stored_arguments(self.session, run_id)

    def _provider_run(self, arguments: dict[str, Any], run_id: UUID) -> PurgeRun:
        locator = arguments.get("sandbox_provider_secret_name")
        if not locator:
            raise LifecycleConflict("Saved sandbox provider locator is missing")
        return PurgeRun(
            scope=RunScope(run_id=run_id, original_resources=arguments["properties"]),
            provider=ProviderLocator(kind=arguments["sandbox_provider"], secret_name=locator),
        )

    def _checkpoint(self, identity: OperationIdentity, run: RelocationRun) -> tuple[RunLifecycle, RelocationCheckpoint]:
        scope = RunScope.model_validate(run.scope.model_dump(mode="json"))
        record = require_owned_hold(self.session, identity=identity, scope=scope, purpose="relocation")
        checkpoint = RelocationCheckpoint.model_validate_json(record.checkpoint_json or "null")
        if (
            checkpoint.identity_sha256 != canonical_digest(identity.model_dump(mode="json"))
            or checkpoint.scope_sha256 != canonical_digest(scope.model_dump(mode="json"))
            or checkpoint.phase != record.phase
            or checkpoint.execution_arguments_sha256 != run.execution_arguments_sha256
            or checkpoint.execution_policy != run.execution_policy
            or checkpoint.destination_resources
            != RunScope.model_validate(
                {"run_id": str(scope.run_id), "original_resources": run.destination_resources.model_dump(mode="json")}
            ).original_resources
            or (record.released_at is not None) != (record.phase == "released")
        ):
            raise LifecycleConflict("Durable relocation checkpoint does not match")
        return record, checkpoint

    def _dispatches(self, run_id: UUID) -> list[ExecutorDispatch]:
        return list(
            self.session.exec(
                select(ExecutorDispatch)
                .where(col(ExecutorDispatch.benchmark_id) == run_id)
                .order_by(col(ExecutorDispatch.id))
                .execution_options(populate_existing=True)
                .with_for_update()
            ).all()
        )

    def _pending(self, run_id: UUID) -> tuple[UUID, ...]:
        tasks = self.session.exec(
            select(Task)
            .where(col(Task.benchmark) == run_id)
            .order_by(col(Task.id))
            .execution_options(populate_existing=True)
        ).all()
        return tuple(
            task.id
            for task in tasks
            if task.status not in {TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED}
            or task.eval_resume_state is not None
        )

    def _result_references(self, run_id: UUID, retired_buckets: frozenset[str]) -> tuple[ExecutionReference, ...]:
        if not retired_buckets:
            return ()

        results = self.session.exec(
            select(EvaluationResult)
            .join(Task, onclause=col(EvaluationResult.task) == col(Task.id))
            .where(col(Task.benchmark) == run_id)
            .order_by(col(EvaluationResult.id))
        ).all()
        return result_locator_references([(item.id, item.result) for item in results], retired_buckets)

    def _evidence_file(self, request: TrackerRequest, supplied: ExternalHostDrain) -> Path:
        for path in request.external_evidence_files:
            if path not in self.evidence_digests:
                self.evidence_digests[path] = evidence_digest(Path(path))

        matched = [
            path for path in request.external_evidence_files if self.evidence_digests[path] == supplied.evidence_sha256
        ]
        if len(matched) != 1:
            raise LifecycleConflict("Exact external drain evidence file is required")

        return Path(matched[0])

    def _host_contract(self, request: TrackerRequest) -> HostContractObservation:
        if request.host_contract is None:
            raise LifecycleConflict("Current named host contract observation is required")

        host = HostContractObservation.model_validate(request.host_contract.model_dump(mode="json"))
        validate_host_contract_observation(host)
        return host

    def _drain(
        self, request: TrackerRequest, identity: OperationIdentity, run: RelocationRun, record: RunLifecycle
    ) -> tuple[DispatchObservation, ...]:
        host = self._host_contract(request)
        dispatches = self._dispatches(run.scope.run_id)
        if any(dispatch.status.value in {"QUEUED", "RUNNING"} for dispatch in dispatches):
            raise LifecycleConflict("Active dispatch requires terminal cleanup before relocation")
        external = [item for item in request.external_host_drains if item.run_id == run.scope.run_id]
        if len(external) > 1:
            raise LifecycleConflict("Duplicate host drain evidence")
        supplied = ExternalHostDrain.model_validate(external[0].model_dump(mode="json")) if external else None
        evidence_file = None if supplied is None else self._evidence_file(request, supplied)
        if record.released_at is None:
            drains = verify_drain(
                self.session,
                identity=identity,
                scope=RunScope.model_validate(run.scope.model_dump(mode="json")),
                purpose="relocation",
                host_contract=host,
                external=supplied,
                external_evidence=None if evidence_file is None else bounded_evidence(evidence_file),
            )
        else:
            # A released hold can no longer establish that queued work cannot start.
            drains = tuple(classify_dispatch(dispatch, host_contract=host) for dispatch in dispatches)
            if supplied is not None:
                acquired_at = (
                    record.acquired_at.replace(tzinfo=UTC) if record.acquired_at.tzinfo is None else record.acquired_at
                )
                legacy_ids = tuple(
                    dispatch.id
                    for dispatch, drain in zip(dispatches, drains, strict=True)
                    if drain.provenance == "pending" and dispatch.started_at is not None
                )
                if (
                    supplied.identity != identity
                    or supplied.run_id != run.scope.run_id
                    or supplied.hold_acquired_at != acquired_at
                    or not acquired_at <= supplied.observed_at <= datetime.now(UTC)
                    or supplied.dispatch_ids != legacy_ids
                    or not legacy_ids
                    or supplied.host_inventory != host.host_inventory
                    or supplied.deployed_host_contract != host.contract
                    or not set(legacy_ids).issubset(host.legacy_dispatch_ids)
                ):
                    raise LifecycleConflict("Released legacy host evidence does not match original hold")
                for dispatch in dispatches:
                    started_at = dispatch.started_at
                    if dispatch.id in legacy_ids and started_at is not None:
                        started_at = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at
                        if started_at >= host.acknowledgement_required_since:
                            raise LifecycleConflict(
                                "External evidence cannot replace current host exit acknowledgement"
                            )
                drains = tuple(
                    drain.model_copy(update={"provenance": "externally_confirmed_host_drain"})
                    if drain.dispatch_id in legacy_ids
                    else drain
                    for drain in drains
                )
            if any(item.provenance in {"pending", "held_unclaimed"} for item in drains):
                raise LifecycleConflict("Released run has no positive current dispatch drain")
        names = {
            "host_process_exit": "process_exited",
            "verified_finished_contract": "finished_current_host",
            "held_unclaimed": "unclaimed_held",
            "externally_confirmed_host_drain": "externally_confirmed_host_drain",
        }
        if any(item.provenance == "pending" for item in drains):
            raise LifecycleConflict("Process exit remains pending")
        return tuple(
            DispatchObservation.model_validate(
                {
                    "dispatch_id": item.id,
                    "status": item.status.value,
                    "started_at": item.started_at,
                    "process_exited_at": item.process_exited_at,
                    "evidence": names[drain.provenance],
                    "external_evidence_sha256": supplied.evidence_sha256
                    if supplied and drain.provenance == "externally_confirmed_host_drain"
                    else None,
                }
            )
            for item, drain in zip(dispatches, drains, strict=True)
        )

    def _validate_run(
        self,
        benchmark: Benchmark,
        arguments: dict[str, Any],
        run: RelocationRun,
        checkpoint: RelocationCheckpoint | None,
    ) -> None:
        expected = (
            run.destination_resources
            if checkpoint and checkpoint.phase in {"relocated", "released", "relocated_history_only"}
            else run.scope.original_resources
        )
        if (
            benchmark.label != run.expected_label
            or arguments.get("properties") != expected.model_dump(mode="json")
            or execution_digest(arguments) != run.execution_arguments_sha256
        ):
            raise LifecycleConflict("Saved resources, label or full execution arguments changed")
        if benchmark.status not in {BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED}:
            raise LifecycleConflict("Relocation requires a terminal run")

    def _save(self, record: RunLifecycle, checkpoint: RelocationCheckpoint) -> None:
        checkpoint = RelocationCheckpoint.model_validate_json(checkpoint.model_dump_json())
        record.phase = checkpoint.phase
        record.checkpoint_json = checkpoint.model_dump_json()
        self.session.add(record)
        self.session.flush()

    def _commit_checkpoint(self, record: RunLifecycle, checkpoint: RelocationCheckpoint, lock: OperationLock) -> None:
        lock.verify()
        self._save(record, checkpoint)
        self.session.commit()

    async def execute(self, request: TrackerRequest) -> TrackerResponse:
        if request.action != "inventory" or request.host_contract is not None:
            self._host_contract(request)

        if request.source_aws_account_id != request.destination_aws_account_id:
            raise LifecycleConflict("Cross-account relocation requires the paired transfer operator")
        if (
            not request.run_ids
            or request.run_ids != tuple(sorted(set(request.run_ids), key=str))
            or database_target(self.session) != request.database_target
        ):
            raise LifecycleConflict("Run scope or actual database target differs")
        if request.action == "inventory":
            return await self._inventory(request)
        if request.plan is None:
            raise LifecycleConflict("Immutable child plan is required")
        identity = OperationIdentity.model_validate(request.plan.identity.model_dump(mode="json"))
        for field in (
            "github_owner_id",
            "org_id",
            "source_aws_account_id",
            "destination_aws_account_id",
            "region",
            "environment",
            "database_target",
            "run_ids",
        ):
            if getattr(request, field) != getattr(identity, field):
                raise LifecycleConflict("Request differs from immutable operation identity")
        with exclusive_operation(self.session, identity) as lock:
            observations: list[RunObservation] = []
            verified_copies: list[CopiedObject] = []
            verified_history: list[DestinationVersion] = []
            for run in request.plan.runs:
                observation, effective_request = await self._execute_run(request, identity, run, lock)
                observations.append(observation)
                verified_copies.extend(
                    item for item in effective_request.copied_objects if item.run_id == run.scope.run_id
                )
                verified_history.extend(
                    item for item in effective_request.destination_versions if item.run_id == run.scope.run_id
                )
            return TrackerResponse(
                nonce=request.nonce,
                action=request.action,
                child_plan_sha256=request.plan.sha256,
                copied_objects_sha256=canonical_digest(
                    [item.model_dump(mode="json") for item in (request.copied_objects or verified_copies)]
                ),
                destination_versions_sha256=canonical_digest(
                    [item.model_dump(mode="json") for item in (request.destination_versions or verified_history)]
                ),
                completion_sha256=request.completion_sha256,
                runs=tuple(observations),
            )

    async def _inventory(self, request: TrackerRequest) -> TrackerResponse:
        scoped: list[tuple[Benchmark, dict[str, Any], RelocationPredecessor | None, str | None]] = []
        for run_id in request.run_ids:
            benchmark, arguments = self._run(request, run_id)
            predecessor = None
            record = self.session.get(RunLifecycle, run_id)
            if record is not None:
                identity = OperationIdentity.model_validate_json(record.identity_json)
                for field in (
                    "github_owner_id",
                    "org_id",
                    "source_aws_account_id",
                    "destination_aws_account_id",
                    "region",
                    "environment",
                    "database_target",
                ):
                    if getattr(identity, field) != getattr(request, field):
                        raise LifecycleConflict("Prior lifecycle owner differs")
                predecessor = capture_predecessor(
                    record, identity, RunScope(run_id=run_id, original_resources=arguments["properties"])
                )
            scoped.append((benchmark, arguments, predecessor, recorded_source_bucket(record, arguments)))

        retired = retired_source_buckets(request, [bucket for _, _, _, bucket in scoped if bucket is not None])
        observations: list[RunObservation] = []
        for benchmark, arguments, predecessor, _ in scoped:
            references = await self.boundary.execution_references(
                arguments, request, retired
            ) + self._result_references(benchmark.id, retired)
            observations.append(
                self._observation(request, benchmark, arguments, references=references, predecessor=predecessor)
            )
        self.session.rollback()
        return TrackerResponse(nonce=request.nonce, action=request.action, runs=tuple(observations))

    async def _execute_run(
        self, request: TrackerRequest, identity: OperationIdentity, run: RelocationRun, lock: OperationLock
    ) -> tuple[RunObservation, TrackerRequest]:
        assert request.plan is not None
        self._host_contract(request)
        benchmark, arguments = self._run(request, run.scope.run_id)
        provider_run = self._provider_run(arguments, run.scope.run_id).model_copy(
            update={"scope": RunScope.model_validate(run.scope.model_dump(mode="json"))}
        )
        if request.action == "prepare":
            existing = self.session.get(RunLifecycle, run.scope.run_id)
            if existing is None or existing.identity_json != identity.model_dump_json():
                self._validate_run(benchmark, arguments, run, None)
                await self.boundary.validate_source(identity, provider_run)
                lock.verify()
                destination = provider_run.model_copy(
                    update={
                        "scope": RunScope.model_validate(
                            {
                                "run_id": str(run.scope.run_id),
                                "original_resources": run.destination_resources.model_dump(mode="json"),
                            }
                        )
                    }
                )
                await self.boundary.validate(identity, destination)
                lock.verify()
                record = acquire_successor_hold(
                    self.session,
                    identity=identity,
                    scope=RunScope.model_validate(run.scope.model_dump(mode="json")),
                    purpose="relocation",
                    predecessor=None
                    if run.predecessor is None
                    else RelocationPredecessor.model_validate(run.predecessor.model_dump(mode="json")),
                )
                checkpoint = RelocationCheckpoint(
                    identity_sha256=canonical_digest(identity.model_dump(mode="json")),
                    scope_sha256=canonical_digest(run.scope.model_dump(mode="json")),
                    child_plan_sha256=request.plan.sha256,
                    execution_arguments_sha256=run.execution_arguments_sha256,
                    execution_policy=run.execution_policy,
                    destination_resources=destination.scope.original_resources,
                    dispatch_ids=tuple(item.id for item in self._dispatches(run.scope.run_id)),
                )
                self._commit_checkpoint(record, checkpoint, lock)
            benchmark, arguments = self._run(request, run.scope.run_id)
            _, checkpoint = self._checkpoint(identity, run)
            if checkpoint.child_plan_sha256 != request.plan.sha256:
                raise LifecycleConflict("Child plan changed before provider cleanup")
            self._validate_run(benchmark, arguments, run, checkpoint)
            await self.boundary.cleanup_sandboxes(provider_run)
            lock.verify()
        benchmark, arguments = self._run(request, run.scope.run_id)
        record, checkpoint = self._checkpoint(identity, run)
        if checkpoint.child_plan_sha256 != request.plan.sha256:
            raise LifecycleConflict("Child plan changed")
        self._validate_run(benchmark, arguments, run, checkpoint)
        if self._pending(run.scope.run_id):
            raise LifecycleConflict("Pending, queued or deferred tasks remain")
        dispatches = self._drain(request, identity, run, record)
        if tuple(item.dispatch_id for item in dispatches) != checkpoint.dispatch_ids:
            raise LifecycleConflict("Dispatch scope changed after hold")
        await self.boundary.validate_source(identity, provider_run)
        lock.verify()
        destination = provider_run.model_copy(
            update={"scope": RunScope(run_id=run.scope.run_id, original_resources=checkpoint.destination_resources)}
        )
        await self.boundary.validate(identity, destination)
        lock.verify()
        await self.boundary.verify_absence(provider_run)
        lock.verify()
        retired = retired_source_buckets(request)
        references = await self.boundary.execution_references(arguments, request, retired) + self._result_references(
            run.scope.run_id, retired
        )
        lock.verify()
        if (
            not request.copied_objects
            and not request.destination_versions
            and checkpoint.phase in {"relocated", "released", "relocated_history_only"}
        ):
            request = request.model_copy(
                update={
                    "copied_objects": checkpoint.copied_objects,
                    "destination_versions": checkpoint.destination_versions,
                }
            )
        if request.action == "prepare" and checkpoint.phase == "held":
            checkpoint = checkpoint.model_copy(update={"phase": "prepared"})
            self._commit_checkpoint(record, checkpoint, lock)
        elif (
            request.action in {"relocate", "release"}
            or request.copied_objects
            or request.destination_versions
            or checkpoint.phase in {"relocated", "released", "relocated_history_only"}
        ):
            if checkpoint.phase == "held":
                raise LifecycleConflict("Preparation is incomplete")
            copies_digest = canonical_digest([item.model_dump(mode="json") for item in request.copied_objects])
            history_digest = canonical_digest([item.model_dump(mode="json") for item in request.destination_versions])
            if checkpoint.copied_objects_sha256 is not None and (
                checkpoint.copied_objects_sha256 != copies_digest
                or checkpoint.destination_versions_sha256 != history_digest
            ):
                raise LifecycleConflict("Copied object or destination history proof changed")
            await self.boundary.verify_objects(
                request,
                run,
                source_removed=request.action == "release",
                source_partial=checkpoint.phase in {"relocated", "released", "relocated_history_only"},
            )
            lock.verify()
            if request.action == "relocate" and checkpoint.phase == "prepared":
                # Direct SQL preserves excluded fields and the exact stored JSON value shape.
                if run.location_policy == "relocate":
                    arguments["properties"]["s3_bucket"] = run.destination_resources.s3_bucket
                    self.session.connection().execute(
                        text("UPDATE benchmark SET arguments=CAST(:arguments AS JSON) WHERE id=:id"),
                        {"arguments": json.dumps(arguments), "id": run.scope.run_id},
                    )
                checkpoint = checkpoint.model_copy(
                    update={
                        "phase": "relocated",
                        "copied_objects_sha256": copies_digest,
                        "destination_versions_sha256": history_digest,
                        "copied_objects": request.copied_objects,
                        "destination_versions": request.destination_versions,
                    }
                )
                self._commit_checkpoint(record, checkpoint, lock)
                benchmark, arguments = self._run(request, run.scope.run_id)
                record, checkpoint = self._checkpoint(identity, run)
                self._validate_run(benchmark, arguments, run, checkpoint)
                await self.boundary.verify_objects(request, run, source_partial=True, reuse_verified=True)
                lock.verify()
            elif request.action == "release":
                if (
                    checkpoint.phase not in {"relocated", "released", "relocated_history_only"}
                    or request.completion_sha256 is None
                ):
                    raise LifecycleConflict("Relocation and parent completion are required")
                if (
                    checkpoint.parent_completion_sha256 is not None
                    and checkpoint.parent_completion_sha256 != request.completion_sha256
                ):
                    raise LifecycleConflict("Parent completion proof changed")
                if run.execution_policy == "portable" and (
                    not references
                    or any(item.kind not in {"builtin_dataset", "retained_s3_object"} for item in references)
                ):
                    raise LifecycleConflict("Portable execution references remain unresolved")
                if run.execution_policy == "portable" and any(
                    dispatch.evidence == "unclaimed_held" for dispatch in dispatches
                ):
                    raise LifecycleConflict(
                        "Portable release requires positive process absence without relying on the hold"
                    )
                phase = "released" if run.execution_policy == "portable" else "relocated_history_only"
                if phase == "released" and record.released_at is None:
                    record.released_at = (
                        self.session.connection().execute(text("SELECT current_timestamp")).scalar_one()
                    )
                    self.session.add(record)

                record.phase = phase
                receipt = checkpoint.receipt or self._observation(
                    request, benchmark, arguments, record=record, dispatches=dispatches, references=references
                )
                checkpoint = checkpoint.model_copy(
                    update={
                        "phase": phase,
                        "parent_completion_sha256": request.completion_sha256,
                        "receipt": receipt,
                    }
                )
                self._commit_checkpoint(record, checkpoint, lock)

                return receipt, request
        benchmark, arguments = self._run(request, run.scope.run_id)
        record, checkpoint = self._checkpoint(identity, run)
        self._validate_run(benchmark, arguments, run, checkpoint)
        return (
            self._observation(
                request, benchmark, arguments, record=record, dispatches=dispatches, references=references
            ),
            request,
        )

    def _observation(
        self,
        request: TrackerRequest,
        benchmark: Benchmark,
        arguments: dict[str, Any],
        *,
        record: RunLifecycle | None = None,
        dispatches: tuple[DispatchObservation, ...] = (),
        references: tuple[ExecutionReference, ...] = (),
        predecessor: RelocationPredecessor | None = None,
    ) -> RunObservation:
        return RunObservation.model_validate(
            {
                "run_id": benchmark.id,
                "org_id": benchmark.org_id,
                "label": benchmark.label,
                "model_sha256": canonical_digest(arguments["contract"].get("model")),
                "dataset_sha256": canonical_digest(arguments.get("dataset")),
                "benchmark_name": benchmark.name,
                "predecessor": None if predecessor is None else predecessor.model_dump(mode="json"),
                "execution_arguments_sha256": execution_digest(arguments),
                "execution_references": references,
                "resources": AWSResources.model_validate(arguments["properties"]),
                "status": benchmark.status.value,
                "hold_identity": None if record is None else json.loads(record.identity_json),
                "hold_scope": None if record is None else json.loads(record.scope_json),
                "hold_phase": None if record is None else record.phase,
                "hold_purpose": None if record is None else record.purpose,
                "hold_released_at": None if record is None else record.released_at,
                "deployed_host_contract_sha256": None
                if request.host_contract is None or record is None
                else canonical_digest(request.host_contract.model_dump(mode="json")),
                "dispatches": dispatches,
                "sandbox_ids": (),
                "pending_task_ids": self._pending(benchmark.id),
                "observed_at": datetime.now(UTC),
            }
        )
