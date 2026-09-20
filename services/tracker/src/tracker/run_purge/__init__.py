"""Explicit prepare, parent write fence, then purge/resume."""

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, ExecutorDispatch, Org, RunLifecycle
from tracker.lifecycle import (
    LifecycleConflict,
    OperationIdentity,
    RunScope,
    abandon_deletion_hold,
    require_owned_hold,
)
from tracker.lifecycle_evidence import (
    DispatchDrain,
    ExternalHostDrain,
    HostContractObservation,
    RunReport,
    validate_host_contract_observation,
    verify_drain,
)
from tracker.run_purge.contracts import (
    DispatchSnapshot,
    InspectionCheckpoint,
    PresentHeldInspection,
    PresentHistoryHeldInspection,
    PresentUnheldInspection,
    ProviderLocator,
    PurgeCheckpoint,
    PurgeInspection,
    PurgeInspectionRun,
    PurgePlan,
    PurgeReport,
    PurgeRun,
    RemovedInspection,
)
from tracker.run_purge.locking import OperationLock, exclusive_operation, verify_database_target
from tracker.run_purge.predecessor import acquire_deletion_hold, capture_completed_history, capture_predecessor
from tracker.run_purge.rows import delete_rows, inventory_rows, verify_foreign_keys, verify_rows_absent
from tracker.utils.run_control import apply_stop_benchmark


_ABANDONABLE_PHASES = {"held", "prepared"}


class PurgeBoundary(Protocol):
    async def validate(self, identity: OperationIdentity, run: PurgeRun, /) -> None: ...
    async def verify_fence(self, identity: OperationIdentity, run: PurgeRun, /) -> str: ...
    async def cleanup_sandboxes(self, run: PurgeRun, /) -> None: ...
    async def verify_absence(self, run: PurgeRun, /) -> None: ...
    async def purge_objects(self, identity: OperationIdentity, run: PurgeRun, /) -> None: ...
    async def purge_logs(self, run: PurgeRun, /) -> None: ...
    async def verify_storage_absence(self, identity: OperationIdentity, run: PurgeRun, /) -> None: ...


def build_plan(session: Session, identity: OperationIdentity) -> PurgePlan:
    """Read saved resources only; no defaults or provider writes."""
    verify_database_target(session, identity)
    runs: list[PurgeRun] = []
    for run_id in identity.run_ids:
        benchmark = session.get(Benchmark, run_id)
        if benchmark is None or benchmark.org_id != identity.org_id:
            raise LifecycleConflict("Run is absent or outside the requested org")
        resources = benchmark.arguments.properties
        locator = benchmark.arguments.sandbox_provider_secret_name
        if resources is None or locator is None:
            raise LifecycleConflict("Saved resources or sandbox provider locator are missing")
        scope = RunScope(run_id=run_id, original_resources=resources)
        relocation, abandoned, history = capture_predecessor(session, identity, scope)
        runs.append(
            PurgeRun(
                scope=scope,
                provider=ProviderLocator(kind=benchmark.arguments.sandbox_provider, secret_name=locator),
                released_relocation=relocation,
                abandoned_deletion=abandoned,
                completed_history=history,
            )
        )
    return PurgePlan(identity=identity, runs=tuple(runs))


def _unstarted_purge_guard(plan: PurgePlan) -> Callable[[Session, RunLifecycle], None]:
    digest = plan.digest()

    def verify_unstarted(_session: Session, record: RunLifecycle) -> None:
        if record.checkpoint_json is None:
            if record.phase != "held":
                raise LifecycleConflict("A purge that has started cannot be abandoned")

            return

        try:
            checkpoint = PurgeCheckpoint.model_validate_json(record.checkpoint_json)
        except ValidationError as error:
            raise LifecycleConflict("Missing or invalid purge checkpoint") from error

        if checkpoint.child_plan_sha256 != digest:
            raise LifecycleConflict("Purge checkpoint identity does not match")

        if (
            record.phase != checkpoint.phase
            or checkpoint.phase not in _ABANDONABLE_PHASES
            or checkpoint.rows
            or checkpoint.fence_policy_sha256 is not None
        ):
            raise LifecycleConflict("A purge that has started cannot be abandoned")

    return verify_unstarted


def _selected_runs(plan: PurgePlan, run_ids: tuple[UUID, ...]) -> tuple[PurgeRun, ...]:
    selected = tuple(run for run in plan.runs if run.scope.run_id in run_ids)
    if not run_ids or len(selected) != len(set(run_ids)):
        raise LifecycleConflict("Abandonment requires exact planned run identifiers")

    return selected


def abandon_runs(session: Session, plan: PurgePlan, run_ids: tuple[UUID, ...]) -> tuple[UUID, ...]:
    """Release named deletion holds of this exact operation before any purge step."""
    verify_database_target(session, plan.identity)
    selected = _selected_runs(plan, run_ids)
    verify_unstarted = _unstarted_purge_guard(plan)

    with exclusive_operation(session, plan.identity) as lock:
        for run in selected:
            abandon_deletion_hold(session, identity=plan.identity, scope=run.scope, verify_unstarted=verify_unstarted)
        lock.verify()
        session.commit()

    return tuple(run.scope.run_id for run in selected)


class PurgeOperator:
    def __init__(
        self,
        session: Session,
        plan: PurgePlan,
        boundary: PurgeBoundary,
        *,
        host_contract: HostContractObservation | None,
        external: tuple[ExternalHostDrain, ...] = (),
        external_evidence: tuple[bytes, ...] = (),
    ) -> None:
        verify_database_target(session, plan.identity)
        self.session = session
        self.plan = plan
        self.boundary = boundary
        self.host_contract = host_contract
        self.external = external
        self.external_evidence = external_evidence

    def _validate_saved_run(self, benchmark: Benchmark, run: PurgeRun) -> None:
        if run.expected_run_label is not None and benchmark.label != run.expected_run_label:
            raise LifecycleConflict("Saved run label changed")

        if (
            benchmark.org_id != self.plan.identity.org_id
            or benchmark.arguments.properties != run.scope.original_resources
            or benchmark.arguments.sandbox_provider != run.provider.kind
            or benchmark.arguments.sandbox_provider_secret_name != run.provider.secret_name
        ):
            raise LifecycleConflict("Saved run scope changed")

    def _lock(self, run: PurgeRun, /) -> tuple[Benchmark | None, RunLifecycle, PurgeCheckpoint]:
        benchmark = self.session.exec(
            select(Benchmark)
            .where(col(Benchmark.id) == run.scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        record = require_owned_hold(self.session, identity=self.plan.identity, scope=run.scope, purpose="deletion")
        try:
            checkpoint = PurgeCheckpoint.model_validate_json(record.checkpoint_json or "null")
        except ValidationError as error:
            raise LifecycleConflict("Missing or invalid purge checkpoint") from error
        if (
            record.released_at is not None
            or checkpoint.child_plan_sha256 != self.plan.digest()
            or checkpoint.provider != run.provider
            or checkpoint.released_relocation != run.released_relocation
            or checkpoint.completed_history != run.completed_history
            or record.phase != checkpoint.phase
        ):
            raise LifecycleConflict("Purge checkpoint identity does not match")
        if benchmark is not None:
            self._validate_saved_run(benchmark, run)
        if benchmark is not None and checkpoint.phase in {"rows_removed", "complete"}:
            raise LifecycleConflict("Removed run unexpectedly exists")
        if benchmark is None and checkpoint.phase not in {"rows_removed", "complete"}:
            raise LifecycleConflict("Absent run has no completed row checkpoint")
        if checkpoint.rows:
            rows = {row.table: row.ids for row in checkpoint.rows}
            if rows["benchmark"] != (run.scope.run_id,) or rows["executordispatch"] != tuple(
                item.dispatch_id for item in checkpoint.original_dispatches
            ):
                raise LifecycleConflict("Purge checkpoint row scope does not match original run")
        return benchmark, record, checkpoint

    def _save(self, run: PurgeRun, checkpoint: PurgeCheckpoint) -> None:
        _, record, current = self._lock(run)
        if current.child_plan_sha256 != checkpoint.child_plan_sha256:
            raise LifecycleConflict("Checkpoint changed")
        checkpoint = PurgeCheckpoint.model_validate_json(checkpoint.model_dump_json())
        record.checkpoint_json = checkpoint.model_dump_json()
        record.phase = checkpoint.phase
        self.session.add(record)
        self.session.flush()

    def _commit_checkpoint(
        self,
        run: PurgeRun,
        checkpoint: PurgeCheckpoint,
        lock: OperationLock,
        *,
        record: RunLifecycle | None = None,
    ) -> None:
        lock.verify()
        if record is None:
            self._save(run, checkpoint)
        else:
            # The run rows are deleted in this same transaction, so _lock can no longer read them back.
            record.checkpoint_json = checkpoint.model_dump_json()
            record.phase = checkpoint.phase
            self.session.add(record)

        self.session.commit()

    def _external(self, run: PurgeRun) -> tuple[ExternalHostDrain | None, bytes | None]:
        matches = [item for item in self.external if item.run_id == run.scope.run_id]
        if not matches:
            return None, None
        if len(matches) != 1:
            raise LifecycleConflict("Duplicate external drain evidence")
        external = matches[0]
        evidence = [
            item for item in self.external_evidence if hashlib.sha256(item).hexdigest() == external.evidence_sha256
        ]
        if len(evidence) != 1:
            raise LifecycleConflict("Exact external drain evidence bytes are required")
        return external, evidence[0]

    def _drain(self, run: PurgeRun, /) -> tuple[DispatchDrain, ...]:
        if self.host_contract is None:
            raise LifecycleConflict("Current host deployment contract is required for drain")
        external, evidence = self._external(run)
        drains = verify_drain(
            self.session,
            identity=self.plan.identity,
            scope=run.scope,
            purpose="deletion",
            host_contract=self.host_contract,
            external=external,
            external_evidence=evidence,
        )
        if any(drain.provenance == "pending" for drain in drains):
            raise LifecycleConflict("Process drain remains pending")
        return drains

    async def inspect(self, *, request_nonce: UUID) -> PurgeInspection:
        with self.session.no_autoflush, exclusive_operation(self.session, self.plan.identity) as lock:
            observations = tuple([await self._inspect_run(run) for run in self.plan.runs])
            if any(
                isinstance(item, RemovedInspection)
                or isinstance(item, PresentHeldInspection)
                and item.checkpoint.phase != "held"
                for item in observations
            ):
                self._validate_host_observation()

            lock.verify()

            return PurgeInspection(
                request_nonce=request_nonce,
                identity=self.plan.identity,
                child_plan_sha256=self.plan.digest(),
                observed_at=datetime.now(UTC),
                runs=observations,
            )

    async def _inspect_run(self, run: PurgeRun) -> PurgeInspectionRun:
        benchmark = self.session.exec(
            select(Benchmark)
            .where(col(Benchmark.id) == run.scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        record = self.session.exec(
            select(RunLifecycle)
            .where(col(RunLifecycle.run_id) == run.scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        if run.completed_history is not None and record is not None and record.purpose == "relocation":
            if benchmark is None:
                raise LifecycleConflict("Completed history row is absent")
            self._validate_saved_run(benchmark, run)
            observed = capture_completed_history(record, self.plan.identity, run.scope)
            if observed != run.completed_history:
                raise LifecycleConflict("Planned completed history predecessor changed")
            await self.boundary.validate(self.plan.identity, run)
            return PresentHistoryHeldInspection(
                scope=run.scope,
                provider=run.provider,
                expected_run_label=run.expected_run_label,
                current_label=benchmark.label,
                completed_history=observed,
            )

        if record is None or record.released_at is not None:
            if benchmark is None:
                raise LifecycleConflict("Absent run has no exact deletion checkpoint")

            self._validate_saved_run(benchmark, run)
            relocation, abandoned, history = capture_predecessor(self.session, self.plan.identity, run.scope)
            if (
                relocation != run.released_relocation
                or abandoned != run.abandoned_deletion
                or history != run.completed_history
            ):
                raise LifecycleConflict("Planned lifecycle predecessor changed")

            await self.boundary.validate(self.plan.identity, run)
            return PresentUnheldInspection(
                scope=run.scope,
                provider=run.provider,
                expected_run_label=run.expected_run_label,
                current_label=benchmark.label,
                released_relocation=relocation,
            )

        benchmark, record, checkpoint = self._lock(run)
        proof = InspectionCheckpoint(
            phase=checkpoint.phase,
            checkpoint_sha256=hashlib.sha256((record.checkpoint_json or "").encode()).hexdigest(),
            child_plan_sha256=checkpoint.child_plan_sha256,
        )
        if benchmark is None:
            self._validate_host_observation()
            if checkpoint.external_host_drain is not None:
                external, _ = self._external(run)
                if external != checkpoint.external_host_drain:
                    raise LifecycleConflict("Saved external drain evidence must be supplied on inspection")
            verify_foreign_keys(self.session)
            verify_rows_absent(self.session, checkpoint.rows)
            await self.boundary.validate(self.plan.identity, run)
            fence_digest = await self.boundary.verify_fence(self.plan.identity, run)
            if fence_digest != checkpoint.fence_policy_sha256:
                raise LifecycleConflict("Current fence differs from saved removal checkpoint")
            await self.boundary.verify_absence(run)
            await self.boundary.verify_storage_absence(self.plan.identity, run)
            return RemovedInspection(
                scope=run.scope,
                provider=run.provider,
                expected_run_label=run.expected_run_label,
                checkpoint=proof,
                fence_policy_sha256=fence_digest,
            )

        await self.boundary.validate(self.plan.identity, run)
        if checkpoint.phase != "held":
            drains = self._drain(run)
            if tuple(item.dispatch_id for item in drains) != tuple(
                item.dispatch_id for item in checkpoint.original_dispatches
            ):
                raise LifecycleConflict("Dispatch scope changed after hold")
            await self.boundary.verify_absence(run)

        return PresentHeldInspection(
            scope=run.scope,
            provider=run.provider,
            expected_run_label=run.expected_run_label,
            current_label=benchmark.label,
            checkpoint=proof,
        )

    async def prepare(self) -> PurgeReport:
        with exclusive_operation(self.session, self.plan.identity) as lock:
            return await self._prepare(lock)

    async def _prepare(self, lock: OperationLock) -> PurgeReport:
        for run in self.plan.runs:
            await self.boundary.validate(self.plan.identity, run)
            saved_run = self.session.exec(
                select(Benchmark)
                .where(col(Benchmark.id) == run.scope.run_id)
                .execution_options(populate_existing=True)
                .with_for_update()
            ).one_or_none()
            if saved_run is not None:
                self._validate_saved_run(saved_run, run)

            record = acquire_deletion_hold(self.session, self.plan.identity, run)
            saved_run = self.session.get(Benchmark, run.scope.run_id)
            if saved_run is not None:
                self._validate_saved_run(saved_run, run)
            if record.checkpoint_json is None:
                if record.phase != "held" or self.session.get(Benchmark, run.scope.run_id) is None:
                    raise LifecycleConflict("Missing purge checkpoint cannot be reconstructed")
                dispatches = self.session.exec(
                    select(ExecutorDispatch)
                    .where(col(ExecutorDispatch.benchmark_id) == run.scope.run_id)
                    .order_by(col(ExecutorDispatch.id))
                    .execution_options(populate_existing=True)
                    .with_for_update()
                ).all()
                checkpoint = PurgeCheckpoint(
                    child_plan_sha256=self.plan.digest(),
                    provider=run.provider,
                    released_relocation=run.released_relocation,
                    completed_history=run.completed_history,
                    original_dispatches=tuple(
                        DispatchSnapshot(
                            dispatch_id=dispatch.id,
                            status=dispatch.status,
                            started_at=dispatch.started_at,
                            process_exited_at=dispatch.process_exited_at,
                        )
                        for dispatch in dispatches
                    ),
                )
                record.checkpoint_json = checkpoint.model_dump_json()
                self.session.add(record)
            lock.verify()
            self.session.commit()
            benchmark, _, checkpoint = self._lock(run)
            org = self.session.get(Org, self.plan.identity.org_id)
            if benchmark is None or org is None:
                raise LifecycleConflict("Run or org missing during preparation")
            lock.verify()
            apply_stop_benchmark(benchmark, self.session, force=True, org=org)
            self.session.commit()
            lock.verify()
            self._lock(run)
            await self.boundary.cleanup_sandboxes(run)
            _, _, checkpoint = self._lock(run)
            drains = self._drain(run)
            self.session.rollback()
            await self.boundary.verify_absence(run)
            self._commit_checkpoint(
                run,
                checkpoint.model_copy(
                    update={
                        "phase": "prepared" if checkpoint.phase == "held" else checkpoint.phase,
                        "dispatch_drain": drains,
                        "external_host_drain": self._external(run)[0],
                    }
                ),
                lock,
            )
        return self.report(outcome="checked")

    async def purge(self) -> PurgeReport:
        with exclusive_operation(self.session, self.plan.identity) as lock:
            return await self._purge(lock)

    async def _purge(self, lock: OperationLock) -> PurgeReport:
        for run in self.plan.runs:
            benchmark, _, checkpoint = self._lock(run)
            if checkpoint.phase == "held":
                raise LifecycleConflict("Preparation is not complete")
            if benchmark is not None:
                drains = self._drain(run)
                original_ids = tuple(item.dispatch_id for item in checkpoint.original_dispatches)
                if tuple(item.dispatch_id for item in drains) != original_ids:
                    raise LifecycleConflict("Dispatch scope changed after hold")
            else:
                self._validate_host_observation()
            if benchmark is None and checkpoint.external_host_drain is not None:
                external, _ = self._external(run)
                if external != checkpoint.external_host_drain:
                    raise LifecycleConflict("Saved external drain evidence must be supplied on resume")
            verify_foreign_keys(self.session)
            self.session.rollback()
            await self.boundary.validate(self.plan.identity, run)
            fence_digest = await self.boundary.verify_fence(self.plan.identity, run)
            await self.boundary.verify_absence(run)
            if benchmark is None:
                await self._complete_run(run, lock)
                continue
            _, _, checkpoint = self._lock(run)
            rows = inventory_rows(self.session, run.scope.run_id, self.plan.identity.org_id)
            self._commit_checkpoint(
                run, checkpoint.model_copy(update={"rows": rows, "fence_policy_sha256": fence_digest}), lock
            )
            lock.verify()
            self._lock(run)
            await self.boundary.purge_objects(self.plan.identity, run)
            _, _, checkpoint = self._lock(run)
            self._commit_checkpoint(run, checkpoint.model_copy(update={"phase": "objects_removed"}), lock)
            await self.boundary.verify_fence(self.plan.identity, run)
            lock.verify()
            self._lock(run)
            await self.boundary.purge_logs(run)
            _, _, checkpoint = self._lock(run)
            self._commit_checkpoint(run, checkpoint.model_copy(update={"phase": "logs_removed"}), lock)
            await self.boundary.verify_fence(self.plan.identity, run)
            await self.boundary.verify_absence(run)
            await self.boundary.verify_storage_absence(self.plan.identity, run)
            _, record, checkpoint = self._lock(run)
            self._drain(run)
            current_rows = inventory_rows(self.session, run.scope.run_id, self.plan.identity.org_id)
            if current_rows != checkpoint.rows:
                raise LifecycleConflict("Row scope changed during purge")
            lock.verify()
            delete_rows(self.session, checkpoint.rows)
            verify_rows_absent(self.session, checkpoint.rows)
            self._commit_checkpoint(run, checkpoint.model_copy(update={"phase": "rows_removed"}), lock, record=record)
            await self._complete_run(run, lock)
        return self.report(outcome="checked")

    def _validate_host_observation(self) -> None:
        if self.host_contract is None:
            raise LifecycleConflict("Current host deployment contract is required for drain")

        validate_host_contract_observation(self.host_contract)

    async def _complete_run(self, run: PurgeRun, lock: OperationLock) -> None:
        fence_digest = await self.boundary.verify_fence(self.plan.identity, run)
        await self.boundary.verify_absence(run)
        await self.boundary.verify_storage_absence(self.plan.identity, run)
        _, _, checkpoint = self._lock(run)
        self._validate_host_observation()
        verify_rows_absent(self.session, checkpoint.rows)
        self._commit_checkpoint(
            run, checkpoint.model_copy(update={"phase": "complete", "fence_policy_sha256": fence_digest}), lock
        )

    def report(self, *, outcome: Literal["checked", "incomplete"] = "incomplete") -> PurgeReport:
        runs: list[RunReport] = []
        for run in self.plan.runs:
            _, _, checkpoint = self._lock(run)
            runs.append(
                RunReport(
                    scope=run.scope,
                    phase=checkpoint.phase,
                    dispatch_drain=checkpoint.dispatch_drain,
                    external_host_drain=checkpoint.external_host_drain,
                )
            )
        self.session.rollback()
        if outcome == "checked":
            self._validate_host_observation()

        return PurgeReport(
            child_plan_sha256=self.plan.digest(),
            outcome=outcome,
            identity=self.plan.identity,
            purpose="deletion",
            observed_at=datetime.now(UTC),
            host_contract=self.host_contract,
            runs=tuple(runs),
        )
