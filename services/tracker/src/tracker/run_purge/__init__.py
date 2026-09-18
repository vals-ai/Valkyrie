"""Explicit prepare, parent write fence, then purge/resume."""

import hashlib
from datetime import UTC, datetime
from typing import Literal, Protocol

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, ExecutorDispatch, Org, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, require_owned_hold
from tracker.lifecycle_evidence import (
    DispatchDrain,
    ExternalHostDrain,
    HostContractObservation,
    RunReport,
    verify_drain,
)
from tracker.run_purge.contracts import (
    DispatchSnapshot,
    ProviderLocator,
    PurgeCheckpoint,
    PurgePlan,
    PurgeReport,
    PurgeRun,
)
from tracker.run_purge.locking import exclusive_operation, verify_database_target
from tracker.run_purge.predecessor import acquire_deletion_hold, capture_predecessor
from tracker.run_purge.rows import delete_rows, inventory_rows, verify_foreign_keys, verify_rows_absent
from tracker.utils.run_control import apply_stop_benchmark


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
        runs.append(
            PurgeRun(
                scope=RunScope(run_id=run_id, original_resources=resources),
                provider=ProviderLocator(kind=benchmark.arguments.sandbox_provider, secret_name=locator),
                released_relocation=capture_predecessor(session, identity, run_id),
            )
        )
    return PurgePlan(identity=identity, runs=tuple(runs))


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

    async def prepare(self) -> PurgeReport:
        with exclusive_operation(self.session, self.plan.identity):
            return await self._prepare()

    async def _prepare(self) -> PurgeReport:
        for run in self.plan.runs:
            await self.boundary.validate(self.plan.identity, run)
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
            self.session.commit()
            benchmark, _, checkpoint = self._lock(run)
            org = self.session.get(Org, self.plan.identity.org_id)
            if benchmark is None or org is None:
                raise LifecycleConflict("Run or org missing during preparation")
            apply_stop_benchmark(benchmark, self.session, force=True, org=org)
            self.session.commit()
            await self.boundary.cleanup_sandboxes(run)
            _, _, checkpoint = self._lock(run)
            drains = self._drain(run)
            self.session.rollback()
            await self.boundary.verify_absence(run)
            self._save(
                run,
                checkpoint.model_copy(
                    update={
                        "phase": "prepared" if checkpoint.phase == "held" else checkpoint.phase,
                        "dispatch_drain": drains,
                        "external_host_drain": self._external(run)[0],
                    }
                ),
            )
            self.session.commit()
        return self.report(outcome="checked")

    async def purge(self) -> PurgeReport:
        with exclusive_operation(self.session, self.plan.identity):
            return await self._purge()

    async def _purge(self) -> PurgeReport:
        for run in self.plan.runs:
            benchmark, _, checkpoint = self._lock(run)
            if checkpoint.phase == "held":
                raise LifecycleConflict("Preparation is not complete")
            if benchmark is not None:
                drains = self._drain(run)
                original_ids = tuple(item.dispatch_id for item in checkpoint.original_dispatches)
                if tuple(item.dispatch_id for item in drains) != original_ids:
                    raise LifecycleConflict("Dispatch scope changed after hold")
            elif (
                self.host_contract is None
                or any(item.provenance == "pending" for item in checkpoint.dispatch_drain)
                or tuple(item.dispatch_id for item in checkpoint.dispatch_drain)
                != tuple(item.dispatch_id for item in checkpoint.original_dispatches)
            ):
                raise LifecycleConflict("Durable drain checkpoint is incomplete")
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
                await self.boundary.verify_storage_absence(self.plan.identity, run)
                _, _, checkpoint = self._lock(run)
                verify_rows_absent(self.session, checkpoint.rows)
                self._save(
                    run, checkpoint.model_copy(update={"phase": "complete", "fence_policy_sha256": fence_digest})
                )
                self.session.commit()
                continue
            _, _, checkpoint = self._lock(run)
            rows = inventory_rows(self.session, run.scope.run_id, self.plan.identity.org_id)
            self._save(run, checkpoint.model_copy(update={"rows": rows, "fence_policy_sha256": fence_digest}))
            self.session.commit()
            await self.boundary.purge_objects(self.plan.identity, run)
            _, _, checkpoint = self._lock(run)
            self._save(run, checkpoint.model_copy(update={"phase": "objects_removed"}))
            self.session.commit()
            await self.boundary.verify_fence(self.plan.identity, run)
            await self.boundary.purge_logs(run)
            _, _, checkpoint = self._lock(run)
            self._save(run, checkpoint.model_copy(update={"phase": "logs_removed"}))
            self.session.commit()
            await self.boundary.verify_fence(self.plan.identity, run)
            await self.boundary.verify_absence(run)
            await self.boundary.verify_storage_absence(self.plan.identity, run)
            _, record, checkpoint = self._lock(run)
            self._drain(run)
            current_rows = inventory_rows(self.session, run.scope.run_id, self.plan.identity.org_id)
            if current_rows != checkpoint.rows:
                raise LifecycleConflict("Row scope changed during purge")
            delete_rows(self.session, checkpoint.rows)
            verify_rows_absent(self.session, checkpoint.rows)
            record.checkpoint_json = checkpoint.model_copy(update={"phase": "rows_removed"}).model_dump_json()
            record.phase = "rows_removed"
            self.session.add(record)
            self.session.commit()
            await self.boundary.verify_absence(run)
            await self.boundary.verify_storage_absence(self.plan.identity, run)
            _, _, checkpoint = self._lock(run)
            verify_rows_absent(self.session, checkpoint.rows)
            self._save(run, checkpoint.model_copy(update={"phase": "complete"}))
            self.session.commit()
        return self.report(outcome="checked")

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
        return PurgeReport(
            child_plan_sha256=self.plan.digest(),
            outcome=outcome,
            identity=self.plan.identity,
            purpose="deletion",
            observed_at=datetime.now(UTC),
            host_contract=self.host_contract,
            runs=tuple(runs),
        )
