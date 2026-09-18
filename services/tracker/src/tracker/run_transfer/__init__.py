"""Guarded historical transfer between two explicit Tracker databases."""

import copy
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select, text
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkArguments, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, require_owned_hold
from tracker.lifecycle_completion import acquire_successor_hold
from tracker.lifecycle_evidence import DispatchDrain, validate_host_contract_observation, verify_drain
from tracker.run_purge.contracts import ProviderLocator
from tracker.run_purge.locking import exclusive_operation
from tracker.run_transfer.contracts import (
    TransferCheckpoint,
    TransferObservation,
    TransferRequest,
    TransferResponse,
    TransferRun,
)
from tracker.run_transfer.predecessor import acquire_source_hold, validate_predecessor
from tracker.run_transfer.references import inventory_references
from tracker.run_transfer.rows import RowClosure, digest, tables
from tracker.runtime.log_history import ArchiveReport


class TransferBoundary(Protocol):
    async def validate(self, request: TransferRequest, run: TransferRun) -> None: ...
    async def drain(
        self, request: TransferRequest, run: TransferRun, arguments: dict[str, Any], *, cleanup: bool = False
    ) -> None: ...
    async def verify_objects(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        source_removed: bool = False,
        source_partial: bool = False,
        archive: ArchiveReport | None = None,
    ) -> None: ...
    async def archive(self, request: TransferRequest, run: TransferRun) -> ArchiveReport: ...
    async def verify_archive(self, request: TransferRequest, run: TransferRun, archive: ArchiveReport) -> None: ...
    async def cleanup_logs(self, request: TransferRequest, run: TransferRun, archive: ArchiveReport) -> None: ...
    async def portable(self, request: TransferRequest, run: TransferRun, rows: RowClosure) -> None: ...


class TransferOperator:
    def __init__(self, source: Session, destination: Session, boundary: TransferBoundary) -> None:
        self.source = source
        self.destination = destination
        self.boundary = boundary

    def _catalog(self, request: TransferRequest, closure: RowClosure) -> RowClosure:
        plan = request.plan
        source_schema, destination_schema = tables(self.source), tables(self.destination)
        for session, schema in ((self.source, source_schema), (self.destination, destination_schema)):
            org = (
                session.connection()
                .execute(select(schema["org"]).where(schema["org"].c.id == plan.source_identity.org_id))
                .mappings()
                .one_or_none()
            )
            if org is None or org["name"] != plan.org_name:
                raise LifecycleConflict("Existing exact organization is required")
        mapped = copy.deepcopy(closure)
        mappings = {item.source_id: item for item in plan.releases}
        referenced: set[str] = set()
        benchmark = mapped.rows["benchmark"][0]
        for row, columns in [
            (benchmark, ("executor_release_id", "current_execution_release_id")),
            *((row, ("executor_release_id",)) for row in mapped.rows["executordispatch"]),
        ]:
            for column in columns:
                identifier = row[column]
                if identifier is None:
                    continue
                referenced.add(identifier)
                mapping = mappings.get(identifier)
                if mapping is None:
                    raise LifecycleConflict("Referenced release has no explicit mapping")
                for session, schema, release_id, uri in (
                    (self.source, source_schema, mapping.source_id, mapping.source_artifact_uri),
                    (self.destination, destination_schema, mapping.destination_id, mapping.destination_artifact_uri),
                ):
                    release = (
                        session.connection()
                        .execute(
                            select(
                                *(
                                    schema["executorrelease"].c[name]
                                    for name in ("id", "artifact_uri", "artifact_digest", "protocol_version")
                                )
                            ).where(schema["executorrelease"].c.id == release_id)
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if release is None or (
                        release["artifact_uri"],
                        release["artifact_digest"],
                        release["protocol_version"],
                    ) != (uri, mapping.artifact_digest, mapping.protocol_version):
                        raise LifecycleConflict("Existing release catalog identity differs")
                if column == "executor_release_id":
                    if (
                        row["executor_artifact_uri"],
                        row["executor_artifact_digest"],
                        row["executor_protocol_version"],
                    ) != (mapping.source_artifact_uri, mapping.artifact_digest, mapping.protocol_version):
                        raise LifecycleConflict("Denormalized release snapshot differs")
                    row["executor_artifact_uri"] = mapping.destination_artifact_uri
                row[column] = mapping.destination_id
        # Every mapping must belong to some run; the caller checks the complete plan set.
        return mapped

    def _source_rows(self, request: TransferRequest, run: TransferRun) -> RowClosure:
        closure = RowClosure.read(self.source, run.source.run_id, request.plan.source_identity.org_id)
        benchmark = closure.rows["benchmark"][0]
        if set(benchmark["arguments"]) - set(BenchmarkArguments.model_fields):
            raise LifecycleConflict("Unknown stored execution argument field")
        if benchmark["log_history"] is not None:
            raise LifecycleConflict(
                "Declared source archive requires an approved exact archive remap; planning refused"
            )
        if benchmark["arguments"].get("properties") != run.source.original_resources.__dict__:
            raise LifecycleConflict("Saved source resources differ")
        if benchmark["status"] not in {"FINISHED", "ERROR", "STOPPED"}:
            raise LifecycleConflict("Source run is not terminal")
        if any(
            row["status"] not in {"FINISHED", "ERROR", "STOPPED"} or row["eval_resume_state"] is not None
            for row in closure.rows["task"]
        ):
            raise LifecycleConflict("Active or resumable tasks remain")
        if any(row["status"] in {"QUEUED", "RUNNING"} for row in closure.rows["executordispatch"]):
            raise LifecycleConflict("Active dispatch remains")
        if request.action != "plan" and (run.source_rows_sha256 is None or closure.sha256 != run.source_rows_sha256):
            raise LifecycleConflict("Exact planned source row content changed")
        return closure

    def _checkpoint(
        self, session: Session, request: TransferRequest, run: TransferRun, *, destination: bool
    ) -> tuple[RunLifecycle, TransferCheckpoint]:
        identity = request.plan.destination_identity if destination else request.plan.source_identity
        scope = run.destination if destination else run.source
        record = require_owned_hold(session, identity=identity, scope=scope, purpose="relocation")
        checkpoint = TransferCheckpoint.model_validate_json(record.checkpoint_json or "null")
        if (
            checkpoint.child_plan_sha256 != request.plan.sha256
            or checkpoint.identity_sha256 != digest(identity.model_dump(mode="json"))
            or checkpoint.scope_sha256 != digest(scope.model_dump(mode="json"))
            or checkpoint.phase != record.phase
            or checkpoint.source_rows_sha256 != run.source_rows_sha256
            or checkpoint.execution_policy != run.execution_policy
            or (record.released_at is not None) != (checkpoint.phase == "released")
        ):
            raise LifecycleConflict("Exact transfer checkpoint differs")
        if checkpoint.copied_objects_sha256 is not None and (
            checkpoint.copied_objects_sha256
            != digest([item.model_dump(mode="json") for item in request.copied_objects])
            or checkpoint.destination_versions_sha256
            != digest([item.model_dump(mode="json") for item in request.destination_versions])
        ):
            raise LifecycleConflict("Durable copied object evidence changed")
        return record, checkpoint

    def _save(self, session: Session, record: RunLifecycle, checkpoint: TransferCheckpoint) -> None:
        checkpoint = TransferCheckpoint.model_validate_json(checkpoint.model_dump_json())
        record.phase = checkpoint.phase
        record.checkpoint_json = checkpoint.model_dump_json()
        session.add(record)
        session.flush()

    def _process_drain(self, request: TransferRequest, run: TransferRun) -> tuple[DispatchDrain, ...]:
        host = request.source_host_contract
        if host is None:
            raise LifecycleConflict("Fresh source host observation is required")
        validate_host_contract_observation(host)
        supplied = [item for item in request.external_host_drains if item.run_id == run.source.run_id]
        if len(supplied) > 1:
            raise LifecycleConflict("Duplicate external host evidence")
        external = supplied[0] if supplied else None
        contents = [Path(path).read_bytes() for path in request.external_evidence_files]
        matched = [
            value
            for value in contents
            if external is not None and hashlib.sha256(value).hexdigest() == external.evidence_sha256
        ]
        if external is not None and len(matched) != 1:
            raise LifecycleConflict("Exact external host evidence file is required")
        drains = verify_drain(
            self.source,
            identity=request.plan.source_identity,
            scope=run.source,
            purpose="relocation",
            host_contract=host,
            external=external,
            external_evidence=matched[0] if matched else None,
        )
        if any(value.provenance == "pending" for value in drains):
            raise LifecycleConflict("Positive process drain is pending")
        return drains

    def _destination_rows(
        self, request: TransferRequest, run: TransferRun, archive: ArchiveReport, source: RowClosure
    ) -> RowClosure:
        mapped = self._catalog(request, source)
        for edit in run.reference_edits:
            row = mapped.rows["benchmark"][0]
            container = row["arguments"] if edit.pointer.startswith("/arguments/") else row
            key = edit.pointer.rsplit("/", 1)[-1]
            if not isinstance(container.get(key), str) or digest(container[key]) != edit.original_sha256:
                raise LifecycleConflict("Exact reference edit source value changed")
            container[key] = edit.replacement
        mapped.rows["benchmark"][0]["arguments"]["properties"] = dict(run.destination.original_resources.__dict__)
        mapped.rows["benchmark"][0]["log_history"] = archive.reference.model_dump(mode="json")
        mapped.sql_nulls["benchmark"][0] = [
            value for value in mapped.sql_nulls["benchmark"][0] if value != "log_history"
        ]
        return mapped

    async def execute(self, request: TransferRequest) -> TransferResponse:
        # Shared advisory locks serialize deletion, relocation and transfer, including absent runs.
        with (
            exclusive_operation(self.source, request.plan.source_identity),
            exclusive_operation(self.destination, request.plan.destination_identity),
        ):
            self._destination_catalog(request)
            if request.action == "inspect" and request.parent_completion is not None:
                self._inspect_completion(request)

            observations: list[TransferObservation] = []
            for run in request.plan.runs:
                observations.append(await self._execute_run(request, run))

            return TransferResponse(
                copied_objects_sha256=digest([item.model_dump(mode="json") for item in request.copied_objects])
                if request.action in {"import", "inspect", "cleanup", "finalize"}
                and all(item.archive is not None for item in observations)
                else None,
                destination_versions_sha256=digest(
                    [item.model_dump(mode="json") for item in request.destination_versions]
                )
                if request.action in {"import", "inspect", "cleanup", "finalize"}
                and all(item.archive is not None for item in observations)
                else None,
                parent_completion_sha256=digest(request.parent_completion.model_dump(mode="json"))
                if request.parent_completion is not None and request.action in {"inspect", "cleanup", "finalize"}
                else None,
                action=request.action,
                nonce=request.nonce,
                child_plan_sha256=request.plan.sha256,
                runs=tuple(observations),
            )

    async def _execute_run(self, request: TransferRequest, run: TransferRun) -> TransferObservation:
        source_record = self.source.get(RunLifecycle, run.source.run_id, populate_existing=True)
        destination_record = self.destination.get(RunLifecycle, run.source.run_id, populate_existing=True)
        destination_exists = self.destination.get(Benchmark, run.source.run_id, populate_existing=True) is not None
        if (
            destination_record is not None
            and destination_record.identity_json != request.plan.destination_identity.model_dump_json()
        ):
            raise LifecycleConflict("Destination lifecycle tombstone or unrelated operation blocks import")
        if destination_exists and destination_record is None:
            raise LifecycleConflict("Destination run has no exact transfer hold")
        if request.action == "plan":
            validate_predecessor(source_record, request, run)
            source = self._source_rows(request, run)
            self._catalog(request, source)
            if not destination_exists:
                source.check_conflicts(self.destination)
            return self._observation(request, run, source, None, source_record, destination_record, None)

        if run.source_rows_sha256 is None:
            raise LifecycleConflict("Reviewed source content digest is required")
        if request.action == "prepare" and (
            source_record is None or source_record.identity_json != request.plan.source_identity.model_dump_json()
        ):
            source = self._source_rows(request, run)
            self._catalog(request, source)
            await self.boundary.validate(request, run)
            source_record = acquire_source_hold(self.source, request, run)
            source_record.acquired_at = (
                self.source.connection().execute(text("SELECT timezone('UTC', current_timestamp)")).scalar_one()
            )
            checkpoint = TransferCheckpoint(
                provider=ProviderLocator(
                    kind=source.rows["benchmark"][0]["arguments"]["sandbox_provider"],
                    secret_name=source.rows["benchmark"][0]["arguments"]["sandbox_provider_secret_name"],
                ),
                child_plan_sha256=request.plan.sha256,
                identity_sha256=digest(request.plan.source_identity.model_dump(mode="json")),
                scope_sha256=digest(run.source.model_dump(mode="json")),
                source_rows_sha256=source.sha256,
                execution_policy=run.execution_policy,
                phase="held",
            )
            self._save(self.source, source_record, checkpoint)
            self.source.commit()

        source_record, checkpoint = self._checkpoint(self.source, request, run, destination=False)
        retired = checkpoint.phase == "transferred_source_retired"
        if retired and self.source.get(Benchmark, run.source.run_id, populate_existing=True) is not None:
            raise LifecycleConflict("Retired source identity was recreated")
        source = None if retired else self._source_rows(request, run)
        await self.boundary.validate(request, run)
        dispatches = checkpoint.dispatches
        if source is not None:
            dispatches = self._process_drain(request, run)
            await self.boundary.drain(
                request, run, source.rows["benchmark"][0]["arguments"], cleanup=request.action == "prepare"
            )
            # Read again after provider work while retaining database row locks.
            source = self._source_rows(request, run)
        if request.action == "prepare" and checkpoint.phase == "held":
            checkpoint = checkpoint.model_copy(update={"phase": "prepared", "dispatches": dispatches})
            self._save(self.source, source_record, checkpoint)
            self.source.commit()

        if retired:
            await self.boundary.drain(
                request,
                run,
                {
                    "sandbox_provider": checkpoint.provider.kind,
                    "sandbox_provider_secret_name": checkpoint.provider.secret_name,
                },
            )

        archive = checkpoint.archive
        destination_rows = None
        if destination_exists:
            destination_record, destination_checkpoint = self._checkpoint(
                self.destination, request, run, destination=True
            )
            archive = destination_checkpoint.archive
            if archive is None:
                raise LifecycleConflict("Destination archive proof is absent")
            destination_rows = RowClosure.read(self.destination, run.source.run_id, request.plan.source_identity.org_id)
            expected_dispatches = tuple(row["id"] for row in destination_rows.rows["executordispatch"])
            if (
                tuple(item.dispatch_id for item in checkpoint.dispatches) != expected_dispatches
                or tuple(item.dispatch_id for item in destination_checkpoint.dispatches) != expected_dispatches
            ):
                raise LifecycleConflict("Retained process evidence differs from exact dispatch set")
            if destination_rows.sha256 != destination_checkpoint.destination_rows_sha256:
                raise LifecycleConflict("Destination content or exact child set changed")
            await self.boundary.verify_archive(request, run, archive)
            if (
                source is not None
                and self._destination_rows(request, run, archive, source).sha256 != destination_rows.sha256
            ):
                raise LifecycleConflict("Destination differs from exact mapped source content")

        if request.action == "import":
            if source is None or checkpoint.phase == "held":
                raise LifecycleConflict("Prepared intact source is required")
            if not destination_exists:
                archive = await self.boundary.archive(request, run)
                await self.boundary.verify_archive(request, run, archive)
                await self.boundary.verify_objects(request, run, archive=archive)
                dispatches = self._process_drain(request, run)
                await self.boundary.drain(request, run, source.rows["benchmark"][0]["arguments"])
                source = self._source_rows(request, run)
                destination_rows = self._destination_rows(request, run, archive, source)
                if run.execution_policy == "portable":
                    if any(item.provenance == "held_unclaimed" for item in dispatches):
                        raise LifecycleConflict(
                            "Portable transfer requires process absence independent of the source hold"
                        )
                    await self.boundary.portable(request, run, destination_rows)
                destination_rows.insert(self.destination)
                destination_record = acquire_successor_hold(
                    self.destination,
                    identity=request.plan.destination_identity,
                    scope=run.destination,
                    purpose="relocation",
                    predecessor=None,
                )
                destination_record.acquired_at = (
                    self.destination.connection()
                    .execute(text("SELECT timezone('UTC', current_timestamp)"))
                    .scalar_one()
                )
                destination_checkpoint = checkpoint.model_copy(
                    update={
                        "identity_sha256": digest(request.plan.destination_identity.model_dump(mode="json")),
                        "scope_sha256": digest(run.destination.model_dump(mode="json")),
                        "phase": "transferred",
                        "destination_rows_sha256": destination_rows.sha256,
                        "archive": archive,
                        "copied_objects_sha256": digest(
                            [item.model_dump(mode="json") for item in request.copied_objects]
                        ),
                        "destination_versions_sha256": digest(
                            [item.model_dump(mode="json") for item in request.destination_versions]
                        ),
                    }
                )
                self._save(self.destination, destination_record, destination_checkpoint)
                observed = RowClosure.read(self.destination, run.source.run_id, request.plan.source_identity.org_id)
                if observed.sha256 != destination_rows.sha256:
                    raise LifecycleConflict("Destination stored readback differs")
                self.destination.commit()
            assert archive is not None and destination_rows is not None
            await self.boundary.verify_objects(request, run, archive=archive)
            checkpoint = checkpoint.model_copy(
                update={
                    "phase": "transferred",
                    "destination_rows_sha256": destination_rows.sha256,
                    "archive": archive,
                    "copied_objects_sha256": digest([item.model_dump(mode="json") for item in request.copied_objects]),
                    "destination_versions_sha256": digest(
                        [item.model_dump(mode="json") for item in request.destination_versions]
                    ),
                }
            )
            self._save(self.source, source_record, checkpoint)
            self.source.commit()

        if request.action in {"cleanup", "finalize"}:
            if destination_rows is None or archive is None or destination_record is None:
                raise LifecycleConflict("Verified destination is required")
            self._completion(request)
            completion_digest = (
                digest(request.parent_completion.model_dump(mode="json")) if request.parent_completion else None
            )
            if checkpoint.parent_completion_sha256 not in {None, completion_digest}:
                raise LifecycleConflict("Parent completion authorization changed")
            await self.boundary.verify_objects(request, run, source_removed=True, archive=archive)
            if request.action == "cleanup" and not retired:
                assert source is not None
                if run.execution_policy == "portable":
                    if any(item.provenance == "held_unclaimed" for item in dispatches):
                        raise LifecycleConflict(
                            "Portable transfer requires process absence independent of the source hold"
                        )
                    await self.boundary.portable(request, run, destination_rows)
                await self.boundary.cleanup_logs(request, run, archive)
                dispatches = self._process_drain(request, run)
                await self.boundary.drain(request, run, source.rows["benchmark"][0]["arguments"])
                source = self._source_rows(request, run)
                source.delete(self.source)
                checkpoint = checkpoint.model_copy(
                    update={"phase": "transferred_source_retired", "parent_completion_sha256": completion_digest}
                )
                self._save(self.source, source_record, checkpoint)
                self.source.commit()
                self.source.expunge_all()
                source = None
            if request.action == "finalize":
                if not retired:
                    raise LifecycleConflict("Separate source cleanup must complete first")
                destination_record, destination_checkpoint = self._checkpoint(
                    self.destination, request, run, destination=True
                )
                if run.execution_policy == "portable":
                    if any(item.provenance == "held_unclaimed" for item in dispatches):
                        raise LifecycleConflict(
                            "Portable transfer requires process absence independent of the source hold"
                        )
                    await self.boundary.portable(request, run, destination_rows)
                phase = "released" if run.execution_policy == "portable" else "transferred_history_only"
                destination_checkpoint = destination_checkpoint.model_copy(
                    update={"phase": phase, "parent_completion_sha256": completion_digest}
                )
                self._save(self.destination, destination_record, destination_checkpoint)
                if phase == "released" and destination_record.released_at is None:
                    destination_record.released_at = (
                        self.destination.connection()
                        .execute(text("SELECT timezone('UTC', current_timestamp)"))
                        .scalar_one()
                    )
                    self.destination.add(destination_record)
                self.destination.commit()
        if request.action == "inspect" and archive is not None:
            await self.boundary.verify_objects(
                request,
                run,
                source_removed=retired,
                source_partial=request.parent_completion is not None and not retired,
                archive=archive,
            )
        return self._observation(
            request, run, source, destination_rows, source_record, destination_record, archive, dispatches
        )

    def _destination_catalog(self, request: TransferRequest) -> None:
        schema = tables(self.destination)
        organization = schema["org"]
        if (
            self.destination.connection()
            .execute(
                select(organization.c.name)
                .where(organization.c.id == request.plan.source_identity.org_id)
                .with_for_update()
            )
            .scalar_one_or_none()
            != request.plan.org_name
        ):
            raise LifecycleConflict("Destination organization catalog changed")
        catalog = schema["executorrelease"]
        for mapping in request.plan.releases:
            release = (
                self.destination.connection()
                .execute(
                    select(catalog.c.artifact_uri, catalog.c.artifact_digest, catalog.c.protocol_version)
                    .where(catalog.c.id == mapping.destination_id)
                    .with_for_update()
                )
                .one_or_none()
            )
            if release is None or tuple(release) != (
                mapping.destination_artifact_uri,
                mapping.artifact_digest,
                mapping.protocol_version,
            ):
                raise LifecycleConflict("Destination release catalog changed")

    def _completion(self, request: TransferRequest) -> None:
        completion = request.parent_completion
        identity = request.plan.source_identity
        if completion is None or (
            completion.operation_id,
            completion.parent_plan_sha256,
            completion.child_plan_sha256,
        ) != (identity.operation_id, identity.parent_plan_sha256, request.plan.sha256):
            raise LifecycleConflict("Exact separate parent completion authorization is required")
        rows: list[dict[str, Any]] = []
        archives: list[dict[str, Any]] = []
        for run in request.plan.runs:
            _, checkpoint = self._checkpoint(self.destination, request, run, destination=True)
            actual = RowClosure.read(self.destination, run.source.run_id, identity.org_id)
            if actual.sha256 != checkpoint.destination_rows_sha256 or checkpoint.archive is None:
                raise LifecycleConflict("Parent destination proof changed")
            rows.append({"run_id": str(run.source.run_id), "sha256": actual.sha256})
            archives.append(checkpoint.archive.model_dump(mode="json"))
        if completion.destination_rows_sha256 != digest(rows) or completion.archives_sha256 != digest(archives):
            raise LifecycleConflict("Parent completion differs from current destination rows or archives")

    def _inspect_completion(self, request: TransferRequest) -> None:
        self._completion(request)
        assert request.parent_completion is not None
        completion_digest = digest(request.parent_completion.model_dump(mode="json"))
        for run in request.plan.runs:
            _, source_checkpoint = self._checkpoint(self.source, request, run, destination=False)
            _, destination_checkpoint = self._checkpoint(self.destination, request, run, destination=True)
            if (source_checkpoint.phase, destination_checkpoint.phase) not in {
                ("transferred", "transferred"),
                ("transferred_source_retired", "transferred"),
                ("transferred_source_retired", "transferred_history_only"),
                ("transferred_source_retired", "released"),
            }:
                raise LifecycleConflict("Parent completion requires transferred source and destination checkpoints")

            for checkpoint in (source_checkpoint, destination_checkpoint):
                if checkpoint.parent_completion_sha256 not in {None, completion_digest}:
                    raise LifecycleConflict("Retained parent completion authorization changed")

    def _observation(
        self,
        request: TransferRequest,
        run: TransferRun,
        source: RowClosure | None,
        destination: RowClosure | None,
        source_record: RunLifecycle | None,
        destination_record: RunLifecycle | None,
        archive: ArchiveReport | None,
        dispatches: tuple[DispatchDrain, ...] = (),
    ) -> TransferObservation:
        reference_rows = source if source is not None else destination
        return TransferObservation.model_validate(
            {
                "run_id": run.source.run_id,
                "predecessor": validate_predecessor(source_record, request, run)
                if request.action == "plan"
                else run.predecessor,
                "source_hold_identity": OperationIdentity.model_validate_json(source_record.identity_json)
                if source_record
                else None,
                "source_hold_scope": RunScope.model_validate_json(source_record.scope_json) if source_record else None,
                "destination_hold_identity": OperationIdentity.model_validate_json(destination_record.identity_json)
                if destination_record
                else None,
                "destination_hold_scope": RunScope.model_validate_json(destination_record.scope_json)
                if destination_record
                else None,
                "source_rows_sha256": None if source is None else source.sha256,
                "destination_rows_sha256": None if destination is None else destination.sha256,
                "references": inventory_references(reference_rows) if reference_rows is not None else (),
                "provider_absence": "not_inspected" if request.action == "plan" else "verified_absent",
                "dispatches": dispatches,
                "drain_origin": "not_inspected"
                if request.action == "plan"
                else "current_source"
                if source is not None
                else "retired_source_checkpoint",
                "host_observation_sha256": digest(request.source_host_contract.model_dump(mode="json"))
                if request.action != "plan" and request.source_host_contract
                else None,
                "source_tables": {} if source is None else source.summary()["tables"],
                "destination_tables": {} if destination is None else destination.summary()["tables"],
                "source_identity": request.plan.source_identity,
                "destination_identity": request.plan.destination_identity,
                "source_scope": run.source,
                "destination_scope": run.destination,
                "source_phase": None if source_record is None else source_record.phase,
                "destination_phase": None if destination_record is None else destination_record.phase,
                "destination_released_at": None if destination_record is None else destination_record.released_at,
                "source_checkpoint_sha256": None if source_record is None else digest(source_record.checkpoint_json),
                "destination_checkpoint_sha256": None
                if destination_record is None
                else digest(destination_record.checkpoint_json),
                "archive": archive,
                "observed_at": datetime.now(UTC),
            }
        )
