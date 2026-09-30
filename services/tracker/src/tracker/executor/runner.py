"""One claimed executor dispatch per runner process."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, TypeVar, cast

import boto3
import psycopg2  # pyright: ignore[reportMissingModuleSource]
from psycopg2.extensions import connection as PostgresConnection  # pyright: ignore[reportMissingModuleSource]
from uuid import UUID

from executor_protocol import (
    DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS,
    DEFAULT_EXECUTOR_DISPATCH_LEASE_TICK_SECONDS,
    DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
    SUPPORTED_PROTOCOL_VERSIONS,
    ExecutorTelemetryContext,
    normalize_executor_telemetry_context,
    validate_executor_artifact_uri,
    validate_executor_digest,
)
from tracker.executor.dispatch_payload import SealedPayload, open_payload
from tracker.executor.runner_observability import (
    capture_dispatch_error,
    capture_runner_failure,
    configure_observability,
    dispatch_observability_context,
    record_dispatch_cancellation,
    record_dispatch_completion,
)

logger = logging.getLogger(__name__)

_AUTHORITY_LOSS_GRACE_SECONDS = 10


class S3Client(Protocol):
    def download_file(self, bucket: str, key: str, filename: str) -> None:
        pass


@dataclass(frozen=True)
class ArtifactDispatch:
    release_id: str
    artifact_uri: str
    artifact_digest: str
    protocol_version: str

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ArtifactDispatch:
        digest = validate_executor_digest(_required_string(payload, "executor_artifact_digest"))
        protocol_version = _required_string(payload, "executor_protocol_version")
        if protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ValueError(f"Unsupported executor protocol version: {protocol_version}")
        return cls(
            release_id=_required_string(payload, "executor_release_id"),
            artifact_uri=_required_string(payload, "executor_artifact_uri"),
            artifact_digest=digest,
            protocol_version=protocol_version,
        )


@dataclass(frozen=True)
class DispatchAuthority:
    dispatch_id: str
    benchmark_id: str


@dataclass(frozen=True)
class ExecutorProcessPayload:
    benchmark_id: str
    verified_task_ids: list[str]
    arguments: dict[str, object]

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        telemetry_context: ExecutorTelemetryContext,
    ) -> ExecutorProcessPayload:
        execution_context = payload.get("execution_context_json")
        access_key_values = (
            payload.get("start_benchmark_request_json"),
            payload.get("benchmark_id_str"),
            payload.get("verified_task_ids"),
        )
        if execution_context is not None:
            if any(value is not None for value in access_key_values):
                raise ValueError("Executor payload mixes access-key and managed execution inputs")
            if not isinstance(execution_context, Mapping):
                raise ValueError("Executor payload has no valid managed execution context")
            execution_context_mapping = cast(Mapping[str, object], execution_context)
            benchmark_id = execution_context_mapping.get("benchmark_id")
            raw_task_ids = execution_context_mapping.get("verified_task_ids")
            arguments: dict[str, object] = {"execution_context_json": dict(execution_context_mapping)}
        else:
            request, benchmark_id, raw_task_ids = access_key_values
            if not isinstance(request, Mapping):
                raise ValueError("Executor payload has no valid access-key benchmark request")
            request_mapping = cast(Mapping[str, object], request)
            arguments = {
                "start_benchmark_request_json": dict(request_mapping),
                "benchmark_id_str": benchmark_id,
                "verified_task_ids": raw_task_ids,
            }
        arguments["telemetry_context_json"] = telemetry_context
        if not isinstance(benchmark_id, str) or not benchmark_id:
            raise ValueError("Executor payload has no valid benchmark ID")
        verified_task_ids = (
            [str(task_id) for task_id in cast(list[object], raw_task_ids)] if isinstance(raw_task_ids, list) else []
        )
        return cls(
            benchmark_id=benchmark_id,
            verified_task_ids=verified_task_ids,
            arguments=arguments,
        )


@dataclass(frozen=True)
class RenewalResult:
    renewed: bool
    classification: tuple[bool, bool] | None  # (live, stopped); None if classification was unavailable


@dataclass(frozen=True)
class ClaimedDispatch:
    authority: DispatchAuthority
    dispatch: ArtifactDispatch
    process_payload: ExecutorProcessPayload
    telemetry_context: ExecutorTelemetryContext


class ExecutorDispatchStore(Protocol):
    async def claim(self, dispatch_id: str) -> ClaimedDispatch | None: ...

    async def renew(self, authority: DispatchAuthority) -> RenewalResult: ...

    async def terminalize(self, authority: DispatchAuthority, task_ids: list[str]) -> bool: ...

    async def finish(self, authority: DispatchAuthority) -> bool: ...


class PostgresExecutorDispatchStore:
    """Persist dispatch lifecycle at the per-task process-owner boundary."""

    def __init__(
        self,
        *,
        host: str,
        port: str,
        dbname: str,
        user: str,
        password: str,
    ) -> None:
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    @classmethod
    def from_environment(cls) -> PostgresExecutorDispatchStore:
        return cls(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USERNAME"],
            password=os.environ["DB_PASSWORD"],
        )

    def _connect(self) -> PostgresConnection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=5,
        )

    async def claim(self, dispatch_id: str) -> ClaimedDispatch | None:
        return await asyncio.to_thread(self._claim, dispatch_id)

    def _claim(self, dispatch_id: str) -> ClaimedDispatch | None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE executordispatch AS dispatch
                SET status = 'RUNNING',
                    started_at = CURRENT_TIMESTAMP,
                    heartbeat_at = CURRENT_TIMESTAMP,
                    lease_expires_at = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')
                FROM benchmark
                WHERE dispatch.id = %s::uuid
                  AND dispatch.benchmark_id = benchmark.id
                  AND benchmark.status = 'IN_PROGRESS'
                  AND benchmark.current_execution_release_id = dispatch.executor_release_id
                  AND dispatch.status = 'QUEUED'
                  AND dispatch.claim_deadline_at > CURRENT_TIMESTAMP
                RETURNING dispatch.benchmark_id::text, dispatch.executor_release_id,
                          dispatch.executor_artifact_uri, dispatch.executor_artifact_digest,
                          dispatch.executor_protocol_version, dispatch.assigned_task_ids
                """,
                (DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS, dispatch_id),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            cursor.execute(
                """
                DELETE FROM executor_dispatch_payload
                WHERE dispatch_id = %s::uuid
                RETURNING ciphertext, encrypted_data_key, nonce
                """,
                (dispatch_id,),
            )
            sealed_row = cursor.fetchone()
            if sealed_row is None:
                raise ValueError(f"Queued executor dispatch {dispatch_id} has no sealed payload")
            sealed = SealedPayload(*(bytes(value) for value in sealed_row))
            payload = open_payload(UUID(dispatch_id), sealed)
            payload.update(
                {
                    "executor_dispatch_id": dispatch_id,
                    "executor_release_id": row[1],
                    "executor_artifact_uri": row[2],
                    "executor_artifact_digest": row[3],
                    "executor_protocol_version": row[4],
                }
            )
            telemetry_context = normalize_executor_telemetry_context(payload.get("telemetry_context_json"))
            artifact = ArtifactDispatch.from_payload(payload)
            process_payload = ExecutorProcessPayload.from_payload(payload, telemetry_context=telemetry_context)
            if process_payload.benchmark_id != row[0] or process_payload.verified_task_ids != row[5]:
                raise ValueError(f"Executor dispatch {dispatch_id} payload does not match assigned benchmark/tasks")
            return ClaimedDispatch(
                authority=DispatchAuthority(dispatch_id=dispatch_id, benchmark_id=row[0]),
                dispatch=artifact,
                process_payload=process_payload,
                telemetry_context=telemetry_context,
            )

    async def renew(self, authority: DispatchAuthority) -> RenewalResult:
        return await asyncio.to_thread(self._renew, authority)

    def _renew(self, authority: DispatchAuthority) -> RenewalResult:
        """Commit dispatch-only renewals before bounded benchmark classification."""
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                WITH lockable AS (
                    SELECT d.id
                    FROM executordispatch AS d
                    WHERE d.id = %s::uuid AND d.benchmark_id = %s::uuid
                      AND d.status = 'RUNNING' AND d.lease_expires_at > CURRENT_TIMESTAMP
                    FOR UPDATE OF d SKIP LOCKED
                )
                UPDATE executordispatch AS d
                SET heartbeat_at = CURRENT_TIMESTAMP,
                    lease_expires_at = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')
                FROM lockable AS l
                WHERE d.id = l.id
                RETURNING d.id
                """,
                (authority.dispatch_id, authority.benchmark_id, DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS),
            )
            renewed = cursor.fetchone() is not None
            connection.commit()  # A blocked benchmark read must never hold back a confirmed lease.
            try:
                cursor.execute("SET LOCAL lock_timeout = '1s'")
                cursor.execute(
                    """
                    SELECT COALESCE(d.status = 'RUNNING' AND d.lease_expires_at > CURRENT_TIMESTAMP, FALSE) AS live,
                           COALESCE(b.status = 'STOPPED', FALSE) AS stopped
                    FROM (SELECT %s::uuid AS id, %s::uuid AS benchmark_id) AS requested
                    LEFT JOIN executordispatch AS d ON d.id = requested.id AND d.benchmark_id = requested.benchmark_id
                    LEFT JOIN benchmark AS b ON b.id = requested.benchmark_id
                    """,
                    (authority.dispatch_id, authority.benchmark_id),
                )
                live, stopped = cursor.fetchone()
                return RenewalResult(renewed=renewed, classification=(live, stopped))
            except psycopg2.OperationalError:
                connection.rollback()  # The committed renewals survive a classification timeout.
                return RenewalResult(renewed=renewed, classification=None)

    async def terminalize(self, authority: DispatchAuthority, task_ids: list[str]) -> bool:
        return await asyncio.to_thread(self._terminalize, authority, task_ids)

    def _terminalize(self, authority: DispatchAuthority, task_ids: list[str]) -> bool:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM benchmark
                WHERE id = %s::uuid
                FOR UPDATE
                """,
                (authority.benchmark_id,),
            )
            benchmark_row = cursor.fetchone()
            if benchmark_row is None:
                return False
            cursor.execute(
                """
                UPDATE executordispatch
                SET status = 'FAILED',
                    finished_at = CURRENT_TIMESTAMP,
                    failure_reason = 'EXECUTOR_FAILED'
                WHERE id = %s::uuid
                  AND benchmark_id = %s::uuid
                  AND status = 'RUNNING'
                  AND lease_expires_at > CURRENT_TIMESTAMP
                RETURNING id
                """,
                (authority.dispatch_id, authority.benchmark_id),
            )
            failed_dispatch_row = cursor.fetchone()
            if failed_dispatch_row is None:
                return False
            cursor.execute(
                """
                UPDATE task
                SET status = 'ERROR', finished_at = CURRENT_TIMESTAMP
                WHERE benchmark = %s::uuid
                  AND task_id = ANY(%s)
                  AND started_at <= (
                      SELECT created_at
                      FROM executordispatch
                      WHERE id = %s::uuid
                  )
                  AND status IN ('PENDING', 'BUILDING', 'IN_PROGRESS', 'EVALUATING')
                """,
                (authority.benchmark_id, task_ids, authority.dispatch_id),
            )
            if benchmark_row[0] == "IN_PROGRESS":
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM executordispatch
                        WHERE benchmark_id = %s::uuid
                          AND id != %s::uuid
                          AND status IN ('QUEUED', 'RUNNING')
                    )
                    """,
                    (authority.benchmark_id, authority.dispatch_id),
                )
                active_dispatch_row = cursor.fetchone()
                assert active_dispatch_row is not None
                if not bool(active_dispatch_row[0]):
                    cursor.execute(
                        """
                        UPDATE benchmark
                        SET status = 'ERROR',
                            finished_at = CURRENT_TIMESTAMP,
                            error_message = 'Executor host failed'
                        WHERE id = %s::uuid
                        """,
                        (authority.benchmark_id,),
                    )
            return True

    async def finish(self, authority: DispatchAuthority) -> bool:
        return await asyncio.to_thread(self._finish, authority)

    def _finish(self, authority: DispatchAuthority) -> bool:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM benchmark
                WHERE id = %s::uuid
                FOR UPDATE
                """,
                (authority.benchmark_id,),
            )
            benchmark_row = cursor.fetchone()
            if benchmark_row is None or benchmark_row[0] in ("STOPPING", "STOPPED"):
                return False

            cursor.execute(
                """
                UPDATE executordispatch
                SET status = 'FINISHED', finished_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid
                  AND benchmark_id = %s::uuid
                  AND status = 'RUNNING'
                  AND lease_expires_at > CURRENT_TIMESTAMP
                RETURNING id
                """,
                (authority.dispatch_id, authority.benchmark_id),
            )
            if cursor.fetchone() is None:
                return False

            if benchmark_row[0] == "IN_PROGRESS":
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM executordispatch
                        WHERE benchmark_id = %s::uuid
                          AND status IN ('QUEUED', 'RUNNING')
                    )
                    """,
                    (authority.benchmark_id,),
                )
                active_dispatch_row = cursor.fetchone()
                assert active_dispatch_row is not None
                if not bool(active_dispatch_row[0]):
                    cursor.execute(
                        """
                        UPDATE benchmark
                        SET status = 'ERROR',
                            finished_at = CURRENT_TIMESTAMP,
                            error_message = 'Executor exited without finalizing benchmark'
                        WHERE id = %s::uuid
                        """,
                        (authority.benchmark_id,),
                    )
            return True


def _required_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def verify_file_digest(path: Path, expected_digest: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    actual_digest = digest.hexdigest()
    if actual_digest != expected_digest:
        raise ValueError(f"Executor artifact digest mismatch: expected {expected_digest}, got {actual_digest}")


@dataclass
class _DispatchLease:
    last_confirmed_renewal_at: float
    lost: asyncio.Event
    revoked: asyncio.Event

    def expires_in(self, now: float) -> float:
        return self.last_confirmed_renewal_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS - now


def _monotonic_time() -> float:
    return asyncio.get_running_loop().time()


class _LeaseKeeper:
    def __init__(
        self, store: ExecutorDispatchStore, *, interval_seconds: float = DEFAULT_EXECUTOR_DISPATCH_LEASE_TICK_SECONDS
    ) -> None:
        self.store = store
        self.interval_seconds = interval_seconds
        self.authority: DispatchAuthority | None = None
        self.lease: _DispatchLease | None = None
        self.timer: asyncio.TimerHandle | None = None
        self.task: asyncio.Task[None] | None = None

    def register(self, authority: DispatchAuthority, confirmed_at: float) -> _DispatchLease:
        if self.authority is not None:
            raise RuntimeError("Lease keeper already owns a dispatch")
        lease = _DispatchLease(confirmed_at, asyncio.Event(), asyncio.Event())
        self.authority = authority
        self.lease = lease
        self.timer = asyncio.get_running_loop().call_at(
            confirmed_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS, lease.lost.set
        )
        self.task = asyncio.create_task(self._run())
        return lease

    async def unregister(self) -> None:
        if self.timer is not None:
            self.timer.cancel()
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.authority = None
        self.lease = None
        self.timer = None
        self.task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_seconds)
            try:
                await self.tick()
            except Exception:
                logger.exception("Lease keeper failed; stopping dispatch")
                if self.lease is not None:
                    self.lease.lost.set()
                return

    async def refresh(self) -> None:
        authority = self.authority
        lease = self.lease
        if authority is None or lease is None:
            return
        checked_at = _monotonic_time()
        try:
            outcome = await self.store.renew(authority)
        except Exception:
            logger.exception("Failed to refresh executor dispatch authority; using local lease")
            return
        self._apply_renewal(lease, outcome, checked_at)

    async def tick(self) -> None:
        authority = self.authority
        lease = self.lease
        if authority is None or lease is None:
            return
        tick_started_at = _monotonic_time()
        try:
            outcome = await self.store.renew(authority)
        except Exception:
            logger.exception("Failed to renew executor dispatch lease; retrying")
            return
        self._apply_renewal(lease, outcome, tick_started_at)

    def _apply_renewal(self, lease: _DispatchLease, outcome: RenewalResult, checked_at: float) -> None:
        if self.lease is not lease or lease.lost.is_set():
            return
        if outcome.renewed and checked_at > lease.last_confirmed_renewal_at and lease.expires_in(_monotonic_time()) > 0:
            lease.last_confirmed_renewal_at = checked_at
            assert self.timer is not None
            self.timer.cancel()
            self.timer = asyncio.get_running_loop().call_at(
                checked_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS, lease.lost.set
            )
        if outcome.classification is None:
            return
        live, stopped = outcome.classification
        if not live and not stopped:
            lease.lost.set()
        elif stopped:
            lease.revoked.set()


class DispatchAuthorityLostError(RuntimeError):
    pass


class ExecutorSupervisor:
    def __init__(
        self,
        cache_dir: Path,
        *,
        s3_client: S3Client | None = None,
        python_executable: str = sys.executable,
        artifact_bucket: str | None = None,
        artifact_prefix: str | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.s3_client = s3_client
        self.python_executable = python_executable
        self.artifact_bucket = artifact_bucket or os.environ["EXECUTOR_RELEASE_BUCKET"]
        self.artifact_prefix = artifact_prefix or os.environ["EXECUTOR_RELEASE_PREFIX"]

    async def prepare_artifact(self, dispatch: ArtifactDispatch) -> Path:
        bucket, key = validate_executor_artifact_uri(
            dispatch.artifact_uri,
            self.artifact_bucket,
            self.artifact_prefix,
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.cache_dir / f"{dispatch.artifact_digest}.pex"
        try:
            verify_file_digest(artifact_path, dispatch.artifact_digest)
            artifact_path.chmod(artifact_path.stat().st_mode | 0o111)
            return artifact_path
        except (OSError, ValueError):
            pass

        temporary_fd, temporary_name = tempfile.mkstemp(
            dir=self.cache_dir,
            prefix=f".{dispatch.artifact_digest}.",
            suffix=".tmp",
        )
        os.close(temporary_fd)
        temporary_path = Path(temporary_name)
        try:
            client = self.s3_client or cast(
                S3Client,
                boto3.client("s3"),  # pyright: ignore[reportUnknownMemberType]
            )

            def download() -> None:
                client.download_file(bucket, key, str(temporary_path))

            await asyncio.to_thread(download)
            verify_file_digest(temporary_path, dispatch.artifact_digest)
            temporary_path.chmod(temporary_path.stat().st_mode | 0o111)
            temporary_path.replace(artifact_path)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
        return artifact_path

    async def run(
        self,
        artifact_path: Path,
        dispatch: ArtifactDispatch,
        *,
        process_payload: ExecutorProcessPayload,
        authority: DispatchAuthority,
        lease: _DispatchLease,
    ) -> None:
        if lease.lost.is_set() or lease.expires_in(_monotonic_time()) <= 0:
            raise DispatchAuthorityLostError(f"Executor dispatch {authority.dispatch_id} lease expired before spawn")
        if lease.revoked.is_set():
            raise DispatchAuthorityLostError(f"Executor dispatch {authority.dispatch_id} was superseded before spawn")
        payload = {**process_payload.arguments, "executor_dispatch_id": authority.dispatch_id}
        with tempfile.TemporaryDirectory(dir=self.cache_dir, prefix=".dispatch-") as temporary_directory:
            payload_path = Path(temporary_directory) / "payload.json"
            payload_path.write_text(json.dumps(payload))
            logger.info(
                "Launching benchmark %s dispatch_id=%s release=%s digest=%s protocol=%s",
                authority.benchmark_id,
                authority.dispatch_id,
                dispatch.release_id,
                dispatch.artifact_digest,
                dispatch.protocol_version,
            )
            process = await asyncio.create_subprocess_exec(
                self.python_executable,
                str(artifact_path),
                str(payload_path),
                start_new_session=True,
                env={**os.environ, "SENTRY_RELEASE": dispatch.release_id},
            )
            try:
                return_code = await self._wait_with_authority(process, lease)
            except BaseException:
                await _terminate_process_group(process)
                raise
            if return_code != 0:
                raise RuntimeError(f"Executor for benchmark {authority.benchmark_id} exited with status {return_code}")

    async def _wait_with_authority(
        self,
        process: asyncio.subprocess.Process,
        lease: _DispatchLease,
    ) -> int:
        process_task = asyncio.create_task(process.wait())
        authority_task = asyncio.create_task(lease.revoked.wait())
        lease_task = asyncio.create_task(lease.lost.wait())
        tasks = (process_task, authority_task, lease_task)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if lease_task in done:
                await lease_task
                await _terminate_process_group(process)
                await process_task
                raise DispatchAuthorityLostError("Executor dispatch lease expired")
            if authority_task in done:
                await authority_task
                completed, _ = await asyncio.wait(
                    (process_task, lease_task),
                    timeout=_AUTHORITY_LOSS_GRACE_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if lease_task in completed:
                    await _terminate_process_group(process)
                    await process_task
                    raise DispatchAuthorityLostError("Executor dispatch lease expired")
                if process_task not in completed:
                    await _terminate_process_group(process)
                    await process_task
                raise DispatchAuthorityLostError("Executor dispatch was superseded")
            return await process_task
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=30)
    except TimeoutError:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        await process.wait()


_Result = TypeVar("_Result")
_RETRY_INITIAL_SECONDS = 0.1
_RETRY_MAX_SECONDS = 5.0


async def _retry_operational_error(
    operation: Callable[[], Awaitable[_Result]], expires_at: Callable[[], float]
) -> _Result:
    delay = _RETRY_INITIAL_SECONDS
    while True:
        try:
            return await operation()
        except psycopg2.OperationalError:
            remaining = expires_at() - _monotonic_time()
            if remaining <= 0:
                raise
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, _RETRY_MAX_SECONDS)
            if expires_at() <= _monotonic_time():
                raise


async def _terminalize_after_failure(
    store: ExecutorDispatchStore,
    authority: DispatchAuthority,
    task_ids: list[str],
    expires_at: Callable[[], float],
) -> None:
    try:
        if not await _retry_operational_error(lambda: store.terminalize(authority, task_ids), expires_at):
            logger.warning(
                "Executor dispatch %s no longer had terminalization authority",
                authority.dispatch_id,
            )
    except Exception:
        logger.exception(
            "Failed to terminalize executor dispatch %s",
            authority.dispatch_id,
        )


async def run_executor_dispatch(
    executor_supervisor: ExecutorSupervisor,
    store: ExecutorDispatchStore,
    *,
    keeper: _LeaseKeeper,
    executor_dispatch_id: str,
) -> None:
    claim_started_at = _monotonic_time()
    attempt_started_at = claim_started_at

    async def claim() -> ClaimedDispatch | None:
        nonlocal attempt_started_at
        attempt_started_at = _monotonic_time()
        return await store.claim(executor_dispatch_id)

    claim_task = asyncio.create_task(
        _retry_operational_error(
            claim,
            lambda: claim_started_at + DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS,
        )
    )
    try:
        claimed = await asyncio.shield(claim_task)
    except asyncio.CancelledError:
        claimed = await claim_task
        if claimed is not None:
            with dispatch_observability_context(
                claimed.authority.benchmark_id,
                claimed.authority.dispatch_id,
                claimed.dispatch.release_id,
                claimed.telemetry_context,
            ) as child_telemetry_context:
                await _terminalize_after_failure(
                    store,
                    claimed.authority,
                    claimed.process_payload.verified_task_ids,
                    lambda: attempt_started_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
                )
                record_dispatch_cancellation(child_telemetry_context)
        raise
    if claimed is None:
        logger.warning("Skipping duplicate, superseded, or non-queued executor dispatch %s", executor_dispatch_id)
        return

    authority = claimed.authority
    process_payload = claimed.process_payload
    dispatch = claimed.dispatch
    with dispatch_observability_context(
        authority.benchmark_id, authority.dispatch_id, dispatch.release_id, claimed.telemetry_context
    ) as child_telemetry_context:
        process_payload.arguments["telemetry_context_json"] = child_telemetry_context
        lease = keeper.register(authority, attempt_started_at)
        try:
            try:
                artifact_path = await executor_supervisor.prepare_artifact(dispatch)
                await keeper.refresh()
                await executor_supervisor.run(
                    artifact_path,
                    dispatch,
                    process_payload=process_payload,
                    authority=authority,
                    lease=lease,
                )
                if not await _retry_operational_error(
                    lambda: store.finish(authority),
                    lambda: lease.last_confirmed_renewal_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
                ):
                    logger.warning(
                        "Executor dispatch %s lost authority before successful finish", authority.dispatch_id
                    )
            except asyncio.CancelledError:
                await _terminalize_after_failure(
                    store,
                    authority,
                    process_payload.verified_task_ids,
                    lambda: lease.last_confirmed_renewal_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
                )
                raise
            except BaseException:
                await _terminalize_after_failure(
                    store,
                    authority,
                    process_payload.verified_task_ids,
                    lambda: lease.last_confirmed_renewal_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
                )
                raise
        except asyncio.CancelledError:
            record_dispatch_cancellation(child_telemetry_context)
            raise
        except BaseException as error:
            capture_dispatch_error(error, child_telemetry_context)
            raise
        else:
            record_dispatch_completion(child_telemetry_context)
        finally:
            await keeper.unregister()


def _parse_dispatch_id(value: str) -> str:
    return str(UUID(value))


async def _run_main(dispatch_id: str) -> None:
    loop = asyncio.get_running_loop()
    store = PostgresExecutorDispatchStore.from_environment()
    if os.environ["EXECUTOR_LAUNCHER"] == "local":
        from tracker.executor.local_release import LocalArtifactStore

        supervisor = ExecutorSupervisor(
            Path(os.environ["EXECUTOR_CACHE_DIR"]),
            s3_client=LocalArtifactStore(Path(os.environ["EXECUTOR_RELEASE_LOCAL_DIR"])),
        )
    else:
        supervisor = ExecutorSupervisor(Path(os.environ["EXECUTOR_CACHE_DIR"]))
    task = asyncio.create_task(
        run_executor_dispatch(supervisor, store, keeper=_LeaseKeeper(store), executor_dispatch_id=dispatch_id)
    )
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await task
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run one executor dispatch")
    parser.add_argument("--dispatch-id", required=True, type=_parse_dispatch_id)
    arguments = parser.parse_args()
    configure_observability()
    try:
        asyncio.run(_run_main(arguments.dispatch_id))
    except asyncio.CancelledError:
        raise SystemExit(1) from None
    except Exception as error:
        logger.exception("Executor dispatch %s failed", arguments.dispatch_id)
        capture_runner_failure(error, arguments.dispatch_id)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
