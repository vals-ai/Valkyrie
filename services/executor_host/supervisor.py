"""Stable Taskiq host that launches immutable executor artifacts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import tempfile
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, Unpack, cast

import boto3
import psycopg2  # pyright: ignore[reportMissingModuleSource]
from psycopg2.extensions import connection as PostgresConnection  # pyright: ignore[reportMissingModuleSource]
from redis.asyncio import Redis
from taskiq import TaskiqEvents
from taskiq_redis import RedisStreamBroker
from executor_protocol import (
    DEFAULT_EXECUTOR_RELEASE_PREFIX,
    DEFAULT_STABLE_QUEUE_NAME,
    EXECUTOR_TASK_NAME,
    SUPPORTED_PROTOCOL_VERSIONS,
    ExecutorPayload,
    ExecutorTelemetryContext,
    executor_payload_benchmark_id,
    normalize_executor_telemetry_context,
    validate_executor_artifact_uri,
    validate_executor_digest,
)
from services.executor_host.observability import (
    capture_dispatch_error,
    configure_observability,
    dispatch_observability_context,
    record_dispatch_cancellation,
    record_dispatch_completion,
)

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = "/var/cache/valkyrie-executors"
ECS_AGENT_URI = os.environ.get("ECS_AGENT_URI")
_PROTECTION_EXPIRY_MINUTES = 120
_PROTECTION_REFRESH_SECONDS = 30 * 60
_PROTECTION_RETRY_SECONDS = 30
_AUTHORITY_LOSS_GRACE_SECONDS = 10
_ACK_AND_DELETE_SCRIPT = """
local acknowledged = redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
if acknowledged == 1 then
    redis.call("XDEL", KEYS[1], ARGV[2])
end
return acknowledged
"""
_active_execution_count = 0
_protection_refresh_task: asyncio.Task[None] | None = None
_execution_lock = asyncio.Lock()


async def _set_task_protection(*, enabled: bool) -> bool:
    if not ECS_AGENT_URI:
        return True
    body: dict[str, object] = {"ProtectionEnabled": enabled}
    if enabled:
        body["ExpiresInMinutes"] = _PROTECTION_EXPIRY_MINUTES
    request = urllib.request.Request(
        f"{ECS_AGENT_URI}/task-protection/v1/state",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )

    def update() -> None:
        with urllib.request.urlopen(request, timeout=5) as response:
            response.read()

    update_task = asyncio.create_task(asyncio.to_thread(update))
    try:
        await asyncio.shield(update_task)
    except asyncio.CancelledError:
        await _await_task_completion(update_task)
        raise
    except Exception:
        logger.exception("Failed to set ECS task protection to %s", enabled)
        return False
    return True


async def _renew_task_protection(delay_seconds: float) -> None:
    while True:
        await asyncio.sleep(delay_seconds)
        updated = await _set_task_protection(enabled=True)
        delay_seconds = _PROTECTION_REFRESH_SECONDS if updated is not False else _PROTECTION_RETRY_SECONDS


async def _await_task_cancellation(task: asyncio.Task[None]) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _acquire_task_protection() -> None:
    global _active_execution_count, _protection_refresh_task
    async with _execution_lock:
        if _active_execution_count == 0:
            updated = await _set_task_protection(enabled=True)
            initial_delay = _PROTECTION_REFRESH_SECONDS if updated is not False else _PROTECTION_RETRY_SECONDS
            _protection_refresh_task = asyncio.create_task(_renew_task_protection(initial_delay))
        _active_execution_count += 1


async def _release_task_protection() -> None:
    global _active_execution_count, _protection_refresh_task
    async with _execution_lock:
        _active_execution_count -= 1
        if _active_execution_count == 0:
            refresh_task = _protection_refresh_task
            _protection_refresh_task = None
            if refresh_task is not None:
                refresh_task.cancel()
                await _await_task_cancellation(refresh_task)
            await _set_task_protection(enabled=False)


async def _await_task_completion(task: asyncio.Task[None]) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass
    await task


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


def _payload_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    return str(value) if value else ""


class ExecutorDispatchStore(Protocol):
    async def claim(
        self,
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> DispatchAuthority | None: ...

    async def is_current(self, authority: DispatchAuthority) -> bool: ...

    async def terminalize(self, authority: DispatchAuthority, task_ids: list[str]) -> bool: ...

    async def finish(self, authority: DispatchAuthority) -> bool: ...


class PostgresExecutorDispatchStore:
    """Persist dispatch lifecycle at the stable process-owner boundary."""

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
            host=os.environ.get("DB_HOST", "localhost"),
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "tracker"),
            user=os.environ.get("DB_USERNAME", "tracker"),
            password=os.environ.get("DB_PASSWORD", "tracker"),
        )

    def _connect(self) -> PostgresConnection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

    async def claim(
        self,
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> DispatchAuthority | None:
        claimed = await asyncio.to_thread(
            self._claim,
            dispatch_id,
            benchmark_id,
            dispatch,
        )
        if not claimed:
            return None
        return DispatchAuthority(
            dispatch_id=dispatch_id,
            benchmark_id=benchmark_id,
        )

    def _claim(
        self,
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> bool:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE executordispatch AS dispatch
                SET status = 'RUNNING',
                    started_at = CURRENT_TIMESTAMP
                FROM benchmark
                WHERE dispatch.id = %s::uuid
                  AND dispatch.benchmark_id = benchmark.id
                  AND benchmark.id = %s::uuid
                  AND benchmark.status = 'IN_PROGRESS'
                  AND dispatch.executor_release_id = %s
                  AND dispatch.executor_artifact_uri = %s
                  AND dispatch.executor_artifact_digest = %s
                  AND dispatch.executor_protocol_version = %s
                  AND dispatch.status = 'QUEUED'
                RETURNING dispatch.id
                """,
                (
                    dispatch_id,
                    benchmark_id,
                    dispatch.release_id,
                    dispatch.artifact_uri,
                    dispatch.artifact_digest,
                    dispatch.protocol_version,
                ),
            )
            return cursor.fetchone() is not None

    async def is_current(self, authority: DispatchAuthority) -> bool:
        return await asyncio.to_thread(self._is_current, authority)

    def _is_current(self, authority: DispatchAuthority) -> bool:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT dispatch.id
                FROM executordispatch AS dispatch
                JOIN benchmark ON benchmark.id = dispatch.benchmark_id
                WHERE dispatch.id = %s::uuid
                  AND benchmark.status != 'STOPPED'
                  AND dispatch.status = 'RUNNING'
                """,
                (authority.dispatch_id,),
            )
            return cursor.fetchone() is not None

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
                SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid
                  AND benchmark_id = %s::uuid
                  AND status = 'RUNNING'
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
        authority_check_interval: float = 5,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.cache_dir = cache_dir
        self.s3_client = s3_client
        self.python_executable = python_executable
        self.artifact_bucket = artifact_bucket or os.environ.get("EXECUTOR_RELEASE_BUCKET", "agentic-harness")
        self.artifact_prefix = artifact_prefix or os.environ.get(
            "EXECUTOR_RELEASE_PREFIX",
            DEFAULT_EXECUTOR_RELEASE_PREFIX,
        )
        self.authority_check_interval = authority_check_interval
        self.sleep = sleep

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
        is_current: Callable[[], Awaitable[bool]],
    ) -> None:
        if not await is_current():
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
                return_code = await self._wait_with_authority(process, is_current)
            except BaseException:
                await _terminate_process_group(process)
                raise
            if return_code != 0:
                raise RuntimeError(f"Executor for benchmark {authority.benchmark_id} exited with status {return_code}")

    async def _wait_with_authority(
        self,
        process: asyncio.subprocess.Process,
        is_current: Callable[[], Awaitable[bool]],
    ) -> int:
        process_task = asyncio.create_task(process.wait())
        authority_task = asyncio.create_task(self._wait_for_authority_loss(is_current))
        try:
            done, _ = await asyncio.wait(
                (process_task, authority_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if authority_task in done:
                await authority_task
                try:
                    await asyncio.wait_for(
                        asyncio.shield(process_task),
                        timeout=_AUTHORITY_LOSS_GRACE_SECONDS,
                    )
                except TimeoutError:
                    await _terminate_process_group(process)
                    await process_task
                raise DispatchAuthorityLostError("Executor dispatch was superseded")
            authority_task.cancel()
            try:
                await authority_task
            except asyncio.CancelledError:
                pass
            return await process_task
        except BaseException:
            authority_task.cancel()
            process_task.cancel()
            raise

    async def _wait_for_authority_loss(self, is_current: Callable[[], Awaitable[bool]]) -> None:
        while True:
            await self.sleep(self.authority_check_interval)
            if not await is_current():
                return


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


class DeleteAfterAckRedisStreamBroker(RedisStreamBroker):
    """Delete stream entries after Taskiq acknowledges their processing."""

    def _ack_generator(self, id: str, queue_name: str) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            async with Redis(connection_pool=self.connection_pool) as redis_conn:
                await redis_conn.eval(
                    _ACK_AND_DELETE_SCRIPT,
                    1,
                    queue_name,
                    self.consumer_group_name,
                    id,
                )

        return _ack


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = os.environ.get("STABLE_QUEUE_NAME", DEFAULT_STABLE_QUEUE_NAME)
CACHE_DIR = Path(os.environ.get("EXECUTOR_CACHE_DIR", DEFAULT_CACHE_DIR))

broker = DeleteAfterAckRedisStreamBroker(
    url=REDIS_URL,
    queue_name=QUEUE_NAME,
    consumer_group_name=QUEUE_NAME,
    idle_timeout=86400000,
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def _init_worker_observability(*_args: object, **_kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]
    configure_observability()


supervisor = ExecutorSupervisor(CACHE_DIR)
dispatch_store = PostgresExecutorDispatchStore.from_environment()


async def _terminalize_after_failure(
    store: ExecutorDispatchStore,
    authority: DispatchAuthority,
    task_ids: list[str],
) -> None:
    try:
        if not await store.terminalize(authority, task_ids):
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
    executor_dispatch_id: str,
    dispatch: ArtifactDispatch,
    process_payload: ExecutorProcessPayload,
) -> None:
    protection_task = asyncio.create_task(_acquire_task_protection())
    try:
        await asyncio.shield(protection_task)
    except asyncio.CancelledError:
        await _await_task_completion(protection_task)
        release_task = asyncio.create_task(_release_task_protection())
        await _await_task_completion(release_task)
        raise

    try:
        claim_task = asyncio.create_task(
            store.claim(
                executor_dispatch_id,
                process_payload.benchmark_id,
                dispatch,
            )
        )
        try:
            authority = await asyncio.shield(claim_task)
        except asyncio.CancelledError:
            authority = await claim_task
            if authority is not None:
                await _terminalize_after_failure(store, authority, process_payload.verified_task_ids)
            raise

        if authority is None:
            logger.warning(
                "Skipping duplicate, superseded, or non-queued executor dispatch %s",
                executor_dispatch_id,
            )
            return

        try:
            artifact_path = await executor_supervisor.prepare_artifact(dispatch)
            await executor_supervisor.run(
                artifact_path,
                dispatch,
                process_payload=process_payload,
                authority=authority,
                is_current=lambda: store.is_current(authority),
            )
            if not await store.finish(authority):
                logger.warning(
                    "Executor dispatch %s lost authority before successful finish",
                    authority.dispatch_id,
                )
        except asyncio.CancelledError:
            await _terminalize_after_failure(store, authority, process_payload.verified_task_ids)
            raise
        except BaseException:
            await _terminalize_after_failure(store, authority, process_payload.verified_task_ids)
            raise
    finally:
        release_task = asyncio.create_task(_release_task_protection())
        await _await_task_completion(release_task)


@broker.task(EXECUTOR_TASK_NAME)
async def launch_executor(**payload: Unpack[ExecutorPayload]) -> None:
    raw_payload: dict[str, object] = dict(payload)
    with dispatch_observability_context(
        executor_payload_benchmark_id(raw_payload),
        _payload_string(raw_payload, "executor_dispatch_id"),
        _payload_string(raw_payload, "executor_release_id"),
        normalize_executor_telemetry_context(raw_payload.get("telemetry_context_json")),
    ) as child_telemetry_context:
        try:
            dispatch_id = _required_string(raw_payload, "executor_dispatch_id")
            dispatch = ArtifactDispatch.from_payload(raw_payload)
            process_payload = ExecutorProcessPayload.from_payload(
                raw_payload,
                telemetry_context=child_telemetry_context,
            )
            await run_executor_dispatch(
                supervisor,
                dispatch_store,
                executor_dispatch_id=dispatch_id,
                dispatch=dispatch,
                process_payload=process_payload,
            )
            record_dispatch_completion(child_telemetry_context)
        except asyncio.CancelledError:
            record_dispatch_cancellation(child_telemetry_context)
            raise
        except BaseException as error:
            capture_dispatch_error(error, child_telemetry_context)
            raise
