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
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol
from urllib.parse import urlparse

import boto3
import psycopg2  # pyright: ignore[reportMissingModuleSource]
from psycopg2.extensions import connection as PostgresConnection  # pyright: ignore[reportMissingModuleSource]
from taskiq_redis import RedisStreamBroker

logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOL_VERSION = "1"
DEFAULT_QUEUE_NAME = "valkyrie-stable"
DEFAULT_CACHE_DIR = "/var/cache/valkyrie-executors"
ECS_AGENT_URI = os.environ.get("ECS_AGENT_URI")
_PROTECTION_EXPIRY_MINUTES = 1440
_active_execution_count = 0
_execution_lock = asyncio.Lock()


async def _set_task_protection(*, enabled: bool) -> None:
    if not ECS_AGENT_URI:
        return
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

    try:
        await asyncio.to_thread(update)
    except Exception:
        logger.exception("Failed to set ECS task protection to %s", enabled)


async def _acquire_task_protection() -> None:
    global _active_execution_count
    async with _execution_lock:
        if _active_execution_count == 0:
            await _set_task_protection(enabled=True)
        _active_execution_count += 1


async def _release_task_protection() -> None:
    global _active_execution_count
    async with _execution_lock:
        _active_execution_count -= 1
        if _active_execution_count == 0:
            await _set_task_protection(enabled=False)


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
        digest = _required_string(payload, "executor_artifact_digest").lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("executor_artifact_digest must be a 64-character SHA-256 digest")
        protocol_version = _required_string(payload, "executor_protocol_version")
        if protocol_version != SUPPORTED_PROTOCOL_VERSION:
            raise ValueError(f"Unsupported executor protocol version: {protocol_version}")
        return cls(
            release_id=_required_string(payload, "executor_release_id"),
            artifact_uri=_required_string(payload, "executor_artifact_uri"),
            artifact_digest=digest,
            protocol_version=protocol_version,
        )


class ExecutorDispatchStore(Protocol):
    async def claim(self, dispatch_id: str, benchmark_id: str, dispatch: ArtifactDispatch) -> bool: ...

    async def finish(self, dispatch_id: str, *, succeeded: bool) -> None: ...


class PostgresExecutorDispatchStore:
    """Persist dispatch lifecycle at the stable process-owner boundary."""

    def __init__(self, *, host: str, port: str, dbname: str, user: str, password: str) -> None:
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

    async def claim(self, dispatch_id: str, benchmark_id: str, dispatch: ArtifactDispatch) -> bool:
        return await asyncio.to_thread(self._claim, dispatch_id, benchmark_id, dispatch)

    def _claim(self, dispatch_id: str, benchmark_id: str, dispatch: ArtifactDispatch) -> bool:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE executordispatch
                SET status = 'RUNNING', started_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid
                  AND benchmark_id = %s::uuid
                  AND executor_release_id = %s
                  AND executor_artifact_uri = %s
                  AND executor_artifact_digest = %s
                  AND executor_protocol_version = %s
                  AND status = 'QUEUED'
                RETURNING id
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

    async def finish(self, dispatch_id: str, *, succeeded: bool) -> None:
        await asyncio.to_thread(self._finish, dispatch_id, succeeded=succeeded)

    def _finish(self, dispatch_id: str, *, succeeded: bool) -> None:
        status = "FINISHED" if succeeded else "FAILED"
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE executordispatch
                SET status = %s, finished_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid AND status = 'RUNNING'
                RETURNING id
                """,
                (status, dispatch_id),
            )
            if cursor.fetchone() is None:
                raise RuntimeError(f"Executor dispatch {dispatch_id} was not running during terminalization")


def _required_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Executor artifact URI must use s3://: {uri}")
    if not key:
        raise ValueError("Executor artifact URI must contain an S3 key")
    return parsed.netloc, key


def verify_file_digest(path: Path, expected_digest: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    actual_digest = digest.hexdigest()
    if actual_digest != expected_digest:
        raise ValueError(f"Executor artifact digest mismatch: expected {expected_digest}, got {actual_digest}")


class ExecutorSupervisor:
    def __init__(
        self,
        cache_dir: Path,
        *,
        s3_client: S3Client | None = None,
        python_executable: str = sys.executable,
    ) -> None:
        self.cache_dir = cache_dir
        self.s3_client = s3_client
        self.python_executable = python_executable

    async def prepare_artifact(self, dispatch: ArtifactDispatch) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.cache_dir / f"{dispatch.artifact_digest}.pex"
        try:
            verify_file_digest(artifact_path, dispatch.artifact_digest)
            artifact_path.chmod(artifact_path.stat().st_mode | 0o111)
            return artifact_path
        except (OSError, ValueError):
            pass

        bucket, key = parse_s3_uri(dispatch.artifact_uri)
        temporary_fd, temporary_name = tempfile.mkstemp(
            dir=self.cache_dir,
            prefix=f".{dispatch.artifact_digest}.",
            suffix=".tmp",
        )
        os.close(temporary_fd)
        temporary_path = Path(temporary_name)
        try:
            client = self.s3_client or boto3.client("s3")

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
        dispatch: ArtifactDispatch,
        *,
        start_benchmark_request_json: Mapping[str, object],
        benchmark_id_str: str,
        verified_task_ids: list[str],
    ) -> None:
        await _acquire_task_protection()
        try:
            await self._run(
                dispatch,
                start_benchmark_request_json=start_benchmark_request_json,
                benchmark_id_str=benchmark_id_str,
                verified_task_ids=verified_task_ids,
            )
        finally:
            await _release_task_protection()

    async def _run(
        self,
        dispatch: ArtifactDispatch,
        *,
        start_benchmark_request_json: Mapping[str, object],
        benchmark_id_str: str,
        verified_task_ids: list[str],
    ) -> None:
        artifact_path = await self.prepare_artifact(dispatch)
        payload = {
            "start_benchmark_request_json": dict(start_benchmark_request_json),
            "benchmark_id_str": benchmark_id_str,
            "verified_task_ids": verified_task_ids,
            "executor_release_id": dispatch.release_id,
            "executor_artifact_uri": dispatch.artifact_uri,
            "executor_artifact_digest": dispatch.artifact_digest,
            "executor_protocol_version": dispatch.protocol_version,
        }
        with tempfile.TemporaryDirectory(dir=self.cache_dir, prefix=".dispatch-") as temporary_directory:
            payload_path = Path(temporary_directory) / "payload.json"
            payload_path.write_text(json.dumps(payload))
            logger.info(
                "Launching benchmark %s with release=%s digest=%s protocol=%s",
                benchmark_id_str,
                dispatch.release_id,
                dispatch.artifact_digest,
                dispatch.protocol_version,
            )
            process = await asyncio.create_subprocess_exec(
                self.python_executable,
                str(artifact_path),
                str(payload_path),
                start_new_session=True,
            )
            try:
                return_code = await process.wait()
            except BaseException as error:
                if not isinstance(error, asyncio.CancelledError):
                    raise
                await _terminate_process_group(process)
                raise
            if return_code != 0:
                raise RuntimeError(f"Executor for benchmark {benchmark_id_str} exited with status {return_code}")


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


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = os.environ.get("STABLE_QUEUE_NAME", DEFAULT_QUEUE_NAME)
CACHE_DIR = Path(os.environ.get("EXECUTOR_CACHE_DIR", DEFAULT_CACHE_DIR))

broker = RedisStreamBroker(
    url=REDIS_URL,
    queue_name=QUEUE_NAME,
    consumer_group_name=QUEUE_NAME,
    idle_timeout=86400000,
)
supervisor = ExecutorSupervisor(CACHE_DIR)
dispatch_store = PostgresExecutorDispatchStore.from_environment()


async def run_executor_dispatch(
    executor_supervisor: ExecutorSupervisor,
    store: ExecutorDispatchStore,
    *,
    executor_dispatch_id: str | None,
    dispatch: ArtifactDispatch,
    start_benchmark_request_json: Mapping[str, object],
    benchmark_id_str: str,
    verified_task_ids: list[str],
) -> None:
    if executor_dispatch_id is not None:
        claimed = await store.claim(executor_dispatch_id, benchmark_id_str, dispatch)
        if not claimed:
            logger.warning("Skipping duplicate or non-queued executor dispatch %s", executor_dispatch_id)
            return

    try:
        await executor_supervisor.run(
            dispatch,
            start_benchmark_request_json=start_benchmark_request_json,
            benchmark_id_str=benchmark_id_str,
            verified_task_ids=verified_task_ids,
        )
    except BaseException:
        if executor_dispatch_id is not None:
            try:
                await store.finish(executor_dispatch_id, succeeded=False)
            except Exception:
                logger.exception("Failed to terminalize executor dispatch %s", executor_dispatch_id)
        raise
    else:
        if executor_dispatch_id is not None:
            await store.finish(executor_dispatch_id, succeeded=True)


@broker.task("tracker.utils:process_benchmark")
async def launch_executor(
    start_benchmark_request_json: dict[str, object],
    benchmark_id_str: str,
    verified_task_ids: list[str],
    executor_dispatch_id: str | None = None,
    executor_release_id: str | None = None,
    executor_artifact_uri: str | None = None,
    executor_artifact_digest: str | None = None,
    executor_protocol_version: str | None = None,
) -> None:
    dispatch = ArtifactDispatch.from_payload(
        {
            "executor_release_id": executor_release_id,
            "executor_artifact_uri": executor_artifact_uri,
            "executor_artifact_digest": executor_artifact_digest,
            "executor_protocol_version": executor_protocol_version,
        }
    )
    await run_executor_dispatch(
        supervisor,
        dispatch_store,
        executor_dispatch_id=executor_dispatch_id,
        dispatch=dispatch,
        start_benchmark_request_json=start_benchmark_request_json,
        benchmark_id_str=benchmark_id_str,
        verified_task_ids=verified_task_ids,
    )
