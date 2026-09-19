"""Private JSON exchange boundary with no credentials or artifact contents in output."""

import asyncio
import json
import os
import tempfile
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlmodel import Session, create_engine

from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.lifecycle import LifecycleConflict
from tracker.run_relocation import RelocationOperator
from tracker.run_relocation.providers import RelocationAWSBoundary
from tracker.storage_migration_exchange import TrackerRequest, TrackerResponse


def write_private(path: Path, payload: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(payload)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def write_response(path: Path, response: TrackerResponse) -> None:
    write_private(path, response.model_dump_json())


def failure_path(report_path: Path) -> Path:
    return report_path.with_name(report_path.name + ".failure")


def write_failure(path: Path, request: TrackerRequest, error: Exception) -> None:
    write_private(
        path,
        json.dumps(
            {
                "schema_version": 1,
                "nonce": str(request.nonce),
                "action": request.action,
                "outcome": "incomplete",
                "run_ids": [str(run_id) for run_id in request.run_ids],
                "reason": str(error) if isinstance(error, LifecycleConflict) else type(error).__name__,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def record_failure(report_path: Path, request: TrackerRequest, error: Exception) -> None:
    """A failed failure record annotates the refusal; it never replaces it."""
    try:
        write_failure(failure_path(report_path), request, error)
    except Exception as write_error:
        error.add_note(f"Failure record was not written ({type(write_error).__name__})")


def execute(payload: bytes, request_path: Path, report_path: Path, expected_database_target: str) -> int:
    request = TrackerRequest.model_validate_json(payload)
    if request.database_target != expected_database_target:
        raise LifecycleConflict("Expected database target differs from request")
    inputs = [request_path, *(Path(path) for path in request.external_evidence_files)]
    outputs = [report_path, failure_path(report_path)]
    if any(output.resolve() == path.resolve() for output in outputs for path in inputs):
        raise LifecycleConflict("Output cannot overwrite immutable request or evidence")
    database_url = os.environ["DATABASE_URL"]
    if make_url(database_url).get_backend_name() != "postgresql":
        raise LifecycleConflict("Relocation requires an explicit PostgreSQL database")
    engine = create_engine(database_url)
    try:
        with Session(engine, expire_on_commit=False) as session:
            boundary = RelocationAWSBoundary(DefaultChainAWSClientProvider(request.region))
            try:
                response = asyncio.run(RelocationOperator(session, boundary).execute(request))
            except Exception as error:
                record_failure(report_path, request, error)
                raise
            write_response(report_path, response)
    finally:
        engine.dispose()
    return 0
