"""Private JSON exchange boundary with no credentials or artifact contents in output."""

import asyncio
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


def write_response(path: Path, response: TrackerResponse) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(response.model_dump_json())
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


def execute(payload: bytes, request_path: Path, report_path: Path, expected_database_target: str) -> int:
    request = TrackerRequest.model_validate_json(payload)
    if request.database_target != expected_database_target:
        raise LifecycleConflict("Expected database target differs from request")
    inputs = [request_path, *(Path(path) for path in request.external_evidence_files)]
    if any(report_path.resolve() == path.resolve() for path in inputs):
        raise LifecycleConflict("Output cannot overwrite immutable request or evidence")
    database_url = os.environ["DATABASE_URL"]
    if make_url(database_url).get_backend_name() != "postgresql":
        raise LifecycleConflict("Relocation requires an explicit PostgreSQL database")
    engine = create_engine(database_url)
    try:
        with Session(engine, expire_on_commit=False) as session:
            boundary = RelocationAWSBoundary(DefaultChainAWSClientProvider(request.region))
            response = asyncio.run(RelocationOperator(session, boundary).execute(request))
            write_response(report_path, response)
    finally:
        engine.dispose()
    return 0
