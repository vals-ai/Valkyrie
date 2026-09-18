"""Private fixed-argument CLI. Reports contain no row or credential payloads."""

import asyncio
import os
import tempfile
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlmodel import Session, create_engine

from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer import TransferOperator
from tracker.run_transfer.contracts import TransferRequest
from tracker.run_transfer.providers import ProfileClients, TransferAWSBoundary


def execute(
    payload: bytes,
    request_path: Path,
    report_path: Path,
    source_url: str,
    destination_url: str,
    expected_source: str,
    expected_destination: str,
    source_profile: str,
    destination_profile: str,
    journal: Path,
) -> int:
    request = TransferRequest.model_validate_json(payload)
    if (request.plan.source_identity.database_target, request.plan.destination_identity.database_target) != (
        expected_source,
        expected_destination,
    ):
        raise LifecycleConflict("Explicit paired database targets differ")
    if any(make_url(url).get_backend_name() != "postgresql" for url in (source_url, destination_url)):
        raise LifecycleConflict("Two explicit PostgreSQL databases are required")
    inputs = [request_path, *(Path(path) for path in request.external_evidence_files)]
    if any(report_path.resolve() == path.resolve() for path in inputs):
        raise LifecycleConflict("Report cannot overwrite immutable input")
    source_engine, destination_engine = create_engine(source_url), create_engine(destination_url)
    try:
        with (
            Session(source_engine, expire_on_commit=False) as source,
            Session(destination_engine, expire_on_commit=False) as destination,
        ):
            boundary = TransferAWSBoundary(
                ProfileClients(source_profile, request.plan.source_identity.region),
                ProfileClients(destination_profile, request.plan.destination_identity.region),
                journal,
            )
            response = asyncio.run(TransferOperator(source, destination, boundary).execute(request))
        descriptor, name = tempfile.mkstemp(prefix=f".{report_path.name}.", dir=report_path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "w") as output:
                os.fchmod(output.fileno(), 0o600)
                output.write(response.model_dump_json() + "\n")
                output.flush()
                os.fsync(output.fileno())
            temporary.replace(report_path)
        finally:
            temporary.unlink(missing_ok=True)
    finally:
        source_engine.dispose()
        destination_engine.dispose()
    return 0
