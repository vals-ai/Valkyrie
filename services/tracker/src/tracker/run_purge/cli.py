"""Private operator CLI. No HTTP endpoint or ordinary retry overrides."""

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, TypeAdapter
from sqlmodel import Session, create_engine

from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.lifecycle import LifecycleConflict, OperationIdentity
from tracker.lifecycle_evidence import ExternalHostDrain, HostContractObservation, write_report
from tracker.run_purge import PurgeOperator, abandon_runs, build_plan
from tracker.run_purge.contracts import PurgePlan
from tracker.run_purge.providers import AWSProviderBoundary, FenceReceipt


def write_plan(path: Path, plan: BaseModel) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(plan.model_dump_json(indent=2, exclude_none=True))
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Prepare, then purge exact tracker run data behind permanent holds")
    result.add_argument("action", nargs="?", choices=("plan", "prepare", "purge", "resume", "abandon"), default="plan")
    result.add_argument("--apply", action="store_true")
    result.add_argument("--database-url-env", required=True)
    result.add_argument("--expected-database-target", required=True)
    result.add_argument("--identity", type=Path)
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--run", type=UUID, action="append", default=[])
    result.add_argument("--report", type=Path)
    result.add_argument("--host-contract", type=Path)
    result.add_argument("--fence-receipts", type=Path)
    result.add_argument("--external-host-drain", type=Path, action="append", default=[])
    result.add_argument("--external-evidence", type=Path, action="append", default=[])
    return result


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    if options.action != "plan" and not options.apply:
        print("Mutations require --apply", file=sys.stderr)
        return 2
    engine = None
    try:
        inputs = [
            path
            for path in (
                options.identity,
                options.host_contract,
                options.fence_receipts,
                *options.external_host_drain,
                *options.external_evidence,
            )
            if path is not None
        ]
        output = options.plan if options.action == "plan" else options.report
        if options.action != "plan":
            inputs.append(options.plan)
        if output is not None and any(output.resolve() == path.resolve() for path in inputs):
            raise LifecycleConflict("Output path cannot overwrite immutable input evidence")
        database_url = os.environ[options.database_url_env]
        engine = create_engine(database_url)
        with Session(engine, expire_on_commit=False) as session:
            if options.action == "plan":
                if options.identity is None:
                    raise LifecycleConflict("Read-only plan requires an identity file")
                identity = OperationIdentity.model_validate_json(options.identity.read_text())
                if identity.database_target != options.expected_database_target:
                    raise LifecycleConflict("Expected database target differs from identity")
                plan = build_plan(session, identity)
                write_plan(options.plan, plan)
                print(f"Planned {len(plan.runs)} runs; child plan SHA256 {plan.digest()}")
                return 0
            plan = PurgePlan.model_validate_json(options.plan.read_text())
            if plan.identity.database_target != options.expected_database_target:
                raise LifecycleConflict("Expected database target differs from the immutable plan")
            if options.action == "abandon":
                abandoned = abandon_runs(session, plan, tuple(options.run))
                print(f"abandon: {len(abandoned)} deletion holds released; child plan SHA256 {plan.digest()}")
                return 0

            if options.report is None or options.host_contract is None:
                raise LifecycleConflict("Apply requires a report path and a current host contract")
            host = HostContractObservation.model_validate_json(options.host_contract.read_text())
            receipts = (
                TypeAdapter(tuple[FenceReceipt, ...]).validate_json(options.fence_receipts.read_text())
                if options.fence_receipts
                else ()
            )
            external = tuple(
                ExternalHostDrain.model_validate_json(path.read_text()) for path in options.external_host_drain
            )
            evidence = tuple(path.read_bytes() for path in options.external_evidence)
            boundary = AWSProviderBoundary(DefaultChainAWSClientProvider(plan.identity.region), fence_receipts=receipts)
            operator = PurgeOperator(
                session, plan, boundary, host_contract=host, external=external, external_evidence=evidence
            )
            try:
                report = asyncio.run(operator.prepare() if options.action == "prepare" else operator.purge())
            except Exception:
                session.rollback()
                try:
                    write_report(options.report, operator.report())
                except Exception:
                    pass
                raise
            write_report(options.report, report)
            print(f"{options.action}: {len(report.runs)} runs checked; child plan SHA256 {plan.digest()}")
            return 0
    except Exception as error:
        # Provider and SQL exceptions can contain secrets or customer payloads.
        print(
            f"Purge remains incomplete ({type(error).__name__}); inspect durable phases and retry the same plan",
            file=sys.stderr,
        )
        return 2
    finally:
        if engine is not None:
            engine.dispose()
