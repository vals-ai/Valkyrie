"""Save each run's sandboxes and fill in what the provider billed for them."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Event, Thread
from uuid import UUID
from zoneinfo import ZoneInfo

from benchmark_service import DaytonaProviderConfig, SandboxProviderConfig
from daytona_analytics_api_client_async import ApiClient as AnalyticsApiClient
from daytona_analytics_api_client_async import Configuration as AnalyticsConfiguration
from daytona_analytics_api_client_async import UsageApi
from daytona_api_client_async import ApiClient, ApiKeysApi, ConfigApi, Configuration
from sqlalchemy import Connection, Engine, and_
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, col, select

from tracker.aws.runtime import AWSResources
from tracker.local.resources import LocalResources
from tracker.database.models import Benchmark, BenchmarkStatus, Org, RunCost, SandboxRecord
from tracker.database.session import engine
from tracker.logging import get_logger
from tracker.config import AUTH_REQUIRED
from tracker.scheduler.store import sandbox_cost_sweep_lock
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax

logger = get_logger(__name__)

SANDBOX_COST_TYPE = "sandbox"
_PRICED_PROVIDERS = ("daytona",)
_FINISHED_RUN_STATUSES = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)
USAGE_WINDOW_MARGIN = timedelta(minutes=5)
USAGE_READ_TIMEOUT_SECONDS = 60.0
_SWEEP_INTERVAL_SECONDS = 3600.0
_LOGGED_RUN_IDS = 50
_SHUTDOWN_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class SandboxLink:
    """What a task attempt knows about itself when it asks the provider for a sandbox."""

    benchmark_id: UUID
    provider: str


def record_sandbox(link: SandboxLink, sandbox_id: str) -> None:
    """Save the sandbox against its run; a failed write is logged so it never fails or leaks the sandbox."""
    try:
        with Session(engine) as session:
            session.exec(
                insert(SandboxRecord)
                .values(provider=link.provider, sandbox_id=sandbox_id, benchmark_id=link.benchmark_id)
                .on_conflict_do_nothing(index_elements=["provider", "sandbox_id"])
            )
            session.commit()
    except Exception:
        logger.exception(
            "Failed to save run sandbox", extra={"sandbox_id": sandbox_id, "benchmark_id": str(link.benchmark_id)}
        )


@dataclass(frozen=True)
class SandboxUsage:
    """One sandbox's usage record from Daytona's analytics API."""

    sandbox_id: str
    total_price: float
    last_end: datetime | None


def add_run_cost(session: Session, *, benchmark_id: UUID, provider: str, usage: SandboxUsage) -> bool:
    """Insert the sandbox's cost row unless one exists, and return whether this call wrote it."""
    result = session.exec(
        insert(RunCost)
        .values(
            benchmark_id=benchmark_id,
            cost_type=SANDBOX_COST_TYPE,
            source_id=usage.sandbox_id,
            provider=provider,
            amount_usd=usage.total_price,
        )
        .on_conflict_do_nothing(
            index_elements=["provider", "source_id"],
            index_where=col(RunCost.cost_type) == SANDBOX_COST_TYPE,
        )
    )
    return result.rowcount == 1


def select_sandboxes_to_price(session: Session) -> list[tuple[SandboxRecord, Benchmark]]:
    """Return each sandbox that has no cost row yet and whose usage can be final, with its run."""
    rows = session.exec(
        select(SandboxRecord, Benchmark)
        .join(Benchmark, col(Benchmark.id) == col(SandboxRecord.benchmark_id))
        .outerjoin(
            RunCost,
            and_(
                col(RunCost.benchmark_id) == col(SandboxRecord.benchmark_id),
                col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                col(RunCost.source_id) == col(SandboxRecord.sandbox_id),
            ),
        )
        .where(col(SandboxRecord.provider).in_(_PRICED_PROVIDERS))
        .where(col(RunCost.source_id).is_(None))
        .where(col(Benchmark.status).in_(_FINISHED_RUN_STATUSES))
    ).all()
    return [(sandbox_record, benchmark) for sandbox_record, benchmark in rows]


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.utcoffset() is not None else moment.replace(tzinfo=ZoneInfo("UTC"))


UsageReader = Callable[[Org, Benchmark, datetime, datetime], Awaitable[dict[str, SandboxUsage]]]


async def _run_provider_config(org: Org, benchmark: Benchmark) -> SandboxProviderConfig:
    """Load the provider settings the run itself used, from a local secrets file or the organization's secrets."""
    from tracker.aws.resolver import deployment_aws_runtime
    from tracker.aws.services import CloudRuntimeFactory
    from tracker.local.runtime import LocalRuntimeFactory

    arguments = benchmark.arguments
    if arguments.environment == "local":
        runtime = LocalRuntimeFactory.create_runtime(
            arguments.properties,
            org.id,
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    else:
        runtime = CloudRuntimeFactory.create_runtime(
            deployment_aws_runtime(org, arguments.properties),
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    return await runtime.get_sandbox_provider_config()


async def read_daytona_usage(org: Org, benchmark: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
    """Read every usage record of the run's Daytona organization in one request."""
    config = await _run_provider_config(org, benchmark)
    if not isinstance(config, DaytonaProviderConfig):
        raise TypeError(f"Expected a Daytona provider secret, got {type(config).__name__}")
    authorization = f"Bearer {config.DAYTONA_API_KEY}"

    async with ApiClient(
        Configuration(host=config.DAYTONA_API_URL), header_name="Authorization", header_value=authorization
    ) as api_client:
        analytics_url = (await ConfigApi(api_client).config_controller_get_config()).analytics_api_url
        organization_id = config.DAYTONA_ORGANIZATION_ID
        if not organization_id:
            api_key = await ApiKeysApi(api_client).get_current_api_key()
            organization_id = api_key.additional_properties.get("organizationId")
    if not analytics_url or not organization_id:
        raise RuntimeError("Daytona did not return an analytics URL and organization for this secret")
    analytics_url = validate_service_url_syntax(analytics_url)
    validate_custom_service_destination(
        analytics_url, org_name=org.name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False
    )

    async with AnalyticsApiClient(
        AnalyticsConfiguration(host=analytics_url), header_name="Authorization", header_value=authorization
    ) as analytics_client:
        records = await UsageApi(analytics_client).organization_organization_id_usage_sandbox_get(
            organization_id=organization_id, var_from=_rfc3339(start), to=_rfc3339(end)
        )
    return {
        record.sandbox_id: SandboxUsage(
            sandbox_id=record.sandbox_id,
            total_price=float(record.total_price),
            last_end=_parse_time(record.last_end),
        )
        for record in records
        if record.sandbox_id and record.total_price is not None
    }


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


ProviderConfigKey = tuple[UUID, str, str | None, AWSResources | LocalResources | None]


def _group_by_provider_config(
    candidates: Iterable[tuple[SandboxRecord, Benchmark]],
) -> dict[ProviderConfigKey, list[tuple[SandboxRecord, Benchmark]]]:
    """Group sandboxes by organization and by the settings a run loads its provider secret with."""
    groups: dict[ProviderConfigKey, list[tuple[SandboxRecord, Benchmark]]] = defaultdict(list)
    for sandbox_record, benchmark in candidates:
        arguments = benchmark.arguments
        key = (benchmark.org_id, arguments.environment, arguments.sandbox_provider_secret_name, arguments.properties)
        groups[key].append((sandbox_record, benchmark))
    return groups


async def sweep_sandbox_costs(
    bind: Engine | Connection,
    *,
    now: datetime,
    read_usage: UsageReader = read_daytona_usage,
    read_timeout_seconds: float = USAGE_READ_TIMEOUT_SECONDS,
) -> int:
    """Price every ready sandbox Daytona has a usage record for, reading usage once per provider-config group."""
    with Session(bind) as session:
        groups = _group_by_provider_config(select_sandboxes_to_price(session))
        org_ids = {key[0] for key in groups}
        orgs = {org_id: session.get(Org, org_id) for org_id in org_ids}

    written = 0
    benchmark_ids_written: dict[str, None] = {}
    for key, members in groups.items():
        org = orgs[key[0]]
        if org is None:
            continue
        runs = {benchmark.id: benchmark for _, benchmark in members}
        reader_run = min(runs.values(), key=lambda run: _as_utc(run.started_at))
        start = _as_utc(reader_run.started_at) - USAGE_WINDOW_MARGIN
        try:
            async with asyncio.timeout(read_timeout_seconds):
                usage = await read_usage(org, reader_run, start, now)
        except Exception as error:
            logger.warning(
                "Sandbox usage unavailable",
                extra={
                    "org_id": str(org.id),
                    "runs": len(runs),
                    "sandboxes": len(members),
                    "error_type": type(error).__name__,
                    "http_status": getattr(error, "status", None),
                },
            )
            continue
        priced = [
            (sandbox_record, usage[sandbox_record.sandbox_id])
            for sandbox_record, _ in members
            if sandbox_record.sandbox_id in usage
            and (last_end := usage[sandbox_record.sandbox_id].last_end) is not None
            and last_end + USAGE_WINDOW_MARGIN <= now
        ]
        with Session(bind) as session:
            for sandbox_record, usage_record in priced:
                if add_run_cost(
                    session,
                    benchmark_id=sandbox_record.benchmark_id,
                    provider=sandbox_record.provider,
                    usage=usage_record,
                ):
                    written += 1
                    benchmark_id = str(sandbox_record.benchmark_id)
                    benchmark_ids_written[benchmark_id] = None
            session.commit()
    logger.info(
        "Sandbox cost sweep complete",
        extra={
            "cost_rows_written": written,
            "runs_written": len(benchmark_ids_written),
            "benchmark_ids_written": list(benchmark_ids_written)[:_LOGGED_RUN_IDS],
        },
    )
    return written


async def sweep_sandbox_costs_once(engine: Engine, *, now: datetime) -> int | None:
    """Run the sweep unless another Tracker worker holds the lock, and return None when skipped."""
    async with sandbox_cost_sweep_lock(engine) as acquired:
        if not acquired:
            logger.debug("Sandbox cost sweep skipped because another worker is running it")
            return None
        return await sweep_sandbox_costs(engine, now=now)


def run_sandbox_cost_loop(stop_event: Event, *, interval_seconds: float = _SWEEP_INTERVAL_SECONDS) -> None:
    """Sweep immediately and then once per interval until shutdown."""
    while not stop_event.is_set():
        try:
            asyncio.run(sweep_sandbox_costs_once(engine, now=datetime.now(ZoneInfo("UTC"))))
        except Exception:
            logger.exception("Sandbox cost sweep failed")
        stop_event.wait(interval_seconds)


class AutomaticSandboxCost:
    """Own the Tracker process's hourly sandbox-cost thread."""

    def __init__(
        self,
        *,
        interval_seconds: float = _SWEEP_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_sandbox_cost_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="sandbox-cost",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            logger.error("Sandbox cost thread did not stop before shutdown")
