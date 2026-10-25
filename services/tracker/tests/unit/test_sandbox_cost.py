"""Tests for saving run sandboxes and pricing them from Daytona usage records."""

import asyncio
import threading
from pathlib import Path
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import DaytonaProviderConfig, ImageSource, Resources
from daytona_analytics_api_client_async.exceptions import ApiException
from sqlmodel import Session, col, select

import tracker.sandbox as sandbox_module
import tracker.sandbox_cost as sandbox_cost
from tracker.aws.runtime import AWSResources
from tracker.local.resources import LocalResources
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    LocalBenchmarkArguments,
    Org,
    RunCost,
    SandboxRecord,
    Task,
)
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.sandbox_cost import (
    USAGE_WINDOW_MARGIN,
    AutomaticSandboxCost,
    SandboxLink,
    SandboxUsage,
    select_sandboxes_to_price,
    sweep_sandbox_costs,
    add_run_cost,
    read_daytona_usage,
)

NOW = datetime(2026, 10, 21, 18, tzinfo=UTC)


def _run(
    session: Session,
    *,
    status: BenchmarkStatus,
    org: Org | None = None,
    hours_ago: float = 6,
    secret: str | None = None,
    region: str | None = None,
    local_root: str | None = None,
) -> tuple[Benchmark, Task]:
    benchmark = make_benchmark(
        status=status, started_at=NOW - timedelta(hours=hours_ago), session=None, **({"org_id": org.id} if org else {})
    )
    if secret is not None:
        benchmark.arguments = benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": secret})
    if local_root is not None:
        benchmark.arguments = LocalBenchmarkArguments(
            contract=benchmark.arguments.contract,
            concurrency=1,
            sandbox_provider="daytona",
            sandbox_provider_secret_name="test-provider-secret",
            properties=LocalResources(data_root=Path(local_root)),
        )
    if region is not None:
        resources = AWSResources(region=region, s3_bucket="bucket", log_group="logs", log_retention_days=7)
        benchmark.arguments = benchmark.arguments.model_copy(update={"properties": resources})
    task = make_task(benchmark, "task-1")
    session.add_all([benchmark, task])
    session.commit()

    return benchmark, task


def _other_org(session: Session, name: str) -> Org:
    org = Org(name=name, storage_key=f"{name}-bucket")
    session.add(org)
    session.commit()
    return org


def _link(benchmark: Benchmark) -> SandboxLink:
    return SandboxLink(benchmark_id=benchmark.id, provider="daytona")


def _save_sandbox(session: Session, benchmark: Benchmark, sandbox_id: str) -> None:
    session.add(SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=benchmark.id))
    session.commit()


def _usage(sandbox_id: str, price: float, *, last_end: datetime | None = NOW - timedelta(hours=2)) -> SandboxUsage:
    return SandboxUsage(
        sandbox_id=sandbox_id,
        total_price=price,
        last_end=last_end,
    )


async def _open_sandbox(sandbox_id: str, link: SandboxLink, *, fail: bool) -> None:
    async with create_sandbox(
        provider=AsyncMock(),
        sandbox_name="task-1",
        source=ImageSource(image="ghcr.io/vals/swebench:latest"),
        resources=Resources(vcpu=1, memory=2, disk=3),
        creation_semaphore=asyncio.Semaphore(1),
        creation_state=SandboxCreationState(cleanup_pending=False),
        sandbox_link=link,
    ) as sandbox:
        assert sandbox.id == sandbox_id
        if fail:
            raise RuntimeError("agent setup failed")


class TestRecordSandbox:
    """The link row create_sandbox writes once the provider names the sandbox."""

    async def test_saves_errored_and_retried_attempts(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        created = iter([AsyncMock(id="sandbox-attempt-1"), AsyncMock(id="sandbox-attempt-2")])

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return next(created)

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", AsyncMock(return_value=True))

        with pytest.raises(RuntimeError, match="agent setup failed"):
            await _open_sandbox("sandbox-attempt-1", _link(benchmark), fail=True)
        await _open_sandbox("sandbox-attempt-2", _link(benchmark), fail=False)

        rows = database_session.exec(select(SandboxRecord).order_by(col(SandboxRecord.sandbox_id))).all()

        assert [row.sandbox_id for row in rows] == ["sandbox-attempt-1", "sandbox-attempt-2"]
        assert all(row.benchmark_id == benchmark.id for row in rows)

    async def test_failed_write_still_deletes_the_sandbox(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        delete = AsyncMock(return_value=True)

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return AsyncMock(id="sandbox-1")

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", delete)
        monkeypatch.setattr(sandbox_cost, "engine", None)

        await _open_sandbox("sandbox-1", _link(benchmark), fail=False)

        delete.assert_awaited_once()
        assert database_session.exec(select(SandboxRecord)).all() == []

    async def test_cancel_during_the_link_write_still_deletes_the_sandbox(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        events: list[str] = []

        def record_delete(*_args: Any, **_kwargs: Any) -> bool:
            events.append("deleted")
            return True

        delete = AsyncMock(side_effect=record_delete)
        write_started = threading.Event()
        release_write = threading.Event()

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return AsyncMock(id="sandbox-1")

        def blocked_write(*_args: Any, **_kwargs: Any) -> None:
            write_started.set()
            release_write.wait(5)
            events.append("written")

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", delete)
        monkeypatch.setattr(sandbox_module, "record_sandbox", blocked_write)

        opening = asyncio.create_task(_open_sandbox("sandbox-1", _link(benchmark), fail=False))
        await asyncio.to_thread(write_started.wait, 5)
        opening.cancel()
        await asyncio.sleep(0.05)
        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await opening

        assert events == ["written", "deleted"]


class TestSelectSandboxesToPrice:
    """Which sandboxes the hourly sweep asks Daytona about."""

    @pytest.mark.parametrize("status", [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED])
    def test_takes_only_finished_runs_without_a_cost_row(
        self, database_session: Session, status: BenchmarkStatus
    ) -> None:
        running, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        finished, _ = _run(database_session, status=status)
        _save_sandbox(database_session, running, "sb-running")
        _save_sandbox(database_session, finished, "sb-unpriced")
        _save_sandbox(database_session, finished, "sb-priced")
        add_run_cost(database_session, benchmark_id=finished.id, provider="daytona", usage=_usage("sb-priced", 1))
        database_session.commit()

        selected = select_sandboxes_to_price(database_session)

        assert [row.sandbox_id for row, _ in selected] == ["sb-unpriced"]


class TestSweepSandboxCosts:
    """One sweep across ready sandboxes."""

    async def test_writes_each_cost_once_from_the_run_start_window(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        _save_sandbox(database_session, benchmark, "sb-1")
        _save_sandbox(database_session, benchmark, "sb-2")
        windows: list[tuple[object, datetime, datetime]] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            windows.append((run.id, start, end))

            return {
                "sb-1": _usage("sb-1", 0.75),
                "sb-2": _usage("sb-2", 0.25),
                "sb-other-run": _usage("sb-other-run", 9.0),
            }

        bind = database_session.get_bind()
        first = await sweep_sandbox_costs(bind, now=NOW, read_usage=read_usage)
        second = await sweep_sandbox_costs(bind, now=NOW, read_usage=read_usage)

        assert (first, second) == (2, 0)
        assert windows == [(benchmark.id, NOW - timedelta(hours=6) - USAGE_WINDOW_MARGIN, NOW)]

        rows = database_session.exec(select(RunCost).order_by(col(RunCost.source_id))).all()

        assert [(row.source_id, row.amount_usd, row.benchmark_id) for row in rows] == [
            ("sb-1", 0.75, benchmark.id),
            ("sb-2", 0.25, benchmark.id),
        ]

    async def test_waits_until_the_usage_record_has_settled(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        last_end = NOW - timedelta(minutes=1)
        _save_sandbox(database_session, benchmark, "sb-1")
        windows: list[tuple[datetime, datetime]] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            windows.append((start, end))

            return {"sb-1": _usage("sb-1", 0.5, last_end=last_end)}

        bind = database_session.get_bind()
        early = await sweep_sandbox_costs(bind, now=NOW, read_usage=read_usage)
        later = await sweep_sandbox_costs(bind, now=NOW + USAGE_WINDOW_MARGIN, read_usage=read_usage)

        assert (early, later) == (0, 1)
        assert [end for _, end in windows] == [NOW, NOW + USAGE_WINDOW_MARGIN]

    async def test_waits_for_a_closed_usage_record(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.STOPPED)
        _save_sandbox(database_session, benchmark, "sb-1")
        answers = iter(
            [
                {},
                {"sb-1": _usage("sb-1", 0.5, last_end=None)},
                {"sb-1": _usage("sb-1", 0.55, last_end=NOW - timedelta(minutes=1))},
                {"sb-1": _usage("sb-1", 0.6)},
            ]
        )
        windows: list[tuple[datetime, datetime]] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            windows.append((start, end))

            return next(answers)

        bind = database_session.get_bind()
        written = [await sweep_sandbox_costs(bind, now=NOW, read_usage=read_usage) for _ in range(4)]

        assert written == [0, 0, 0, 1]
        assert windows[0] == (NOW - timedelta(hours=6) - USAGE_WINDOW_MARGIN, NOW)
        assert database_session.exec(select(RunCost.amount_usd)).all() == [0.6]

    async def test_duplicate_from_another_tracker_keeps_its_cost_and_writes_the_rest(
        self, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        _save_sandbox(database_session, benchmark, "sb-a")
        _save_sandbox(database_session, benchmark, "sb-b")

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            with Session(database_session.get_bind()) as other_session:
                add_run_cost(other_session, benchmark_id=run.id, provider="daytona", usage=_usage("sb-a", 0.75))
                other_session.commit()
            return {"sb-a": _usage("sb-a", 1.0), "sb-b": _usage("sb-b", 0.5)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 1
        rows = database_session.exec(select(RunCost.source_id, RunCost.amount_usd).order_by(col(RunCost.source_id)))
        assert rows.all() == [("sb-a", 0.75), ("sb-b", 0.5)]

    def test_non_sandbox_cost_without_a_source_id_is_saved(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        database_session.add(RunCost(benchmark_id=benchmark.id, cost_type="search", provider="tavily", amount_usd=0.01))
        database_session.commit()

        rows = database_session.exec(select(RunCost.cost_type, RunCost.source_id)).all()

        assert rows == [("search", None)]

    def test_non_sandbox_costs_sharing_a_source_id_are_both_saved(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        database_session.add_all(
            [
                RunCost(
                    benchmark_id=benchmark.id, cost_type="search", source_id="req-1", provider="tavily", amount_usd=0.01
                )
                for _ in range(2)
            ]
        )
        database_session.commit()

        rows = database_session.exec(select(RunCost.source_id)).all()

        assert rows == ["req-1", "req-1"]

    @pytest.mark.parametrize("error", [ApiException(status=429), RuntimeError("provider unavailable")])
    async def test_an_error_skips_that_org_with_one_warning_and_the_next_org_is_priced(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch, error: Exception
    ) -> None:
        failing_org = _other_org(database_session, "failing")
        healthy_org = _other_org(database_session, "healthy")
        failing_runs = [_run(database_session, status=BenchmarkStatus.FINISHED, org=failing_org)[0] for _ in range(3)]
        healthy, _ = _run(database_session, status=BenchmarkStatus.FINISHED, org=healthy_org)
        for index, run in enumerate(failing_runs):
            _save_sandbox(database_session, run, f"sb-failing-{index}")
        _save_sandbox(database_session, healthy, "sb-healthy")
        requests: list[object] = []
        warning = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "warning", warning)

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            requests.append(org.id)
            if org.id == failing_org.id:
                raise error
            return {"sb-healthy": _usage("sb-healthy", 0.5)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 1
        assert sorted(requests, key=str) == sorted([failing_org.id, healthy_org.id], key=str)
        warning.assert_called_once()
        assert warning.call_args.args == ("Sandbox usage unavailable",)
        extra = warning.call_args.kwargs["extra"]
        assert extra["org_id"] == str(failing_org.id)
        assert (extra["runs"], extra["sandboxes"]) == (3, 3)
        assert extra["http_status"] == getattr(error, "status", None)
        assert database_session.exec(select(RunCost.source_id)).all() == ["sb-healthy"]

    async def test_the_complete_log_has_no_run_ids_when_no_run_needs_pricing(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        info = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "info", info)

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW)

        assert written == 0
        info.assert_called_once_with(
            "Sandbox cost sweep complete",
            extra={"cost_rows_written": 0, "runs_written": 0, "benchmark_ids_written": []},
        )

    async def test_the_complete_log_names_the_runs_that_were_priced(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        priced, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        unpriced, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        _save_sandbox(database_session, priced, "sb-1")
        _save_sandbox(database_session, unpriced, "sb-2")
        info = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "info", info)

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            return {"sb-1": _usage("sb-1", 0.5)}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        info.assert_called_once_with(
            "Sandbox cost sweep complete",
            extra={"cost_rows_written": 1, "runs_written": 1, "benchmark_ids_written": [str(priced.id)]},
        )

    async def test_a_stalled_read_times_out_and_the_next_org_is_priced(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stalled_org = _other_org(database_session, "stalled")
        healthy_org = _other_org(database_session, "healthy")
        first, _ = _run(database_session, status=BenchmarkStatus.FINISHED, org=stalled_org)
        second, _ = _run(database_session, status=BenchmarkStatus.FINISHED, org=healthy_org)
        _save_sandbox(database_session, first, "sb-1")
        _save_sandbox(database_session, second, "sb-2")
        warning = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "warning", warning)

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            if org.id == stalled_org.id:
                await asyncio.Event().wait()
            return {"sb-2": _usage("sb-2", 0.5)}

        written = await sweep_sandbox_costs(
            database_session.get_bind(), now=NOW, read_usage=read_usage, read_timeout_seconds=0.01
        )

        assert written == 1
        assert warning.call_args.kwargs["extra"]["error_type"] == "TimeoutError"


class TestOneUsageReadPerOrg:
    """Reads Daytona once for each organization, however many runs it has to price."""

    @pytest.mark.parametrize("run_count", [1, 5])
    async def test_reads_once_from_the_earliest_run_and_prices_each_run(
        self, database_session: Session, run_count: int
    ) -> None:
        runs = [
            _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6 + index)[0]
            for index in range(run_count)
        ]
        for index, run in enumerate(runs):
            _save_sandbox(database_session, run, f"sb-{index}")
        windows: list[tuple[datetime, datetime]] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            windows.append((start, end))
            return {f"sb-{index}": _usage(f"sb-{index}", 0.5) for index in range(run_count)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == run_count
        assert windows == [(NOW - timedelta(hours=6 + run_count - 1) - USAGE_WINDOW_MARGIN, NOW)]
        rows = database_session.exec(select(RunCost.benchmark_id, RunCost.source_id).order_by(col(RunCost.source_id)))
        assert rows.all() == [(run.id, f"sb-{index}") for index, run in enumerate(runs)]

    async def test_reads_once_per_org(self, database_session: Session) -> None:
        orgs = [_other_org(database_session, "org-a"), _other_org(database_session, "org-b")]
        reads: list[tuple[object, datetime]] = []
        for org in orgs:
            for index in range(3):
                run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, org=org, hours_ago=6 + index)
                _save_sandbox(database_session, run, f"sb-{org.name}-{index}")

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            reads.append((org.id, start))
            return {f"sb-{org.name}-{index}": _usage(f"sb-{org.name}-{index}", 0.5) for index in range(3)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 6
        assert sorted(reads, key=str) == sorted(
            [(org.id, NOW - timedelta(hours=8) - USAGE_WINDOW_MARGIN) for org in orgs], key=str
        )

    async def test_runs_of_one_org_with_different_provider_secrets_are_read_separately(
        self, database_session: Session
    ) -> None:
        first, _ = _run(database_session, status=BenchmarkStatus.FINISHED, secret="secret-a", hours_ago=7)
        second, _ = _run(database_session, status=BenchmarkStatus.FINISHED, secret="secret-b", hours_ago=8)
        _save_sandbox(database_session, first, "sb-a")
        _save_sandbox(database_session, second, "sb-b")
        reads: list[tuple[object, datetime]] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            reads.append((run.id, start))
            return {"sb-a": _usage("sb-a", 0.5)} if run.id == first.id else {"sb-b": _usage("sb-b", 0.25)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 2
        assert set(reads) == {
            (first.id, NOW - timedelta(hours=7) - USAGE_WINDOW_MARGIN),
            (second.id, NOW - timedelta(hours=8) - USAGE_WINDOW_MARGIN),
        }

    async def test_runs_of_one_org_with_different_resource_settings_are_read_separately(
        self, database_session: Session
    ) -> None:
        first, _ = _run(database_session, status=BenchmarkStatus.FINISHED, region="us-east-1")
        second, _ = _run(database_session, status=BenchmarkStatus.FINISHED, region="us-west-2")
        _save_sandbox(database_session, first, "sb-a")
        _save_sandbox(database_session, second, "sb-b")
        reads: list[object] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            reads.append(run.id)
            return {"sb-a": _usage("sb-a", 0.5)} if run.id == first.id else {"sb-b": _usage("sb-b", 0.25)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 2
        assert set(reads) == {first.id, second.id}

    async def test_local_runs_of_one_org_with_different_resource_settings_are_read_separately(
        self, database_session: Session
    ) -> None:
        first, _ = _run(database_session, status=BenchmarkStatus.FINISHED, local_root="/data/a")
        second, _ = _run(database_session, status=BenchmarkStatus.FINISHED, local_root="/data/b")
        _save_sandbox(database_session, first, "sb-a")
        _save_sandbox(database_session, second, "sb-b")
        reads: list[object] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            reads.append(run.id)
            return {"sb-a": _usage("sb-a", 0.5)} if run.id == first.id else {"sb-b": _usage("sb-b", 0.25)}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert written == 2
        assert set(reads) == {first.id, second.id}

    async def test_an_org_with_nothing_to_price_is_not_read(self, database_session: Session) -> None:
        finished, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        _save_sandbox(database_session, finished, "sb-priced")
        add_run_cost(database_session, benchmark_id=finished.id, provider="daytona", usage=_usage("sb-priced", 1))
        database_session.commit()
        reads: list[object] = []

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            reads.append(org.id)
            return {}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        assert (written, reads) == (0, [])

    async def test_prices_the_listed_settled_sandboxes_and_waits_for_the_rest(self, database_session: Session) -> None:
        runs = [_run(database_session, status=BenchmarkStatus.FINISHED)[0] for _ in range(3)]
        for index, run in enumerate(runs):
            _save_sandbox(database_session, run, f"sb-{index}")
        answers = iter(
            [
                {
                    "sb-0": _usage("sb-0", 0.5),
                    "sb-1": _usage("sb-1", 0.5, last_end=NOW - timedelta(minutes=1)),
                },
                {
                    "sb-1": _usage("sb-1", 0.6, last_end=NOW - timedelta(minutes=1)),
                    "sb-2": _usage("sb-2", 0.7),
                },
            ]
        )

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            return next(answers)

        bind = database_session.get_bind()
        first = await sweep_sandbox_costs(bind, now=NOW, read_usage=read_usage)
        second = await sweep_sandbox_costs(bind, now=NOW + USAGE_WINDOW_MARGIN, read_usage=read_usage)

        assert (first, second) == (1, 2)
        rows = database_session.exec(select(RunCost.source_id, RunCost.amount_usd).order_by(col(RunCost.source_id)))
        assert rows.all() == [("sb-0", 0.5), ("sb-1", 0.6), ("sb-2", 0.7)]

    async def test_the_complete_log_counts_each_priced_run_once(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        _save_sandbox(database_session, run, "sb-1")
        _save_sandbox(database_session, run, "sb-2")
        info = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "info", info)

        async def read_usage(org: Org, run: Benchmark, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
            return {"sb-1": _usage("sb-1", 0.5), "sb-2": _usage("sb-2", 0.5)}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, read_usage=read_usage)

        info.assert_called_once_with(
            "Sandbox cost sweep complete",
            extra={"cost_rows_written": 2, "runs_written": 1, "benchmark_ids_written": [str(run.id)]},
        )


class TestReadDaytonaUsage:
    """The Daytona reader that the hourly sweep uses."""

    async def test_refuses_an_analytics_url_on_an_internal_host(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)

        async def provider_config(_org: Org, _run: Benchmark) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://daytona.test/api",
                DAYTONA_TARGET="us",
                DAYTONA_ORGANIZATION_ID="org-1",
            )

        class FakeConfigApi:
            def __init__(self, _client: object) -> None:
                pass

            async def config_controller_get_config(self) -> SimpleNamespace:
                return SimpleNamespace(analytics_api_url="http://169.254.169.254/latest")

        monkeypatch.setattr(sandbox_cost, "_run_provider_config", provider_config)
        monkeypatch.setattr(sandbox_cost, "ConfigApi", FakeConfigApi)
        monkeypatch.setattr(sandbox_cost, "AUTH_REQUIRED", True)

        def not_shared(_org_name: str) -> bool:
            return False

        monkeypatch.setattr("tracker.secret_access.may_use_shared_secrets", not_shared)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        with pytest.raises(ValueError):
            await read_daytona_usage(org, benchmark, NOW - timedelta(hours=1), NOW)

    async def test_reads_the_org_from_the_api_key_and_parses_each_record(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        requests: list[dict[str, Any]] = []

        async def provider_config(_org: Org, _run: Benchmark) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key", DAYTONA_API_URL="https://daytona.test/api", DAYTONA_TARGET="us"
            )

        class FakeConfigApi:
            def __init__(self, _client: object) -> None:
                pass

            async def config_controller_get_config(self) -> SimpleNamespace:
                return SimpleNamespace(analytics_api_url="https://analytics.daytona.test")

        class FakeApiKeysApi:
            def __init__(self, _client: object) -> None:
                pass

            async def get_current_api_key(self) -> SimpleNamespace:
                return SimpleNamespace(additional_properties={"organizationId": "org-from-key"})

        class FakeUsageApi:
            def __init__(self, _client: object) -> None:
                pass

            async def organization_organization_id_usage_sandbox_get(self, **kwargs: Any) -> list[SimpleNamespace]:
                requests.append(kwargs)

                return [
                    SimpleNamespace(
                        sandbox_id="sb-1",
                        total_price=0.25,
                        first_start="2026-10-21T19:30:26Z",
                        last_end="2026-10-21T19:31:00Z",
                    ),
                    SimpleNamespace(sandbox_id="", total_price=9.0, first_start=None, last_end=None),
                    SimpleNamespace(sandbox_id="sb-2", total_price=None, first_start=None, last_end=None),
                ]

        monkeypatch.setattr(sandbox_cost, "_run_provider_config", provider_config)
        monkeypatch.setattr(sandbox_cost, "ConfigApi", FakeConfigApi)
        monkeypatch.setattr(sandbox_cost, "ApiKeysApi", FakeApiKeysApi)
        monkeypatch.setattr(sandbox_cost, "UsageApi", FakeUsageApi)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, benchmark, NOW - timedelta(hours=1), NOW)

        assert requests == [
            {
                "organization_id": "org-from-key",
                "var_from": (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                "to": NOW.isoformat().replace("+00:00", "Z"),
            }
        ]
        assert usage == {
            "sb-1": SandboxUsage(
                sandbox_id="sb-1",
                total_price=0.25,
                last_end=datetime(2026, 10, 21, 19, 31, tzinfo=UTC),
            )
        }


class TestAutomaticSandboxCost:
    """The Tracker thread that runs the sweep."""

    def test_sweeps_on_start_and_stops_on_shutdown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        swept = threading.Event()

        async def sweep(_bind: object, *, now: datetime) -> int:
            swept.set()

            return 0

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs_once", sweep)
        thread = AutomaticSandboxCost(interval_seconds=60, shutdown_timeout_seconds=5)

        thread.start()
        assert swept.wait(5)
        thread.stop()

        assert not [alive for alive in threading.enumerate() if alive.name == "sandbox-cost"]
