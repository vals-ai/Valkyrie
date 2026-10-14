"""Provides FakeSource, a RunSource backed by fixtures, so the UI runs and tests without a tracker.

Runs come from fixtures/runs.json, shaped like the SDK's models so captured tracker responses can replace them.
Tasks and logs are generated from a seed to match each run's status counts. Active runs move one step forward per
second of `clock`, and tests pass a clock that never moves, which freezes them.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections import Counter
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from valkyrie.sdk.models import (
    BenchmarkDetails,
    BenchmarkStatus,
    BenchmarkStatusEntry,
    BenchmarkTableRow,
    DocentReadingStatus,
    FailureCategory,
    FetchBenchmarkResponse,
    LogEvent,
    SingleTaskResponse,
    TasksResponse,
    TaskStatus,
    TaskSummary,
)

from valkyrie.tui.source import FINAL_STATUSES, TASK_PAGE_LIMIT, TRIAGE_ORDER, Environment

FIXTURES = Path(__file__).parent / "fixtures"
ACTIVE = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)

# Weighted so failures cluster on a few messages, the way real runs do.
ERRORS = [
    (FailureCategory.INFRASTRUCTURE, "dockerd not ready after 120s", 30),
    (FailureCategory.INFRASTRUCTURE, "Container killed: OOMKilled (memory limit 8 GiB)", 8),
    (FailureCategory.INFRASTRUCTURE, "Sandbox creation timed out after 600s (daytona)", 5),
    (FailureCategory.AGENT, "Agent exited with code 1", 10),
    (FailureCategory.AGENT, "Required output artifact /tmp/valkyrie/patch.diff was not produced", 6),
    (FailureCategory.AGENT, "Agent exceeded the 3600s time limit", 4),
    (FailureCategory.BENCHMARK_SERVICE, "Benchmark service returned 502 Bad Gateway from POST /evaluate", 5),
    (FailureCategory.UNKNOWN, "KeyError: 'test_results'", 2),
]

SWEBENCH_REPOS = [
    "django__django",
    "sympy__sympy",
    "astropy__astropy",
    "scikit-learn__scikit-learn",
    "matplotlib__matplotlib",
    "sphinx-doc__sphinx",
    "pytest-dev__pytest",
    "pydata__xarray",
    "psf__requests",
    "pylint-dev__pylint",
]
TASK_NAMES = {
    "terminal": [
        "fix-git",
        "build-linux-kernel-qemu",
        "crack-7z-hash",
        "sqlite-with-gcov",
        "train-fasttext",
        "configure-git-webserver",
        "hf-model-inference",
        "openssl-selfsigned-cert",
        "polyglot-c-py",
        "kv-store-grpc",
        "write-compressor",
        "path-tracing",
        "chess-best-move",
        "regex-log",
        "count-dataset-tokens",
        "pytorch-model-cli",
        "qemu-startup",
        "fix-pandas-version",
        "nginx-request-logging",
        "password-recovery",
    ],
    "aider": [
        f"{language}-{exercise}"
        for language in ("python", "rust", "go", "cpp", "java", "javascript")
        for exercise in ("bowling", "forth", "bank-account", "react", "zipper", "grep", "poker", "dominoes")
    ],
    "tau": [f"{domain}-{number}" for domain in ("airline", "retail", "telecom") for number in range(50)],
    "mle": [
        "spaceship-titanic",
        "aerial-cactus-identification",
        "denoising-dirty-documents",
        "dog-breed-identification",
        "jigsaw-toxic-comment",
        "leaf-classification",
        "plant-pathology-2020",
        "random-acts-of-pizza",
        "spooky-author-identification",
        "tabular-playground-may-2022",
        "detecting-insults-in-social-commentary",
        "histopathologic-cancer-detection",
        "mlsp-2013-birds",
        "new-york-city-taxi-fare-prediction",
        "siim-isic-melanoma-classification",
        "tweet-sentiment-extraction",
        "us-patent-phrase-matching",
        "ranzcr-clip-catheter-line",
        "google-quest-challenge",
        "text-normalization-challenge",
        "whale-categorization",
        "nomad2018-transparent-conductors",
    ],
}
AGENT_STEPS = [
    "read_file {path}",
    'grep -rn "{symbol}" {directory}',
    "run pytest {test} -x -q",
    "edit {path} (+{added} -{removed})",
    "think: {symbol} returns None when the input is empty, which the caller never checks",
    "list_dir {directory}",
]
PATHS = ["src/core/query.py", "src/core/expressions.py", "lib/utils/encoding.py", "src/io/reader.py"]
TESTS = ["tests/test_query.py", "tests/regression/test_issue.py", "tests/io/test_reader.py"]
SYMBOLS = ["resolve_expression", "_combine", "as_sql", "get_prep_value", "parse_header"]
DIRECTORIES = ["src/core", "lib/utils", "tests", "src/io"]


@dataclass
class _Task:
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None = None
    error: tuple[FailureCategory, str] | None = None
    resolved: bool = False


@dataclass
class _Run:
    row: BenchmarkTableRow
    tasks: list[_Task]
    concurrency: int


class FakeSource:
    environment = Environment("fake", "built-in sample data")

    def __init__(self, *, clock: Callable[[], datetime] | None = None, follow: bool = True, seed: int = 7) -> None:
        """With `follow`, run and log streams stay open like the tracker's. Tests turn it off so streams end."""
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.follow = follow
        self._rng = random.Random(seed)
        self._last_tick = self._clock()
        fixture = json.loads((FIXTURES / "runs.json").read_text())
        offset = self._last_tick - datetime.fromisoformat(fixture["captured_at"])
        rows = [BenchmarkTableRow.model_validate(raw) for raw in fixture["runs"]]
        self._runs = [self._load_run(row, offset) for row in rows]

    async def runs(self, limit: int) -> list[BenchmarkTableRow]:
        self._advance()
        return [self._row(run) for run in self._runs[:limit]]

    async def statuses(self, run_ids: Sequence[UUID]) -> list[BenchmarkStatusEntry]:
        self._advance()
        wanted = set(run_ids)
        return [
            BenchmarkStatusEntry.model_validate(self._row(run).model_dump())
            for run in self._runs
            if run.row.id in wanted
        ]

    async def watch_run(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]:
        run = self._run(run_id)
        last: FetchBenchmarkResponse | None = None
        while True:
            self._advance()
            snapshot = self._snapshot(run)
            if snapshot != last:
                yield snapshot
                last = snapshot
            if not self.follow or run.row.status in FINAL_STATUSES:
                return
            await asyncio.sleep(1)

    async def tasks(self, run_id: UUID) -> TasksResponse:
        self._advance()
        run = self._run(run_id)
        ordered = sorted(run.tasks, key=lambda task: TRIAGE_ORDER.index(task.status))
        summaries = [self._summary(run, task) for task in ordered[:TASK_PAGE_LIMIT]]
        return TasksResponse(tasks=summaries, total_count=len(run.tasks))

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse:
        run = self._run(run_id)
        task = self._task(run, task_id)
        summary = self._summary(run, task)
        evaluation = {"resolved": task.resolved} if task.status == TaskStatus.FINISHED else None
        exit_reason = "exit_code_1" if task.error and task.error[0] == FailureCategory.AGENT else None
        return SingleTaskResponse(
            **summary.model_dump(), evaluation_result=evaluation, agent_caused_exit_reason=exit_reason
        )

    async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]:
        self._advance()
        run = self._run(run_id)
        return self._log(run, self._task(run, task_id))

    async def stream_logs(self, run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]:
        run = self._run(run_id)
        task = self._task(run, task_id)
        emitted = 0
        while True:
            self._advance()
            events = self._log(run, task)
            for event in events[emitted:]:
                yield event
            emitted = len(events)
            if not self.follow or task.status not in ACTIVE:
                return
            await asyncio.sleep(0.8)

    async def close(self) -> None:
        return None

    def _load_run(self, row: BenchmarkTableRow, offset: timedelta) -> _Run:
        row = row.model_copy(
            update={
                "started_at": row.started_at + offset,
                "finished_at": row.finished_at and row.finished_at + offset,
            }
        )
        rng = random.Random(str(row.id))
        ids = _task_ids(row.name, row.total_tasks, rng)
        rng.shuffle(ids)
        end = row.finished_at or self._last_tick
        tasks: list[_Task] = []
        for status_name, count in row.task_state_counts.items():
            status = TaskStatus(status_name)
            for _ in range(count):
                task = _Task(ids.pop(), status, row.started_at)
                if status in ACTIVE:
                    task.started_at = end - timedelta(seconds=rng.randint(20, 1200))
                elif status != TaskStatus.PENDING:
                    task.started_at = row.started_at + (end - row.started_at) * rng.random() * 0.8
                    task.finished_at = min(end, task.started_at + timedelta(seconds=rng.randint(180, 1500)))
                if status == TaskStatus.ERROR:
                    task.error = _pick_error(rng)
                task.resolved = rng.random() < (row.final_score or 0.65)
                tasks.append(task)
        active = sum(task.status in ACTIVE for task in tasks)
        return _Run(row, tasks, concurrency=max(active, 1))

    def _advance(self) -> None:
        """Move live runs forward one step per elapsed second, at most five steps per call."""
        now = self._clock()
        steps = min(int((now - self._last_tick).total_seconds()), 5)
        if steps <= 0:
            return
        self._last_tick = now
        for _ in range(steps):
            for run in self._runs:
                if run.row.status not in FINAL_STATUSES:
                    self._step(run, now)

    def _step(self, run: _Run, now: datetime) -> None:
        stopping = run.row.status == BenchmarkStatus.STOPPING
        for task in run.tasks:
            if task.status not in ACTIVE or self._rng.random() > 0.06:
                continue
            if stopping:
                task.status, task.finished_at = TaskStatus.STOPPED, now
            elif task.status == TaskStatus.BUILDING and self._rng.random() < 0.05:
                task.status, task.finished_at = TaskStatus.ERROR, now
                task.error = (FailureCategory.INFRASTRUCTURE, ERRORS[0][1])
            elif task.status == TaskStatus.EVALUATING:
                failed = self._rng.random() < 0.1
                task.status, task.finished_at = (TaskStatus.ERROR if failed else TaskStatus.FINISHED), now
                task.error = _pick_error(self._rng) if failed else None
            else:
                task.status = ACTIVE[ACTIVE.index(task.status) + 1]
        pending = [task for task in run.tasks if task.status == TaskStatus.PENDING]
        free = run.concurrency - sum(task.status in ACTIVE for task in run.tasks)
        if not stopping:
            for task in pending[: max(free, 0)]:
                task.status, task.started_at = TaskStatus.BUILDING, now
        if not any(task.status in ACTIVE or task.status == TaskStatus.PENDING for task in run.tasks):
            done = BenchmarkStatus.STOPPED if stopping else BenchmarkStatus.FINISHED
            resolved = sum(task.resolved and task.status == TaskStatus.FINISHED for task in run.tasks)
            score = None if stopping else round(resolved / len(run.tasks), 3)
            run.row = run.row.model_copy(update={"status": done, "finished_at": now, "final_score": score})

    def _run(self, run_id: UUID) -> _Run:
        return next(run for run in self._runs if run.row.id == run_id)

    def _task(self, run: _Run, task_id: str) -> _Task:
        return next(task for task in run.tasks if task.task_id == task_id)

    def _row(self, run: _Run) -> BenchmarkTableRow:
        counts = Counter(task.status.value for task in run.tasks)
        finished = counts[TaskStatus.FINISHED.value] + counts[TaskStatus.ERROR.value]
        return run.row.model_copy(update={"task_state_counts": dict(counts), "finished_tasks": finished})

    def _snapshot(self, run: _Run) -> FetchBenchmarkResponse:
        row = self._row(run)
        done = row.status == BenchmarkStatus.FINISHED
        return FetchBenchmarkResponse(
            benchmark_name=row.name,
            benchmark_id=row.id,
            details=BenchmarkDetails(
                status=row.status,
                started_at=row.started_at,
                total_tasks=row.total_tasks,
                finished_tasks=row.finished_tasks,
                task_breakdown={TaskStatus(name): count for name, count in row.task_state_counts.items()},
                docent_reading_status=DocentReadingStatus.DONE if done else DocentReadingStatus.IDLE,
                docent_reading_url=f"https://docent.example.com/runs/{row.id}" if done else None,
            ),
            s3_bucket_url=f"s3://valkyrie-benchmarks/{row.id}",
            label=row.label,
            final_score=row.final_score,
            error_message=row.error_message,
        )

    def _summary(self, run: _Run, task: _Task) -> TaskSummary:
        category, message = task.error or (None, None)
        return TaskSummary(
            id=uuid5(NAMESPACE_URL, f"{run.row.id}/{task.task_id}"),
            task_id=task.task_id,
            status=task.status,
            started_at=task.started_at,
            finished_at=task.finished_at,
            error_message=message,
            failure_category=category,
        )

    def _log(self, run: _Run, task: _Task) -> list[LogEvent]:
        """Return the task's log so far. Each status only appends lines, so a live stream can resume by line count."""
        if task.status == TaskStatus.PENDING:
            return []
        rng = random.Random(f"{run.row.id}/{task.task_id}")
        lines = [
            (0, f"Creating sandbox (daytona) from image ghcr.io/vals-ai/{run.row.name}:{task.task_id}"),
            (38, f"Sandbox sb-{rng.randrange(16**6):06x} ready in {rng.uniform(20, 60):.1f}s"),
            (41, f"Uploading agent bundle {run.row.agent_name} ({rng.uniform(4, 30):.1f} MB)"),
        ]
        category = task.error[0] if task.error else None
        if category == FailureCategory.INFRASTRUCTURE and task.error:
            lines.append((150, f"ERROR {task.error[1]}"))
        if task.status == TaskStatus.BUILDING or category == FailureCategory.INFRASTRUCTURE:
            return self._events(task, lines)
        lines.append(
            (52, f"Starting agent: {run.row.agent_name} --model {run.row.model} --problem /tmp/valkyrie/problem.md")
        )
        steps = rng.randint(8, 30)
        if task.status == TaskStatus.IN_PROGRESS:
            elapsed = (self._clock() - task.started_at).total_seconds()
            steps = min(steps, max(1, int((elapsed - 52) / 20)))
        for step in range(steps):
            template = rng.choice(AGENT_STEPS)
            text = template.format(
                path=rng.choice(PATHS),
                test=rng.choice(TESTS),
                symbol=rng.choice(SYMBOLS),
                directory=rng.choice(DIRECTORIES),
                added=rng.randint(1, 40),
                removed=rng.randint(0, 12),
            )
            lines.append((60 + step * 20, f"[agent] step {step + 1}: {text}"))
        if task.status == TaskStatus.IN_PROGRESS:
            return self._events(task, lines)
        offset = 60 + steps * 20
        if category == FailureCategory.AGENT:
            lines.append((offset, f"ERROR {task.error[1] if task.error else ''}"))
            return self._events(task, lines)
        lines.append((offset, "Agent exited with code 0"))
        lines.append((offset + 4, "Collecting artifacts from /tmp/valkyrie (3 files)"))
        lines.append((offset + 9, "Evaluating"))
        if task.status == TaskStatus.EVALUATING:
            return self._events(task, lines)
        if task.error:
            lines.append((offset + 60, f"ERROR {task.error[1]}"))
        elif task.status == TaskStatus.STOPPED:
            lines.append((offset + 10, "Task stopped by user"))
        else:
            lines.append((offset + 70, f"Evaluation finished: {'resolved' if task.resolved else 'unresolved'}"))
        return self._events(task, lines)

    @staticmethod
    def _events(task: _Task, lines: list[tuple[int, str]]) -> list[LogEvent]:
        return [
            LogEvent(timestamp=task.started_at + timedelta(seconds=offset), message=message, task_id=task.task_id)
            for offset, message in lines
        ]


def _task_ids(benchmark: str, count: int, rng: random.Random) -> list[str]:
    if benchmark.startswith("swebench"):
        ids: set[str] = set()
        while len(ids) < count:
            ids.add(f"{rng.choice(SWEBENCH_REPOS)}-{rng.randint(1000, 26000)}")
        return sorted(ids)
    names = next((names for prefix, names in TASK_NAMES.items() if benchmark.startswith(prefix)), None)
    if names is None:
        return [f"task-{index:04}" for index in range(count)]
    return [
        names[index % len(names)] + (f"-{index // len(names) + 1}" if index >= len(names) else "")
        for index in range(count)
    ]


def _pick_error(rng: random.Random) -> tuple[FailureCategory, str]:
    category, message, _ = rng.choices(ERRORS, weights=[weight for *_, weight in ERRORS])[0]
    return category, message
