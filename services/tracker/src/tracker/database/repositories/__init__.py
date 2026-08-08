"""Named persistence repositories for tracker application operations."""

from tracker.database.repositories.benchmark import BenchmarkRepository, TaskPage
from tracker.database.repositories.executor_control import (
    ActiveExecutorReleaseWork,
    EnqueueFailureResolution,
    ExecutorControlRepository,
    MaintenanceStopSummary,
)
from tracker.database.repositories.org import OrgRepository
from tracker.database.repositories.reporting import BenchmarkPage, BenchmarkTaskCounts, ReportingRepository
from tracker.database.repositories.run_control import RetrySelection, RunControlRepository
from tracker.database.repositories.task import TaskRepository
from tracker.database.repositories.task_execution import TaskExecutionRepository

__all__ = [
    "BenchmarkPage",
    "BenchmarkRepository",
    "BenchmarkTaskCounts",
    "ActiveExecutorReleaseWork",
    "EnqueueFailureResolution",
    "ExecutorControlRepository",
    "MaintenanceStopSummary",
    "OrgRepository",
    "ReportingRepository",
    "RetrySelection",
    "RunControlRepository",
    "TaskPage",
    "TaskExecutionRepository",
    "TaskRepository",
]
