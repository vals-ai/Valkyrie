from functools import partial
from typing import Any

from pytest import MonkeyPatch
from sqlmodel import Session, select

from main import app
from tracker.benchmark_service import BenchmarkService
from tracker.database.models import AgentContractRequest, BenchmarkStatus, Task, TaskStatus
from tracker.database.session import get_session
from tracker.types import FinalScoreResponse, RetrieveTaskResponse, StartRunRequest, VerifyTaskIdsResponse
from tracker.utils import process_benchmark, resume_benchmark, stop_benchmark


class TestStopAndResume:
    @staticmethod
    async def _mock_install_dependencies(*args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    async def _mock_run_agent(*args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    async def _mock_upload_contract(*args: Any, **kwargs: Any) -> None:
        pass

    async def _mock_request_verify_task_ids(
        self, *args: Any, task_ids: list[str], **kwargs: Any
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    @staticmethod
    async def _mock_request_retrieve_task(*args: Any, **kwargs: Any) -> RetrieveTaskResponse:
        return RetrieveTaskResponse(
            docker_image="test-image:latest",
            problem_statement="Test problem statement",
            request_setup=False,
            cwd="/testbed",
        )

    @staticmethod
    async def _mock_request_final_score(
        *args: Any, evaluation_results: dict[str, Any], **kwargs: Any
    ) -> FinalScoreResponse:
        tasks_evaluated = list(evaluation_results.keys())
        return FinalScoreResponse(
            tasks_evaluated=tasks_evaluated,
            final_score=50.0,
            metadata={"resolved_tasks": [], "unresolved_tasks": tasks_evaluated},
        )

    async def test_stop_and_resume(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
    ):
        """
        Tests stop and resume when some tasks have already completed.

        Test Cases:
            - Start benchmark with 5 tasks
            - 2 tasks are completed (finished), 3 are still starting
            - Stop benchmark - 3 tasks are stopped (starting -> stopped)
            - Resume benchmark - only the 3 tasks that are stopped should be resumed
            - Process benchmark - all 5 tasks should have evaluation results after completion
        """

        def get_test_session():
            yield database_session

        app.dependency_overrides[get_session] = get_test_session

        task_ids: list[str] = [
            "astropy__astropy-12907",
            "astropy__astropy-13033",
            "django__django-11066",
            "django__django-12325",
            "django__django-12858",
        ]

        start_run_request = StartRunRequest(
            benchmark_name="swebench",
            contract=contract,
            concurrency=2,
            task_ids=task_ids,
        )

        benchmark_row = BenchmarkService.start_run_request_to_benchmark_object(start_run_request)
        database_session.add(benchmark_row)
        database_session.commit()

        monkeypatch.setattr("tracker.utils.engine", database_session.bind)
        monkeypatch.setattr("tracker.utils.upload_agent_artifacts", self._mock_upload_contract)
        monkeypatch.setattr("tracker.utils.install_agent_dependencies", self._mock_install_dependencies)
        monkeypatch.setattr("tracker.utils.run_agent", self._mock_run_agent)
        monkeypatch.setattr(BenchmarkService, "request_retrieve_task", self._mock_request_retrieve_task)
        monkeypatch.setattr(BenchmarkService, "request_final_score", self._mock_request_final_score)

        # Create tasks - 2 tasks are finished, 3 tasks are starting
        finished_task_ids = task_ids[:2]
        starting_task_ids = task_ids[2:]

        for task_id in finished_task_ids:
            task_row = Task(task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            database_session.add(task_row)

        for task_id in starting_task_ids:
            task_row = Task(task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.STARTING)
            database_session.add(task_row)

        database_session.commit()

        # Stop benchmark - only tasks that are starting become stopped
        await stop_benchmark(benchmark_row, database_session, force=False)

        # Verify: 2 tasks are finished, 3 tasks are stopped
        finished_count = len(
            database_session.exec(
                select(Task).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.FINISHED)
            ).all()
        )
        stopped_count = len(
            database_session.exec(
                select(Task).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STOPPED)
            ).all()
        )
        assert finished_count == 2
        assert stopped_count == 3

        # Set benchmark to stopped (simulating first run completion of all tasks)
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Resume benchmark - mock verify to return only the starting task IDs
        monkeypatch.setattr(
            BenchmarkService,
            "request_verify_task_ids",
            partial(self._mock_request_verify_task_ids, task_ids=starting_task_ids),
        )

        verified_task_ids = await resume_benchmark(benchmark_row, database_session, start_run_request.benchmark_service)

        # Only 3 tasks should be verified for resume (the 3 tasks that are stopped)
        assert len(verified_task_ids) == 3
        assert set(verified_task_ids) == set(starting_task_ids)

        # Run process_benchmark to complete the remaining tasks (the 3 tasks that are starting)
        await process_benchmark(
            start_run_request.model_dump(),
            str(benchmark_row.id),
            verified_task_ids,
        )

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.FINISHED, benchmark_row.error_message
