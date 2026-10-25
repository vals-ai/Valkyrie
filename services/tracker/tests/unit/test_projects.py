"""Projects created once and linked to each run at start and by retag.

Run: uv run pytest tests/unit/test_projects.py
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, col, select

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_runner, get_current_starter
from tracker.database.models import (
    AgentContractRequest,
    AWSBenchmarkArguments,
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    Project,
    RunProject,
    SpendType,
    Task,
)
from tracker.types import StartBenchmarkRequest

pytestmark = pytest.mark.usefixtures("mapped_providers")

client = TestClient(app)

_CUSTOMER_PROJECT = {
    "handle": "google-report-xyz",
    "name": "Google Report XYZ",
    "spend_type": "customer",
    "subcategory": "report",
    "customer": "google",
    "extra_tags": {"engagement": "q4"},
}


@pytest.fixture
def active_executor_release(database_session: Session) -> None:
    """Provide the admission target a run start needs."""
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
    )
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    database_session.add(release)
    database_session.flush()
    admission.release_id = release.id
    database_session.add(admission)
    database_session.commit()


@pytest.fixture
def router_bench(database_session: Session) -> Project:
    project = Project(
        org_id=TEST_ORG_ID,
        handle="router-bench",
        name="Router Bench",
        spend_type=SpendType.DEVELOPMENT,
        subcategory="new_benchmark_generation",
    )
    database_session.add(project)
    database_session.commit()
    return project


@pytest.fixture
def google_report(database_session: Session) -> Project:
    project = Project.model_validate({**_CUSTOMER_PROJECT, "org_id": TEST_ORG_ID})
    database_session.add(project)
    database_session.commit()
    return project


async def _verify_single_task_id(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
    return VerifyTaskIdsResponse(task_ids=["task_0"])


def _start_request(contract: AgentContractRequest, project: str | None) -> dict[str, Any]:
    request = StartBenchmarkRequest(
        contract=contract, benchmark_name="swebench", sandbox_provider_secret_name="provider-secret", project=project
    )
    return request.model_dump(mode="json", exclude={"project"} if project is None else set())


def _saved_run(database_session: Session, contract: AgentContractRequest, org_id: UUID = TEST_ORG_ID) -> Benchmark:
    benchmark = Benchmark(
        org_id=org_id,
        name="swebench",
        status=BenchmarkStatus.FINISHED,
        finished_at=datetime.now(UTC),
        arguments=AWSBenchmarkArguments(contract=contract, concurrency=1),
    )
    database_session.add(benchmark)
    database_session.commit()
    return benchmark


def _other_org(database_session: Session) -> Org:
    org = Org(name="other-org")
    database_session.add(org)
    database_session.commit()
    return org


def _project_fields(project: Project) -> dict[str, Any]:
    return {
        "handle": project.handle,
        "name": project.name,
        "spend_type": project.spend_type.value,
        "subcategory": project.subcategory,
        "customer": project.customer,
        "extra_tags": project.extra_tags,
    }


def _link_rows(database_session: Session, run_id: UUID) -> list[RunProject]:
    database_session.expire_all()
    return list(
        database_session.exec(
            select(RunProject).where(RunProject.benchmark_id == run_id).order_by(col(RunProject.id))
        ).all()
    )


@pytest.mark.parametrize(
    "project",
    [
        {
            "handle": "new-benchmark",
            "name": "New Benchmark",
            "spend_type": "development",
            "subcategory": "new_benchmark_generation",
        },
        {
            "handle": "launch-runs",
            "name": "Launch Runs",
            "spend_type": "development",
            "subcategory": "new_benchmark_launch_runs",
        },
        {
            "handle": "benchmark-update",
            "name": "Benchmark Update",
            "spend_type": "development",
            "subcategory": "post_launch_benchmark_update",
        },
        {
            "handle": "leaderboard",
            "name": "Leaderboard",
            "spend_type": "internal",
            "subcategory": "unpaid_leaderboard_run",
        },
        {
            "handle": "other-unpaid",
            "name": "Other Unpaid",
            "spend_type": "internal",
            "subcategory": "other_unpaid_run",
        },
        {
            "handle": "intern-program",
            "name": "Fellows/Interns",
            "spend_type": "internal",
            "subcategory": "fellows_interns",
        },
        {"handle": "blog", "name": "Blog", "spend_type": "internal", "subcategory": "blog"},
        {
            "handle": "internal-misc",
            "name": "Internal Miscellaneous",
            "spend_type": "internal",
            "subcategory": "miscellaneous",
        },
        _CUSTOMER_PROJECT,
        {
            "handle": "google-paid",
            "name": "Google Paid Run",
            "spend_type": "customer",
            "subcategory": "paid_run",
            "customer": "google",
        },
        {
            "handle": "google-eval",
            "name": "Google Eval Server",
            "spend_type": "customer",
            "subcategory": "eval_server",
            "customer": "google",
        },
        {
            "handle": "google-platform",
            "name": "Google Platform",
            "spend_type": "customer",
            "subcategory": "platform",
            "customer": "google",
        },
        {
            "handle": "a" * 64,
            "name": "Long",
            "spend_type": "development",
            "subcategory": "new_benchmark_generation",
        },
        {
            "handle": "blog-extra-tag",
            "name": "Blog",
            "spend_type": "internal",
            "subcategory": "blog",
            "extra_tags": {"extra_tags": "x"},
        },
    ],
    ids=[
        "new-benchmark-generation",
        "new-benchmark-launch-runs",
        "post-launch-benchmark-update",
        "unpaid-leaderboard-run",
        "other-unpaid-run",
        "intern-program",
        "blog",
        "internal-miscellaneous",
        "report",
        "paid-run",
        "eval-server",
        "platform",
        "handle-64-chars",
        "extra-tag-named-extra-tags",
    ],
)
def test_create_saves_the_project_with_its_spend_fields(
    monkeypatch: MonkeyPatch, database_session: Session, project: dict[str, Any]
) -> None:
    creator = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: creator)

    response = client.post(
        "/projects", json={**project, "handle": f"  {project['handle']} ", "name": f" {project['name']}  "}
    )

    assert response.status_code == 200, response.text
    (row,) = database_session.exec(select(Project)).all()
    expected: dict[str, Any] = {"customer": None, "extra_tags": {}, **project}
    assert _project_fields(row) == expected
    assert (row.org_id, row.created_by_email) == (TEST_ORG_ID, "alice@vals.ai")
    assert {key: response.json()[key] for key in expected} == expected


_BLOG = {"handle": "blog", "name": "Blog", "spend_type": "internal", "subcategory": "blog"}
_HANDLE_RULE = "must be lowercase letters, digits and hyphens, at most 64 characters, e.g. my-project"


@pytest.mark.parametrize(
    ("project", "message"),
    [
        (
            {**_BLOG, "subcategory": "report"},
            "subcategory 'report' is not allowed for spend type 'internal'; allowed: unpaid_leaderboard_run, other_unpaid_run, fellows_interns, blog, miscellaneous",
        ),
        (
            {**_BLOG, "subcategory": "blogs", "spend_type": "development"},
            "subcategory 'blogs' is not allowed for spend type 'development'; allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
        ),
        (
            {**_BLOG, "subcategory": "miscellaneous", "spend_type": "development"},
            "subcategory 'miscellaneous' is not allowed for spend type 'development'; allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
        ),
        (
            {**_CUSTOMER_PROJECT, "subcategory": "miscellaneous"},
            "subcategory 'miscellaneous' is not allowed for spend type 'customer'; allowed: report, paid_run, eval_server, platform",
        ),
        ({key: value for key, value in _BLOG.items() if key != "subcategory"}, "Field required"),
        ({**_BLOG, "subcategory": None}, "Input should be a valid string"),
        ({**_CUSTOMER_PROJECT, "customer": None}, "spend type 'customer' requires customer"),
        ({**_CUSTOMER_PROJECT, "customer": "   "}, "spend type 'customer' requires customer"),
        ({**_BLOG, "spend_type": "research"}, "Input should be 'development', 'internal'"),
        ({key: value for key, value in _BLOG.items() if key != "handle"}, "Field required"),
        ({**_BLOG, "handle": ""}, _HANDLE_RULE),
        ({**_BLOG, "handle": "   "}, _HANDLE_RULE),
        ({**_BLOG, "handle": "My Project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "my_project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "my--project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "-my-project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "a" * 65}, _HANDLE_RULE),
        ({key: value for key, value in _BLOG.items() if key != "name"}, "Field required"),
        ({**_BLOG, "name": ""}, "at least 1 character"),
        ({**_BLOG, "name": "   "}, "at least 1 character"),
        ({**_BLOG, "extra_tags": {"name": "Blog"}}, "extra tag keys ['name'] name core fields"),
        ({**_BLOG, "extra_tags": {"handle": "blog"}}, "extra tag keys ['handle'] name core fields"),
        ({**_BLOG, "extra_tags": {"spend_type": "customer"}}, "extra tag keys ['spend_type'] name core fields"),
        ({**_BLOG, "extra_tags": {"week": 4}}, "Input should be a valid string"),
    ],
    ids=[
        "subcategory-of-another-spend-type",
        "retired-development-subcategory",
        "development-miscellaneous",
        "customer-miscellaneous",
        "missing-subcategory",
        "null-subcategory",
        "customer-without-customer",
        "blank-customer",
        "unknown-spend-type",
        "missing-handle",
        "empty-handle",
        "blank-handle",
        "handle-with-space-and-capitals",
        "handle-with-underscore",
        "handle-with-double-hyphen",
        "handle-with-leading-hyphen",
        "handle-65-chars",
        "missing-name",
        "empty-name",
        "blank-name",
        "extra-tag-shadows-name",
        "extra-tag-shadows-handle",
        "extra-tag-shadows-spend-type",
        "extra-tag-value-not-a-string",
    ],
)
def test_create_rejects_invalid_projects_without_saving(
    database_session: Session, project: dict[str, Any], message: str
) -> None:
    response = client.post("/projects", json=project)

    assert response.status_code == 422
    assert message in response.text
    assert database_session.exec(select(Project)).all() == []


def test_create_refuses_a_handle_the_org_already_uses(database_session: Session, router_bench: Project) -> None:
    response = client.post("/projects", json={**_BLOG, "handle": "router-bench", "name": "Another name"})

    assert response.status_code == 409
    assert response.json()["detail"] == "Project 'router-bench' already exists"
    assert len(database_session.exec(select(Project)).all()) == 1


def test_create_accepts_a_name_the_org_already_uses_under_another_handle(
    database_session: Session, router_bench: Project
) -> None:
    response = client.post("/projects", json={**_BLOG, "handle": "router-bench-2", "name": "Router Bench"})

    assert response.status_code == 200, response.text
    assert sorted((row.handle, row.name) for row in database_session.exec(select(Project)).all()) == [
        ("router-bench", "Router Bench"),
        ("router-bench-2", "Router Bench"),
    ]


def test_list_shows_only_the_callers_org_projects_by_handle(
    database_session: Session, router_bench: Project, google_report: Project
) -> None:
    database_session.add(
        Project(
            org_id=router_bench.org_id,
            handle="a-first-by-handle",
            name="Zulu last by name",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.get("/projects")

    assert response.status_code == 200, response.text
    projects = response.json()["projects"]
    assert [(project["handle"], project["name"]) for project in projects] == [
        ("a-first-by-handle", "Zulu last by name"),
        ("google-report-xyz", "Google Report XYZ"),
        ("router-bench", "Router Bench"),
    ]
    assert {key: projects[1][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT


def test_update_changes_spend_fields_without_replacing_project_identity(
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
) -> None:
    stable = (router_bench.id, router_bench.org_id, router_bench.created_at, router_bench.created_by_email)
    benchmark = _saved_run(database_session, contract)
    database_session.add(RunProject(benchmark_id=benchmark.id, project_id=router_bench.id))
    database_session.commit()
    history_before = [(row.id, row.benchmark_id, row.project_id) for row in _link_rows(database_session, benchmark.id)]

    response = client.put(
        "/projects/router-bench",
        json={**_CUSTOMER_PROJECT, "handle": "router-bench"},
    )

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in _CUSTOMER_PROJECT} == {
        **_CUSTOMER_PROJECT,
        "handle": "router-bench",
    }
    database_session.expire_all()
    saved = database_session.get(Project, router_bench.id)
    assert saved is not None
    assert _project_fields(saved) == {**_CUSTOMER_PROJECT, "handle": "router-bench"}
    assert (saved.id, saved.org_id, saved.created_by_email) == (stable[0], stable[1], stable[3])
    assert saved.created_at == stable[2].replace(tzinfo=None)
    assert [
        (row.id, row.benchmark_id, row.project_id) for row in _link_rows(database_session, benchmark.id)
    ] == history_before
    metadata = client.get(f"/fetch-benchmark-metadata/{benchmark.id}").json()
    assert {key: metadata["project"][key] for key in _CUSTOMER_PROJECT} == {
        **_CUSTOMER_PROJECT,
        "handle": "router-bench",
    }


def test_update_rejects_a_body_handle_that_differs_from_the_path(
    database_session: Session, router_bench: Project
) -> None:
    project_id = router_bench.id
    before = _project_fields(router_bench)

    response = client.put("/projects/router-bench", json={**_BLOG, "handle": "blog"})

    assert response.status_code == 422
    assert response.json()["detail"] == "Project handle in the body must match the path"
    database_session.expire_all()
    saved = database_session.get(Project, project_id)
    assert saved is not None
    assert _project_fields(saved) == before


def test_update_reuses_project_spend_validation(database_session: Session, router_bench: Project) -> None:
    project_id = router_bench.id
    before = _project_fields(router_bench)

    response = client.put(
        "/projects/router-bench",
        json={**_BLOG, "handle": "router-bench", "spend_type": "customer", "customer": None},
    )

    assert response.status_code == 422
    assert "subcategory 'blog' is not allowed for spend type 'customer'" in response.text
    database_session.expire_all()
    saved = database_session.get(Project, project_id)
    assert saved is not None
    assert _project_fields(saved) == before


def test_update_hides_another_orgs_project(database_session: Session) -> None:
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.put("/projects/theirs", json={**_BLOG, "handle": "theirs"})

    assert response.status_code == 404


def test_delete_returns_and_removes_an_unused_project(database_session: Session, router_bench: Project) -> None:
    project_id = router_bench.id
    expected = _project_fields(router_bench)

    response = client.delete("/projects/router-bench")

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in expected} == expected
    database_session.expire_all()
    assert database_session.get(Project, project_id) is None


def test_delete_hides_unknown_and_other_org_projects(database_session: Session) -> None:
    other = Project(
        org_id=_other_org(database_session).id,
        handle="theirs",
        name="Theirs",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(other)
    database_session.commit()

    for handle in ("missing", "theirs"):
        response = client.delete(f"/projects/{handle}")
        assert response.status_code == 404
    assert database_session.get(Project, other.id) is not None


def test_delete_rejects_a_project_with_any_historical_run_link(
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
    google_report: Project,
) -> None:
    benchmark = _saved_run(database_session, contract)
    database_session.add_all(
        [
            RunProject(benchmark_id=benchmark.id, project_id=router_bench.id),
            RunProject(benchmark_id=benchmark.id, project_id=google_report.id),
        ]
    )
    database_session.commit()

    response = client.delete("/projects/router-bench")

    assert response.status_code == 409
    assert response.json()["detail"] == "Project 'router-bench' has run history and cannot be deleted"
    assert database_session.get(Project, router_bench.id) is not None


@pytest.mark.usefixtures("active_executor_release")
@pytest.mark.parametrize("final_status", [BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED])
async def test_start_links_the_project_and_the_link_is_unchanged_when_the_run_status_becomes_error_or_stopped(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    google_report: Project,
    final_status: BenchmarkStatus,
) -> None:
    starter = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: starter)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

    response = client.post("/start-benchmark", json=_start_request(contract, " google-report-xyz "))

    assert response.status_code == 200, response.text
    assert {key: response.json()["project"][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT
    run_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, run_id)
    assert benchmark is not None
    benchmark.status = final_status
    database_session.add(benchmark)
    database_session.commit()

    (row,) = _link_rows(database_session, run_id)
    assert row.project_id == google_report.id
    assert row.created_by_email == benchmark.started_by_email == "alice@vals.ai"
    metadata = client.get(f"/fetch-benchmark-metadata/{run_id}").json()
    assert {key: metadata["project"][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT


@pytest.mark.usefixtures("active_executor_release")
async def test_start_whose_project_link_cannot_be_inserted_saves_no_run_and_no_tasks(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    original_add = Session.add

    def refuse_run_project(self: Session, instance: object, *args: Any, **kwargs: Any) -> None:
        if isinstance(instance, RunProject):
            raise RuntimeError("project link cannot be inserted")
        original_add(self, instance, *args, **kwargs)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    monkeypatch.setattr(Session, "add", refuse_run_project)

    response = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert response.status_code == 500
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(Task)).all() == []
    assert database_session.exec(select(RunProject)).all() == []


def test_start_in_an_unknown_project_is_refused_with_the_handles_that_exist(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, google_report: Project
) -> None:
    response = client.post("/start-benchmark", json=_start_request(contract, "router-bnech"))

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Project 'router-bnech' does not exist. Existing projects: google-report-xyz, router-bench. "
        "Create it with: valkyrie project create router-bnech --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY"
    )
    assert database_session.exec(select(Benchmark)).all() == []


def test_start_cannot_use_another_orgs_project(database_session: Session, contract: AgentContractRequest) -> None:
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.post("/start-benchmark", json=_start_request(contract, "theirs"))

    assert response.status_code == 422
    assert "Existing projects: none." in response.text
    assert database_session.exec(select(Benchmark)).all() == []


@pytest.mark.usefixtures("active_executor_release")
async def test_start_without_a_project_is_accepted_and_saves_no_link(
    monkeypatch: MonkeyPatch, database_session: Session, contract: AgentContractRequest, mock_launcher: Any
) -> None:
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

    response = client.post("/start-benchmark", json=_start_request(contract, None))

    assert response.status_code == 200, response.text
    assert response.json()["project"] is None
    run_id = UUID(response.json()["benchmark_id"])
    assert _link_rows(database_session, run_id) == []
    assert client.get(f"/fetch-benchmark-metadata/{run_id}").json()["project"] is None


def test_retag_of_a_finished_run_adds_a_link_and_metadata_returns_the_newest(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
    google_report: Project,
) -> None:
    benchmark = _saved_run(database_session, contract)
    database_session.add(
        RunProject(
            benchmark_id=benchmark.id,
            project_id=router_bench.id,
            created_at=datetime.now(UTC) - timedelta(hours=1),
            created_by_email="alice@vals.ai",
        )
    )
    database_session.commit()
    retagger = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="bob@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: retagger)

    response = client.post(f"/benchmarks/{benchmark.id}/project", json={"project": "google-report-xyz"})

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT
    old, new = _link_rows(database_session, benchmark.id)
    assert (old.project_id, old.created_by_email) == (router_bench.id, "alice@vals.ai")
    assert (new.project_id, new.created_by_email) == (google_report.id, "bob@vals.ai")
    metadata = client.get(f"/fetch-benchmark-metadata/{benchmark.id}").json()
    assert (metadata["project"]["handle"], metadata["project"]["name"]) == ("google-report-xyz", "Google Report XYZ")


def test_the_last_link_appended_is_the_current_project_when_timestamps_tie(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, google_report: Project
) -> None:
    moved_at = datetime.now(UTC)
    runs = {
        "google-report-xyz": (_saved_run(database_session, contract), [router_bench, google_report]),
        "router-bench": (_saved_run(database_session, contract), [google_report, router_bench]),
    }
    for run, projects in runs.values():
        database_session.add_all(
            RunProject(benchmark_id=run.id, project_id=project.id, created_at=moved_at) for project in projects
        )
    database_session.commit()

    current = {handle: run.fetch_project(database_session) for handle, (run, _) in runs.items()}

    assert {handle: project.handle if project else None for handle, project in current.items()} == {
        handle: handle for handle in runs
    }


def test_retag_into_an_unknown_project_is_refused_without_a_link(
    database_session: Session, contract: AgentContractRequest, router_bench: Project
) -> None:
    benchmark = _saved_run(database_session, contract)

    response = client.post(f"/benchmarks/{benchmark.id}/project", json={"project": "router-bnech"})

    assert response.status_code == 422
    assert "Existing projects: router-bench." in response.text
    assert _link_rows(database_session, benchmark.id) == []


def test_retag_hides_missing_and_other_org_runs(
    database_session: Session, contract: AgentContractRequest, router_bench: Project
) -> None:
    other_run = _saved_run(database_session, contract, org_id=_other_org(database_session).id)

    for run_id in (uuid4(), other_run.id):
        response = client.post(f"/benchmarks/{run_id}/project", json={"project": "router-bench"})
        assert response.status_code == 404
    assert database_session.exec(select(RunProject)).all() == []
