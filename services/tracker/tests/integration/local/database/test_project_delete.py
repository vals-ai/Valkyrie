"""PostgreSQL protection for project deletion races."""

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from main import app
from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import DEFAULT_ORG_NAME, Org, Project, RunProject, SpendType
from tracker.database.session import get_session


def test_project_create_edit_delete_http_lifecycle_uses_postgres(
    monkeypatch: pytest.MonkeyPatch,
    postgres_session: Session,
) -> None:
    org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    postgres_session.add(org)
    postgres_session.commit()

    def request_session() -> Generator[Session, None, None]:
        yield postgres_session

    identity = RequestIdentity(org=org, email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_session, request_session)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)

    with TestClient(app) as client:
        created = client.post(
            "/projects",
            json={
                "handle": "router-bench",
                "name": "Router Bench",
                "spend_type": "development",
                "subcategory": "new_benchmark_generation",
            },
        )
        edited = client.put(
            "/projects/router-bench",
            json={
                "handle": "router-bench",
                "name": "Customer Router",
                "spend_type": "customer",
                "subcategory": "report",
                "customer": "acme",
                "extra_tags": {"engagement": "q4"},
            },
        )
        deleted = client.delete("/projects/router-bench")
        listed = client.get("/projects")

    assert created.status_code == 200, created.text
    assert edited.status_code == 200, edited.text
    assert edited.json()["name"] == "Customer Router"
    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == edited.json()
    assert listed.json() == {"projects": []}


def test_concurrent_link_wins_and_project_delete_returns_conflict(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
) -> None:
    org, benchmark_id, project_id = _seed_linkable_project(postgres_engine)
    start_link = Event()
    link_finished = Event()

    def link_project() -> None:
        assert start_link.wait(5)
        try:
            with Session(postgres_engine) as session:
                session.add(RunProject(benchmark_id=benchmark_id, project_id=project_id))
                session.commit()
        finally:
            link_finished.set()

    with Session(postgres_engine, expire_on_commit=False) as delete_session:

        def request_session() -> Generator[Session, None, None]:
            yield delete_session

        def link_before_delete_flush(_session: Session, _flush_context: object, _instances: object) -> None:
            start_link.set()
            assert link_finished.wait(5)

        identity = RequestIdentity(org=org, email="alice@vals.ai")
        monkeypatch.setitem(app.dependency_overrides, get_session, request_session)
        monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
        event.listen(delete_session, "before_flush", link_before_delete_flush)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                linked = pool.submit(link_project)
                with TestClient(app) as client:
                    response = client.delete("/projects/router-bench")
                linked.result(timeout=5)
        finally:
            event.remove(delete_session, "before_flush", link_before_delete_flush)

    assert response.status_code == 409
    assert response.json()["detail"] == "Project 'router-bench' has run history and cannot be deleted"
    with Session(postgres_engine) as session:
        assert session.get(Project, project_id) is not None
        assert len(session.exec(select(RunProject)).all()) == 1


def test_project_delete_wins_and_concurrent_link_cannot_commit(postgres_engine: Engine) -> None:
    _, benchmark_id, project_id = _seed_linkable_project(postgres_engine)
    start = Event()
    project_deleted = Event()

    def delete_project() -> None:
        assert start.wait(5)
        try:
            with Session(postgres_engine) as session:
                project = session.get(Project, project_id)
                assert project is not None
                session.delete(project)
                session.commit()
        finally:
            project_deleted.set()

    def link_project() -> None:
        assert start.wait(5)
        assert project_deleted.wait(5)
        with Session(postgres_engine) as session:
            session.add(RunProject(benchmark_id=benchmark_id, project_id=project_id))
            with pytest.raises(IntegrityError):
                session.commit()
            session.rollback()

    with ThreadPoolExecutor(max_workers=2) as pool:
        deleted = pool.submit(delete_project)
        linked = pool.submit(link_project)
        start.set()
        deleted.result(timeout=5)
        linked.result(timeout=5)

    with Session(postgres_engine) as session:
        assert session.get(Project, project_id) is None
        assert session.exec(select(RunProject)).all() == []


def _seed_linkable_project(engine: Engine) -> tuple[Org, UUID, UUID]:
    with Session(engine, expire_on_commit=False) as session:
        org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
        session.add(org)
        session.commit()
        benchmark = make_benchmark(session=session)
        project = Project(
            org_id=TEST_ORG_ID,
            handle="router-bench",
            name="Router Bench",
            spend_type=SpendType.DEVELOPMENT,
            subcategory="new_benchmark_generation",
        )
        session.add(project)
        session.commit()
        return org, benchmark.id, project.id
