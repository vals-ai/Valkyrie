"""Run with `uv run pytest tests/integration/local/api/test_costs.py`.

Exercise per-email and per-project run cost totals through the real app and local database.
"""

from datetime import datetime, timedelta
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import BenchmarkStatus, Org, Project, RunCost, RunProject, SpendType


def _run_with_cost(
    session: Session,
    *,
    email: str | None,
    started_at: datetime,
    amounts: list[float],
    org_id: UUID = TEST_ORG_ID,
    provider: str = "daytona",
) -> None:
    benchmark = make_benchmark(
        status=BenchmarkStatus.FINISHED,
        started_at=started_at,
        started_by_email=email,
        org_id=org_id,
        session=session,
    )
    for amount in amounts:
        session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type="sandbox",
                source_id=str(uuid4()),
                provider=provider,
                amount_usd=amount,
            )
        )
    session.commit()


class TestCostsByEmail:
    """Per-email cost totals for runs started in a window."""

    def test_sums_each_starter_for_runs_started_in_window(self, client: TestClient, database_session: Session) -> None:
        """Costs must be summed per starter email over runs started inside [start, end).

        Test cases:
        - Two runs by the same person, with differently cased emails, sum into one lowercased row.
        - A run started exactly at `start` is counted; one started exactly at `end` is not.
        - A run with no starter email is left out.
        - Another organization's runs are left out.
        - The same person's costs from another provider come back as their own row.
        """
        utc = ZoneInfo("UTC")
        _run_with_cost(
            database_session, email="Alice@Vals.ai", started_at=datetime(2026, 10, 1, tzinfo=utc), amounts=[1.0, 0.5]
        )
        _run_with_cost(
            database_session, email="alice@vals.ai ", started_at=datetime(2026, 10, 31, 23, tzinfo=utc), amounts=[2.0]
        )
        _run_with_cost(
            database_session, email="bob@vals.ai", started_at=datetime(2026, 10, 15, tzinfo=utc), amounts=[0.25]
        )
        _run_with_cost(
            database_session, email="bob@vals.ai", started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[9.0]
        )
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 10, tzinfo=utc),
            amounts=[0.75],
            provider="modal",
        )
        _run_with_cost(database_session, email=None, started_at=datetime(2026, 10, 15, tzinfo=utc), amounts=[9.0])
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 15, tzinfo=utc),
            amounts=[9.0],
            org_id=other_org.id,
        )

        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-01T00:00:00Z"
        assert body["end"] == "2026-11-01T00:00:00Z"
        assert body["costs"] == [
            {
                "email": "alice@vals.ai",
                "cost_type": "sandbox",
                "provider": "daytona",
                "amount_usd": 3.5,
                "run_count": 2,
            },
            {"email": "alice@vals.ai", "cost_type": "sandbox", "provider": "modal", "amount_usd": 0.75, "run_count": 1},
            {"email": "bob@vals.ai", "cost_type": "sandbox", "provider": "daytona", "amount_usd": 0.25, "run_count": 1},
        ]

    def test_end_must_follow_start(self, client: TestClient) -> None:
        """A window whose end is not after its start must be rejected.

        Test cases:
        - Equal start and end receives 422.
        """
        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-01", "end": "2026-10-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 422

    def test_costs_unauth_401(self, client: TestClient) -> None:
        """Cost totals must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        response = client.get("/costs/by-email", params={"start": "2026-10-01", "end": "2026-11-01"})

        assert response.status_code == 401


def _project(
    session: Session, handle: str, spend_type: SpendType, subcategory: str, *, org_id: UUID = TEST_ORG_ID
) -> Project:
    project = Project(
        org_id=org_id,
        handle=handle,
        name=handle.replace("-", " ").title(),
        spend_type=spend_type,
        subcategory=subcategory,
        customer="google" if spend_type is SpendType.CUSTOMER else None,
    )
    session.add(project)
    session.commit()
    return project


def _link(session: Session, benchmark_id: UUID, project: Project, *, created_at: datetime) -> None:
    session.add(RunProject(benchmark_id=benchmark_id, project_id=project.id, created_at=created_at))
    session.flush()


def _tagged_run(
    session: Session,
    *,
    started_at: datetime,
    amounts: list[float],
    provider: str = "daytona",
    org_id: UUID = TEST_ORG_ID,
) -> UUID:
    benchmark = make_benchmark(
        status=BenchmarkStatus.FINISHED,
        started_at=started_at,
        started_by_email="alice@vals.ai",
        org_id=org_id,
        session=session,
    )
    for amount in amounts:
        session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type="sandbox",
                source_id=str(uuid4()),
                provider=provider,
                amount_usd=amount,
            )
        )
    session.commit()
    return benchmark.id


class TestCostsByProject:
    """Costs per month, vendor and project for runs started in a window."""

    def test_groups_costs_under_each_runs_newest_project(self, client: TestClient, database_session: Session) -> None:
        """Costs must be grouped under the project of each run's newest link.

        Test cases:
        - A run moved from one project to another counts once, under the newest project.
        - Two links written with the same created_at resolve to the one appended last.
        - A run with no project comes back with null project fields, and is never dropped.
        - The vendor name is the provider title-cased, and the month is the UTC month the run started.
        - A run started exactly at `end` and another organization's run are left out.
        - Every run in the window is listed with its project handle.
        """
        utc = ZoneInfo("UTC")
        stamp = datetime(2026, 10, 20, tzinfo=utc)
        router = _project(database_session, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        blog = _project(database_session, "blog-xyz", SpendType.INTERNAL, "blog")
        google = _project(database_session, "google-report-xyz", SpendType.CUSTOMER, "report")

        moved = _tagged_run(database_session, started_at=datetime(2026, 10, 5, tzinfo=utc), amounts=[1.0, 0.5])
        _link(database_session, moved, router, created_at=stamp)
        _link(database_session, moved, blog, created_at=stamp + timedelta(minutes=5))
        tied = _tagged_run(database_session, started_at=datetime(2026, 10, 6, tzinfo=utc), amounts=[2.0])
        _link(database_session, tied, google, created_at=stamp)
        _link(database_session, tied, router, created_at=stamp)
        untagged = _tagged_run(
            database_session, started_at=datetime(2026, 10, 7, tzinfo=utc), amounts=[0.25], provider="modal"
        )
        november = _tagged_run(database_session, started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[9.0])
        _link(database_session, november, router, created_at=stamp)
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        foreign = _tagged_run(
            database_session, started_at=datetime(2026, 10, 8, tzinfo=utc), amounts=[9.0], org_id=other_org.id
        )
        foreign_project = _project(
            database_session, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation", org_id=other_org.id
        )
        _link(database_session, foreign, foreign_project, created_at=stamp)
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-01T00:00:00Z"
        assert body["end"] == "2026-11-01T00:00:00Z"
        assert body["rows"] == [
            {
                "month": "2026-10",
                "vendor": "Daytona",
                "spend_type": "internal",
                "subcategory": "blog",
                "project_handle": "blog-xyz",
                "project_name": "Blog Xyz",
                "customer": None,
                "amount_usd": 1.5,
                "run_count": 1,
            },
            {
                "month": "2026-10",
                "vendor": "Daytona",
                "spend_type": "development",
                "subcategory": "new_benchmark_generation",
                "project_handle": "router-bench",
                "project_name": "Router Bench",
                "customer": None,
                "amount_usd": 2.0,
                "run_count": 1,
            },
            {
                "month": "2026-10",
                "vendor": "Modal",
                "spend_type": None,
                "subcategory": None,
                "project_handle": None,
                "project_name": None,
                "customer": None,
                "amount_usd": 0.25,
                "run_count": 1,
            },
        ]
        assert {(run["run_id"], run["project_handle"]) for run in body["runs"]} == {
            (str(moved), "blog-xyz"),
            (str(tied), "router-bench"),
            (str(untagged), None),
        }

    def test_month_follows_the_utc_start_and_window_edges_are_half_open(
        self, client: TestClient, database_session: Session
    ) -> None:
        """Runs must be grouped by the UTC month of their start, with `start` included and `end` excluded.

        Test cases:
        - A run started at 23:30 UTC on the last day of October counts in 2026-10, whatever the host timezone.
        - A run started exactly at `start` is counted and one started exactly at `end` is not.
        - Every returned start time carries an explicit UTC offset.
        """
        utc = ZoneInfo("UTC")
        late_october = _tagged_run(
            database_session, started_at=datetime(2026, 10, 31, 23, 30, tzinfo=utc), amounts=[1.0]
        )
        first_instant = _tagged_run(database_session, started_at=datetime(2026, 10, 1, tzinfo=utc), amounts=[2.0])
        _tagged_run(database_session, started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[4.0])

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert [(row["month"], row["amount_usd"]) for row in body["rows"]] == [("2026-10", 3.0)]
        assert {run["run_id"] for run in body["runs"]} == {str(late_october), str(first_instant)}
        assert all(run["started_at"].endswith("Z") for run in body["runs"])

    def test_a_link_to_another_orgs_project_is_not_attributed(
        self, client: TestClient, database_session: Session
    ) -> None:
        """A run must never show the details of a project that belongs to another organization.

        Test cases:
        - A run linked to another org's project comes back untagged, with null project fields.
        """
        utc = ZoneInfo("UTC")
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        foreign_project = _project(
            database_session, "secret-project", SpendType.CUSTOMER, "report", org_id=other_org.id
        )
        run = _tagged_run(database_session, started_at=datetime(2026, 10, 5, tzinfo=utc), amounts=[1.0])
        _link(database_session, run, foreign_project, created_at=datetime(2026, 10, 6, tzinfo=utc))
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert [row["project_handle"] for row in body["rows"]] == [None]
        assert [run["project_handle"] for run in body["runs"]] == [None]

    @pytest.mark.parametrize("end", ["2026-10-01", "2026-09-30"])
    def test_end_must_follow_start(self, client: TestClient, end: str) -> None:
        """A window whose end is not after its start must be rejected.

        Test cases:
        - Equal start and end receives 422.
        - An end before the start receives 422.
        """
        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": end},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 422

    def test_costs_by_project_unauth_401(self, client: TestClient) -> None:
        """Project cost totals must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        response = client.get("/costs/by-project", params={"start": "2026-10-01", "end": "2026-11-01"})

        assert response.status_code == 401
