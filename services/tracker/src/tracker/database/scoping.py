"""Org-scoped query helpers for multi-tenant data isolation."""

from typing import TypeVar
from uuid import UUID

from fastapi import HTTPException
from sqlmodel import SQLModel, Session, select

from tracker.database.models import Org

T = TypeVar("T", bound=SQLModel)


def scoped_select(model: type[T], org: Org):  # type: ignore[type-var]
    """Return a SELECT filtered to the given org. Replaces bare select(Model) for org-scoped tables."""
    return select(model).where(model.org_id == org.id)  # type: ignore[attr-defined]


def assert_org(row: T | None, org: Org) -> T:
    """Validate a row belongs to the given org. Returns 404 for both missing and wrong-org (no existence leak)."""
    if row is None or row.org_id != org.id:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Not found")
    return row


def get_scoped(model: type[T], row_id: UUID, session: Session, org: Org) -> T:
    """Fetch a row by PK and validate it belongs to the given org. Returns 404 if missing or wrong org."""
    return assert_org(session.get(model, row_id), org)
