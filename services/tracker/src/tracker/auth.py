"""Authentication and org resolution for the tracker service."""

from fastapi import Depends
from sqlmodel import Session, select

from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session

_cached_default_org: Org | None = None


def get_default_org(session: Session) -> Org:
    """Fetch the default org, cached after first load. Used in self-hosted mode."""
    global _cached_default_org
    if _cached_default_org is not None:
        return _cached_default_org
    org = session.exec(select(Org).where(Org.name == DEFAULT_ORG_NAME)).first()
    if not org:
        raise RuntimeError("Default org not found — run the migration")
    _cached_default_org = org
    return org


def get_current_org(session: Session = Depends(get_session)) -> Org:
    """FastAPI dependency that resolves the current org.

    Phase 1: always returns the default org (self-hosted mode).
    Phase 2: will add Descope auth dispatch based on AUTH_REQUIRED.
    """
    return get_default_org(session)
