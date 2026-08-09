"""Organization persistence operations."""

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session, select

from tracker.database.models import DEFAULT_ORG_NAME, Org


class OrgRepository:
    """Load organizations without exposing query construction to callers."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def session(self) -> Session:
        """Return the caller-owned session used by this repository."""
        return self._session

    def get_by_id(self, org_id: UUID) -> Org | None:
        """Return an organization by primary key, if it exists."""
        return self._session.get(Org, org_id)

    def get_default(self) -> Org | None:
        """Return the configured default organization, if it exists."""
        return self._session.exec(select(Org).where(Org.name == DEFAULT_ORG_NAME)).first()

    def find_by_name(self, name: str) -> Org | None:
        """Return the organization with the given tenant name, if it exists."""
        return self._session.exec(select(Org).where(Org.name == name)).first()

    def ensure_by_name(self, name: str) -> tuple[Org | None, bool]:
        """Ensure an organization exists and return it with whether this call created it.

        The insert participates in the caller's transaction; this method never commits.
        """
        statement = pg_insert(Org).values(name=name).on_conflict_do_nothing(index_elements=["name"])
        result = self._session.exec(statement)
        created = result.rowcount > 0
        return self.find_by_name(name), created
