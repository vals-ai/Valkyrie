"""Projects that runs are started in, and the spend fields every run in a project shares."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from valkyrie.sdk.models._base import ResponseModel, serialize_utc


class SpendType(str, Enum):
    """Why a run spends money."""

    DEVELOPMENT = "development"
    INTERNAL = "internal"
    CUSTOMER = "customer"


SPEND_SUBCATEGORIES: dict[SpendType, tuple[str, ...]] = {
    SpendType.DEVELOPMENT: (
        "new_benchmark_generation",
        "new_benchmark_launch_runs",
        "post_launch_benchmark_update",
    ),
    SpendType.CUSTOMER: ("report", "paid_run", "eval_server", "platform"),
    SpendType.INTERNAL: (
        "unpaid_leaderboard_run",
        "other_unpaid_run",
        "fellows_interns",
        "blog",
        "miscellaneous",
    ),
}


class ProjectCreateRequest(BaseModel):
    """A new project: its immutable handle, name, spend type and subcategory, and optional customer and extra tags.

    ``handle`` is the string runs are started with (lowercase letters, digits and hyphens, e.g. ``my-project``) and
    ``name`` is the label people read (``My Project``). The tracker accepts only the subcategories in
    ``SPEND_SUBCATEGORIES`` for the spend type and requires ``customer`` when the spend type is ``customer``.
    """

    model_config = ConfigDict(extra="forbid")

    handle: str
    name: str = Field(min_length=1)
    spend_type: SpendType
    subcategory: str
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)


class ProjectResponse(ResponseModel):
    """One saved project."""

    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)
    created_at: datetime
    created_by_email: str | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        """Serialize the creation time with an explicit offset."""
        serialized = serialize_utc(value)
        assert serialized is not None
        return serialized


class ProjectsResponse(ResponseModel):
    """The organization's projects, by handle."""

    projects: list[ProjectResponse]
