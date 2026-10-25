"""Create and list the projects an organization's runs are started in."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import Org, Project, RunProject
from tracker.database.session import get_session
from tracker.types import ProjectCreateRequest, ProjectResponse, ProjectsResponse

router = APIRouter(prefix="/projects")


def _org_projects(session: Session, org_id: UUID) -> list[Project]:
    return list(session.exec(select(Project).where(col(Project.org_id) == org_id).order_by(col(Project.handle))).all())


def find_project(session: Session, org_id: UUID, handle: str) -> Project:
    """Return the organization's project with exactly this handle, or refuse with the handles that exist."""
    project = session.exec(select(Project).where(col(Project.org_id) == org_id, col(Project.handle) == handle)).first()
    if project is not None:
        return project
    handles = ", ".join(row.handle for row in _org_projects(session, org_id)) or "none"
    raise HTTPException(
        status_code=422,
        detail=f"Project '{handle}' does not exist. Existing projects: {handles}. "
        f"Create it with: valkyrie project create {handle} --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY",
    )


def _org_project_or_404(session: Session, org_id: UUID, handle: str) -> Project:
    project = session.exec(select(Project).where(col(Project.org_id) == org_id, col(Project.handle) == handle)).first()
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{handle}' not found")
    return project


@router.post("", response_model=ProjectResponse, responses={409: {"description": "The handle is already taken"}})
def create_project(
    request: ProjectCreateRequest,
    session: Session = Depends(get_session),
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    """Create a project that runs can be started in; the handle is unique within the organization."""
    project = Project(org_id=identity.org.id, created_by_email=identity.email, **request.model_dump())
    session.add(project)
    try:
        session.commit()
    except IntegrityError as error:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Project '{request.handle}' already exists") from error
    return ProjectResponse.from_row(project)


@router.put("/{handle}", response_model=ProjectResponse, responses={404: {"description": "The project was not found"}})
def update_project(
    handle: str,
    request: ProjectCreateRequest,
    session: Session = Depends(get_session),
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    """Change a project's shared metadata; linked spend is reclassified under the current values."""
    if request.handle != handle:
        raise HTTPException(status_code=422, detail="Project handle in the body must match the path")
    project = _org_project_or_404(session, identity.org.id, handle)
    project.name = request.name
    project.spend_type = request.spend_type
    project.subcategory = request.subcategory
    project.customer = request.customer
    project.extra_tags = request.extra_tags
    session.add(project)
    session.commit()
    return ProjectResponse.from_row(project)


@router.delete(
    "/{handle}",
    response_model=ProjectResponse,
    responses={404: {"description": "The project was not found"}, 409: {"description": "The project has run history"}},
)
def delete_project(
    handle: str,
    session: Session = Depends(get_session),
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    """Delete an unused project; any historical run link keeps it for spend attribution."""
    project = _org_project_or_404(session, identity.org.id, handle)
    linked = session.exec(select(RunProject).where(col(RunProject.project_id) == project.id).limit(1)).first()
    if linked is not None:
        raise HTTPException(status_code=409, detail=f"Project '{handle}' has run history and cannot be deleted")
    response = ProjectResponse.from_row(project)
    session.delete(project)
    try:
        session.commit()
    except IntegrityError as error:
        session.rollback()
        raise HTTPException(
            status_code=409, detail=f"Project '{handle}' has run history and cannot be deleted"
        ) from error
    return response


@router.get("", response_model=ProjectsResponse)
def list_projects(session: Session = Depends(get_session), org: Org = Depends(get_current_org)) -> ProjectsResponse:
    """List the organization's projects by handle."""
    return ProjectsResponse(projects=[ProjectResponse.from_row(row) for row in _org_projects(session, org.id)])
