"""GET /users — list users in the current org."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Org, User
from tracker.database.session import get_session

router = APIRouter()


class UserSummary(BaseModel):
    id: str
    descope_user_id: str
    email: str


@router.get("/users", response_model=list[UserSummary])
def list_users(
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> list[UserSummary]:
    _, org = user_and_org
    users = session.exec(select(User).where(User.org_id == org.id).order_by(User.email)).all()
    return [
        UserSummary(id=str(u.id), descope_user_id=u.descope_user_id, email=u.email)
        for u in users
    ]
