"""Dispatch credentials shared by version-one executor routers."""

from collections.abc import Generator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import Session

from tracker.database.session import get_session
from tracker.executor.dispatch_api import DispatchAccessDenied, DispatchConflict, authenticate_dispatch

_bearer = HTTPBearer(auto_error=False, scheme_name="ExecutorDispatchAuth")


def _dispatch_session(
    dispatch_id: UUID,
    session: Annotated[Session, Depends(get_session)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> Generator[Session, None, None]:
    try:
        if credentials is None:
            raise DispatchAccessDenied("Invalid executor credential")
        authenticate_dispatch(session, dispatch_id, credentials.credentials)
        yield session
    except DispatchAccessDenied as error:
        session.rollback()
        raise HTTPException(
            401, detail="Invalid executor credential", headers={"WWW-Authenticate": "Bearer"}
        ) from error
    except DispatchConflict as error:
        session.rollback()
        raise HTTPException(409, detail=str(error)) from error


DispatchSession = Annotated[Session, Depends(_dispatch_session)]
