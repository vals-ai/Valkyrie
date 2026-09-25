"""Disposable executor API server used by process-restart integration tests."""

import json
import os
import sys
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from sqlmodel import Session, create_engine

from tracker.database.session import get_session
from tracker.executor_api.v1.router import router
from tracker.executor_api.v1.task_schemas import SaveCheckpoint, TaskWriteRequest


def main() -> None:
    engine = create_engine(os.environ["TEST_EXECUTOR_DATABASE_URL"])

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        print(json.dumps({"event": "ready", "pid": os.getpid()}), flush=True)
        try:
            yield
        finally:
            engine.dispose()

    def session_dependency() -> Generator[Session, None, None]:
        with Session(engine, expire_on_commit=False) as session:
            yield session

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    app.dependency_overrides[get_session] = session_dependency
    failures_remaining = int(os.environ.get("TEST_EXECUTOR_CHECKPOINT_FAILURES", "0"))

    async def checkpoint_outage(request: Request, call_next: RequestResponseEndpoint) -> Response:
        nonlocal failures_remaining
        if failures_remaining and request.url.path.endswith("/write"):
            command = TaskWriteRequest.model_validate(await request.json())
            if isinstance(command.mutation, SaveCheckpoint):
                failures_remaining -= 1
                return JSONResponse({"detail": "Checkpoint service restarting"}, status_code=503)

        return await call_next(request)

    app.middleware("http")(checkpoint_outage)
    uvicorn.run(app, fd=int(sys.argv[1]), log_level="error", access_log=False)


if __name__ == "__main__":
    main()
