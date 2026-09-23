"""Disposable executor API server used by process-restart integration tests."""

import json
import os
import sys
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from sqlmodel import Session, create_engine

from tracker.database.session import get_session
from tracker.executor_api.v1.router import router


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
    uvicorn.run(app, fd=int(sys.argv[1]), log_level="error", access_log=False)


if __name__ == "__main__":
    main()
