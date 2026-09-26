"""Programmatic uvicorn entrypoint.

Starts uvicorn with log_config=None so our configure_logging() dictConfig
is not overwritten by uvicorn's default logging setup.
"""

import argparse
from pathlib import Path

import uvicorn

from tracker.local import config


def _register_source_release() -> None:
    from sqlmodel import Session

    from tracker.database.session import engine
    from tracker.local.releases import register_source_release

    source_root = Path(__file__).resolve().parents[1]
    with Session(engine) as session:
        release = register_source_release(session, source_root)
        session.commit()
        print(f"Executor source release ready: {release.id} ({source_root})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, help="Local execution resource configuration")
    args = parser.parse_args()
    if args.config is not None:
        config.configure(args.config)
        _register_source_release()
    is_local = config.resources is not None
    uvicorn.run(
        "main:app",
        host="127.0.0.1" if is_local else "0.0.0.0",
        port=8000,
        workers=1 if is_local else 2,
        log_config=None,
    )


if __name__ == "__main__":
    main()
