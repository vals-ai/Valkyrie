"""Programmatic uvicorn entrypoint.

Starts uvicorn with log_config=None so our configure_logging() dictConfig
is not overwritten by uvicorn's default logging setup.
"""

import argparse
from pathlib import Path

import uvicorn

from tracker.local import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    if args.config is not None:
        config.configure(args.config)
    uvicorn.run(
        "main:app",
        host="127.0.0.1" if config.resources is not None else "0.0.0.0",
        port=8000,
        workers=1 if config.resources is not None else 2,
        log_config=None,
    )


if __name__ == "__main__":
    main()
