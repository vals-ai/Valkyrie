"""Programmatic uvicorn entrypoint.

Starts uvicorn with log_config=None so our configure_logging() dictConfig
is not overwritten by uvicorn's default logging setup.
"""

import uvicorn


def main() -> None:
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_config=None,
    )


if __name__ == "__main__":
    main()
