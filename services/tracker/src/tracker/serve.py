"""Programmatic uvicorn entrypoint.

Starts uvicorn with log_config=None so our configure_logging() dictConfig
is not overwritten by uvicorn's default logging setup.
"""

import argparse
import base64
import os
from pathlib import Path

import uvicorn
from sqlalchemy.engine import make_url

from tracker import config as tracker_config
from tracker.local import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, help="Local execution resource configuration")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    args = parser.parse_args()
    if args.config is not None:
        config.configure(args.config)
        os.environ["EXECUTOR_LAUNCHER"] = "local"
        os.environ["EXECUTOR_SOURCE_ROOT"] = str(Path(__file__).resolve().parents[1])
        cache_dir = config.resources.data_root / "executor-cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ["EXECUTOR_CACHE_DIR"] = str(cache_dir)
        database_url = make_url(tracker_config.DATABASE_URL)
        os.environ.update(
            DB_HOST=str(database_url.host),
            DB_PORT=str(database_url.port),
            DB_NAME=str(database_url.database),
            DB_USERNAME=str(database_url.username),
            DB_PASSWORD=str(database_url.password),
        )
        os.environ.pop("EXECUTOR_PAYLOAD_KMS_KEY_ID", None)
        if "EXECUTOR_PAYLOAD_LOCAL_KEY" not in os.environ:
            os.environ["EXECUTOR_PAYLOAD_LOCAL_KEY"] = base64.b64encode(os.urandom(32)).decode()
        from tracker.local.releases import register_checkout_release

        register_checkout_release()
    is_local = config.resources is not None
    uvicorn.run(
        "main:app",
        host="127.0.0.1" if is_local else "0.0.0.0",
        port=args.port,
        workers=1 if is_local else 2,
        log_config=None,
    )


if __name__ == "__main__":
    main()
