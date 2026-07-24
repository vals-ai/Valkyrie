"""Operator commands for managing executor releases."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from sqlmodel import Session

from tracker.database.models import ExecutorRelease
from tracker.database.session import engine
from tracker.release_control import (
    ReleaseControlError,
    activate_release,
    executor_releases_status,
    promote_release,
    register_release,
    retire_if_empty,
    verify_release_artifact,
)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    register_parser = subparsers.add_parser("register", help="Register an immutable candidate release")
    register_parser.add_argument("release_id")
    register_parser.add_argument("artifact_uri")
    register_parser.add_argument("artifact_digest")
    register_parser.add_argument("--protocol-version", default=SUPPORTED_PROTOCOL_VERSION)

    activate_parser = subparsers.add_parser("activate", help="Verify and activate one immutable release")
    activate_parser.add_argument("release_id")
    activate_parser.add_argument("artifact_uri")
    activate_parser.add_argument("artifact_digest")
    activate_parser.add_argument("--protocol-version", default=SUPPORTED_PROTOCOL_VERSION)

    for command in ("verify", "promote", "retire"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("release_id")
    subparsers.add_parser("status", help="Report global release health and retirement blockers")

    args = parser.parse_args(argv)
    try:
        with Session(engine) as session:
            if args.command == "activate":
                release = activate_release(
                    session,
                    ExecutorRelease(
                        id=args.release_id,
                        artifact_uri=args.artifact_uri,
                        artifact_digest=args.artifact_digest,
                        protocol_version=args.protocol_version,
                    ),
                    expected_bucket=_required_environment("EXECUTOR_RELEASE_BUCKET"),
                    expected_prefix=_required_environment("EXECUTOR_RELEASE_PREFIX"),
                )
                session.commit()
                result = {
                    "id": release.id,
                    "status": release.status.value,
                    "readiness_verified": release.readiness_verified,
                }
            elif args.command == "register":
                release = register_release(
                    session,
                    ExecutorRelease(
                        id=args.release_id,
                        artifact_uri=args.artifact_uri,
                        artifact_digest=args.artifact_digest,
                        protocol_version=args.protocol_version,
                    ),
                )
                session.commit()
                result = {"id": release.id, "status": release.status.value}
            elif args.command == "verify":
                release = verify_release_artifact(session, args.release_id)
                session.commit()
                result = {"id": release.id, "status": release.status.value, "readiness_verified": True}
            elif args.command == "promote":
                release = promote_release(session, args.release_id)
                session.commit()
                result = {"id": release.id, "status": release.status.value}
            elif args.command == "retire":
                retired = retire_if_empty(session, args.release_id)
                session.commit()
                result = {"id": args.release_id, "retired": retired}
            else:
                result = executor_releases_status(session).model_dump(mode="json")
    except ReleaseControlError as exc:
        parser.error(str(exc))
    print(json.dumps(result))


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ReleaseControlError(f"{name} is required")
    return value


if __name__ == "__main__":
    main()
