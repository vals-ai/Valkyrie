"""Operator commands for registering and promoting executor releases."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from sqlmodel import Session

from tracker.database.models import ExecutorRelease
from tracker.database.session import engine
from tracker.release_control import (
    ReleaseControlError,
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
    register_parser.add_argument("--protocol-version", default="1")

    for command in ("verify", "promote", "retire"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("release_id")

    args = parser.parse_args(argv)
    try:
        with Session(engine) as session:
            if args.command == "register":
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
            else:
                retired = retire_if_empty(session, args.release_id)
                session.commit()
                result = {"id": args.release_id, "retired": retired}
    except ReleaseControlError as exc:
        parser.error(str(exc))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
