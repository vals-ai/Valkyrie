"""Check local listeners and the running ACL helpers without an upstream dependency."""

import socket
import sys
from pathlib import Path


def check() -> None:
    for port in (3128, 3129):
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            pass

    helpers = 0
    for process in Path("/proc").glob("[0-9]*"):
        try:
            arguments = (process / "cmdline").read_bytes().split(b"\0")
            if b"/opt/proxy/sni_acl.py" not in arguments:
                continue

            state = (process / "stat").read_text().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            # Squid can replace a helper while its process is being inspected.
            continue

        if state not in {"R", "S"}:
            raise RuntimeError(f"ACL helper {process.name} is not runnable: {state}")

        helpers += 1

    if not helpers:
        raise RuntimeError("No running ACL helpers")


if __name__ == "__main__":
    try:
        check()
    except (OSError, RuntimeError, ValueError, IndexError) as error:
        print(f"Proxy health check failed: {error}", file=sys.stderr)
        sys.exit(1)
