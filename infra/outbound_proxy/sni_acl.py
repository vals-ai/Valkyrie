"""Fail-closed Squid external ACL for the original CONNECT target and actual SNI."""

import re
import sys
from urllib.parse import unquote

DNS_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\.[a-z]{2,63}")


def authorize(line: str) -> bool:
    fields = line.split()
    if len(fields) != 3 or fields[2] != "-":
        return False

    authority, server_name = (unquote(field).lower() for field in fields[:2])
    host, separator, port = authority.rpartition(":")
    if not separator or port != "443" or len(host) > 253:
        return False

    return DNS_NAME.fullmatch(host) is not None and host == server_name


def main() -> None:
    for line in sys.stdin:
        print("OK" if authorize(line) else "ERR", flush=True)


if __name__ == "__main__":
    main()
