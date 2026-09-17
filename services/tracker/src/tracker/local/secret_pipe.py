"""Bounded credential transfer through an inherited socket, without files or environment values."""

import json
import os
import socket
import secrets
import tempfile
from pathlib import Path
from collections.abc import Mapping
from typing import cast

SECRET_SOCKET_ENV = "VALKYRIE_EXECUTION_SECRET_FD"
MAX_SECRET_PAYLOAD_BYTES = 8 * 1024 * 1024


class LocalSecretsError(RuntimeError):
    """A local executor could not receive its transient credentials."""


def validate_values(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        raise LocalSecretsError("Invalid local execution secret mapping")
    values = cast(dict[object, object], value)
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in values.items()):
        raise LocalSecretsError("Invalid local execution secret mapping")
    return cast(dict[str, str], values)


def send_execution_secrets(connection: socket.socket, values: Mapping[str, str]) -> None:
    encoded = json.dumps(dict(values)).encode()
    if len(encoded) > MAX_SECRET_PAYLOAD_BYTES:
        raise LocalSecretsError("Local execution secret payload exceeds the transfer limit")
    connection.settimeout(30)
    connection.sendall(encoded)
    connection.shutdown(socket.SHUT_WR)
    if connection.recv(1) != b"A":
        raise LocalSecretsError("Local executor did not acknowledge credential receipt")


def receive_execution_secrets() -> dict[str, str] | None:
    descriptor = os.environ.pop(SECRET_SOCKET_ENV, None)
    if descriptor is None:
        return None
    with socket.socket(fileno=int(descriptor)) as connection:
        connection.settimeout(30)
        with connection.makefile("rb") as stream:
            encoded = stream.read(MAX_SECRET_PAYLOAD_BYTES + 1)
        if len(encoded) > MAX_SECRET_PAYLOAD_BYTES:
            raise LocalSecretsError("Local execution secret payload exceeds the transfer limit")
        try:
            values = validate_values(json.loads(encoded))
        except (ValueError, UnicodeError) as error:
            raise LocalSecretsError("Invalid local execution secret payload") from error
        connection.sendall(b"A")
        return values


def local_handoff_token() -> str:
    """Share one private authentication token through the installation's data root."""
    root = Path(os.environ["VALKYRIE_LOCAL_DATA_ROOT"])
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".execution-handoff-token"
    if not path.exists():
        with tempfile.NamedTemporaryFile(mode="w", dir=root) as temporary:
            temporary.write(secrets.token_hex(32))
            temporary.flush()
            try:
                os.link(temporary.name, path)
            except FileExistsError:
                pass
    return path.read_text()
