"""Retrieve transient credentials using the current dispatch claim."""

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from tracker.local.secret_pipe import MAX_SECRET_PAYLOAD_BYTES, LocalSecretsError, local_handoff_token, validate_values


@dataclass(frozen=True)
class LocalExecutionSecretsClient:
    base_url: str
    dispatch_id: str
    token: str = field(repr=False)

    @classmethod
    def from_env(cls, dispatch_id: str) -> "LocalExecutionSecretsClient":
        base_url = os.environ.get("VALKYRIE_LOCAL_TRACKER_URL", "")
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise LocalSecretsError("Local execution requires VALKYRIE_LOCAL_TRACKER_URL")
        return cls(base_url.rstrip("/"), dispatch_id, local_handoff_token())

    def _request(self, operation: str) -> bytes:
        request = urllib.request.Request(
            f"{self.base_url}/internal/local-execution-secrets/{self.dispatch_id}/{operation}",
            data=b"",
            headers={"X-Local-Handoff-Token": self.token},
            method="POST",
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=15) as response:
                data = response.read(MAX_SECRET_PAYLOAD_BYTES + 1)
        except (urllib.error.URLError, OSError) as error:
            raise LocalSecretsError(
                "Local execution secret handoff failed; resume with fresh execution secrets"
            ) from error
        if len(data) > MAX_SECRET_PAYLOAD_BYTES:
            raise LocalSecretsError("Local execution secret response exceeds the transfer limit")
        return data

    def receive(self) -> dict[str, str]:
        try:
            return validate_values(json.loads(self._request("receive")))
        except (ValueError, UnicodeError) as error:
            raise LocalSecretsError("Invalid local execution secret response") from error

    def acknowledge(self) -> None:
        self._request("acknowledge")
