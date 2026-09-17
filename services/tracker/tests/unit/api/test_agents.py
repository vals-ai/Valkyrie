"""Run with `uv run pytest tests/unit/api/test_agents.py`.

Cover agent listing and download-link routes.
"""

import io
import stat
import struct
import zipfile
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from botocore.exceptions import ClientError
from tracker import config
from tracker.exceptions import S3Error

import tracker.api.agents as agents_api
from main import app
from tracker.aws.runtime import AWSRuntime
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider

_client = TestClient(app)


def _agent_archive(
    member: str = "demo/run.py",
    contract: str | None = None,
    *,
    symlink: bool = False,
    unsupported_compression: bool = False,
) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "demo/contract.yaml",
            contract
            if contract is not None
            else "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n",
        )
        info = zipfile.ZipInfo(member)
        if symlink:
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "agent content")

    body = bytearray(stream.getvalue())
    if unsupported_compression:
        # Set the local and central-directory compression fields to an unsupported method.
        struct.pack_into("<H", body, 8, 99)
        struct.pack_into("<H", body, body.index(b"PK\x01\x02") + 10, 99)

    return bytes(body)


class TestAgentWrites:
    """Write validation and clean storage permission failures."""

    @pytest.mark.parametrize(
        "member", ["../escape", "/escape", "demo/../escape", "other/file", "demo\\file", "demo/C:file"]
    )
    def test_upload_rejects_unsafe_members(self, harness_headers: dict[str, str], member: str) -> None:
        response = _client.put(
            "/agents/demo",
            headers={**harness_headers, "Content-Type": "application/zip"},
            content=_agent_archive(member),
        )

        assert response.status_code == 400

    @pytest.mark.parametrize(
        "body",
        [
            b"not a zip",
            _agent_archive(contract="name: demo"),
            _agent_archive(symlink=True),
            _agent_archive(unsupported_compression=True),
            _agent_archive().replace(b"agent content", b"wrong content"),
        ],
    )
    def test_upload_rejects_invalid_contract_symlink_and_crc(
        self, harness_headers: dict[str, str], body: bytes
    ) -> None:
        response = _client.put(
            "/agents/demo", headers={**harness_headers, "Content-Type": "application/zip"}, content=body
        )

        assert response.status_code == 400

    @pytest.mark.parametrize(
        "setting", ["AGENT_UPLOAD_MAX_BYTES", "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", "AGENT_ARCHIVE_MAX_ENTRIES"]
    )
    def test_upload_limits_return_413(
        self, monkeypatch: pytest.MonkeyPatch, harness_headers: dict[str, str], setting: str
    ) -> None:
        monkeypatch.setattr(config, setting, 1)
        response = _client.put(
            "/agents/demo", headers={**harness_headers, "Content-Type": "application/zip"}, content=_agent_archive()
        )

        assert response.status_code == 413
        assert setting in response.json()["detail"]

    def test_actual_upload_bytes_are_bounded(
        self, monkeypatch: pytest.MonkeyPatch, harness_headers: dict[str, str]
    ) -> None:
        monkeypatch.setattr(config, "AGENT_UPLOAD_MAX_BYTES", 3)
        response = _client.put(
            "/agents/demo",
            headers={**harness_headers, "Content-Type": "application/zip", "Content-Length": "1"},
            content=b"oversized body",
        )

        assert response.status_code == 413

    def test_metadata_limits_precede_decompression(
        self, monkeypatch: pytest.MonkeyPatch, harness_headers: dict[str, str]
    ) -> None:
        body = _agent_archive()
        monkeypatch.setattr(config, "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", 1)
        monkeypatch.setattr(zipfile.ZipFile, "open", MagicMock(side_effect=AssertionError("archive was decompressed")))

        response = _client.put(
            "/agents/demo", headers={**harness_headers, "Content-Type": "application/zip"}, content=body
        )

        assert response.status_code == 413

    def test_actual_decompressed_bytes_are_bounded(
        self, monkeypatch: pytest.MonkeyPatch, harness_headers: dict[str, str]
    ) -> None:
        body = _agent_archive()
        monkeypatch.setattr(config, "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", 1000)
        monkeypatch.setattr(zipfile.ZipExtFile, "read", MagicMock(return_value=b"x" * 1001))

        response = _client.put(
            "/agents/demo", headers={**harness_headers, "Content-Type": "application/zip"}, content=body
        )

        assert response.status_code == 413

    @pytest.mark.parametrize("operation", ["put", "delete"])
    @pytest.mark.parametrize(
        "code, status, detail",
        [("AccessDenied", 403, "permission denied"), ("InternalError", 502, "storage operation failed")],
    )
    def test_storage_denial_is_actionable(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_headers: dict[str, str],
        operation: str,
        code: str,
        status: int,
        detail: str,
    ) -> None:
        error = S3Error("storage failed")
        error.__cause__ = ClientError({"Error": {"Code": code}}, "PutObject")
        monkeypatch.setattr(agents_api, "s3_object_exists", AsyncMock(return_value=True))
        monkeypatch.setattr(agents_api, "upload_stream_to_s3", AsyncMock(side_effect=error))
        monkeypatch.setattr(agents_api, "delete_from_s3", AsyncMock(side_effect=error))

        response = _client.request(
            operation,
            "/agents/demo",
            headers={**harness_headers, "Content-Type": "application/zip"},
            content=_agent_archive(),
        )

        assert response.status_code == status
        assert detail in response.json()["detail"]

    @pytest.mark.parametrize(
        "name, headers, status",
        [
            ("invalid name", {"Content-Type": "application/zip"}, 400),
            ("demo", {"Content-Type": "application/json"}, 415),
            ("demo", {"Content-Type": "application/zip", "Content-Length": "invalid"}, 400),
            ("demo", {"Content-Type": "application/zip", "Content-Length": "-1"}, 400),
        ],
    )
    def test_upload_rejects_invalid_headers(
        self, harness_headers: dict[str, str], name: str, headers: dict[str, str], status: int
    ) -> None:
        response = _client.put(f"/agents/{name}", headers={**harness_headers, **headers}, content=b"archive")

        assert response.status_code == status

    @pytest.mark.parametrize("value", ["0", "-1", "invalid"])
    def test_operator_limits_must_be_positive(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("AGENT_UPLOAD_MAX_BYTES", value)

        with pytest.raises(ValueError):
            getattr(config, "_positive_int_setting")("AGENT_UPLOAD_MAX_BYTES", 1073741824)


class TestAgentRoutes:
    """Agent catalog and download route behavior."""

    @pytest.mark.parametrize("method, path", [("DELETE", "/agents/demo"), ("GET", "/agents/demo/download-url")])
    def test_denied_head_returns_permission_error(
        self, monkeypatch: pytest.MonkeyPatch, harness_headers: dict[str, str], method: str, path: str
    ) -> None:
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.head_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")

        def s3_client(_provider: ExplicitCredentialsAWSClientProvider) -> AsyncMock:
            return client

        monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", s3_client)

        response = _client.request(method, path, headers=harness_headers)

        assert response.status_code == 403
        assert "permission denied" in response.json()["detail"]
        client.head_object.assert_awaited_once()

    def test_list_agents_returns_storage_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Agent listing must expose the names and timestamps returned by storage.

        Test cases:
        - The route serializes one stored agent and its last-modified timestamp.
        """

        list_agents = AsyncMock(return_value=[("agent-a", datetime(2026, 1, 2, tzinfo=timezone.utc))])
        monkeypatch.setattr(agents_api, "list_agents", list_agents)

        response = _client.get("/agents", headers=harness_headers)

        assert response.status_code == 200
        assert response.json() == {"agents": [{"name": "agent-a", "last_modified": "2026-01-02 00:00:00+00:00"}]}
        list_agents.assert_awaited_once_with(aws_runtime)

    def test_agent_download_url_uses_route_expiration(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Download links must use the route's configured expiration in both the signer and response.

        Test cases:
        - An existing agent receives a URL signed with the configured TTL.
        """
        exists = AsyncMock(return_value=True)
        presigned_url = AsyncMock(return_value="https://example.test/agent-a.zip")
        monkeypatch.setattr(agents_api, "s3_object_exists", exists)
        monkeypatch.setattr(agents_api, "create_presigned_url", presigned_url)

        response = _client.get("/agents/agent-a/download-url", headers=harness_headers)

        assert response.status_code == 200
        assert response.json() == {
            "name": "agent-a",
            "download_url": "https://example.test/agent-a.zip",
            "expires_in": agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        }
        exists.assert_awaited_once_with("agents/agent-a.zip", aws_runtime)
        presigned_url.assert_awaited_once_with(
            "agents/agent-a.zip",
            aws_runtime,
            expiration=agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        )

    def test_agent_download_url_returns_not_found_for_missing_agent(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
        harness_headers: dict[str, str],
    ) -> None:
        """Missing agent artifacts must return not found instead of a useless signed URL.

        Test cases:
        - A missing storage object receives a stable 404 response.
        """

        exists = AsyncMock(return_value=False)
        monkeypatch.setattr(agents_api, "s3_object_exists", exists)

        response = _client.get("/agents/missing/download-url", headers=harness_headers)

        assert response.status_code == 404
        assert response.json()["detail"] == "Agent 'missing' not found in S3"
        exists.assert_awaited_once_with("agents/missing.zip", aws_runtime)
