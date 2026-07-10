import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from tracker.types import (
    AgentDownloadURLResponse,
    AgentEntry,
    AgentsResponse,
    AgentUploadURLResponse,
    StatusResponse,
)

import valkyrie.cli.agent.storage as storage
from valkyrie.schemas import AgentConfig


def _agent_zip(name: str = "agent") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            f"{name}/contract.yaml",
            f"name: {name}\ninstall_cmd: echo install\nrun_cmd: echo {{problem_statement_path}}\n",
        )
        archive.writestr(f"{name}/README.md", "agent docs")
    return stream.getvalue()


class _Tracker:
    deleted: list[str] = []
    upload_sizes: list[int] = []

    def __init__(self) -> None:
        pass

    def close(self) -> None:
        pass

    def create_agent_upload_url(self, name: str, size_bytes: int) -> AgentUploadURLResponse:
        self.upload_sizes.append(size_bytes)
        return AgentUploadURLResponse(
            name=name,
            upload_url="https://s3.test/upload",
            fields={"policy": "signed"},
            expires_in=300,
        )

    def get_agent_download_url(self, name: str) -> AgentDownloadURLResponse:
        return AgentDownloadURLResponse(name=name, download_url="https://s3.test/download", expires_in=300)

    def list_agents(self) -> AgentsResponse:
        return AgentsResponse(agents=[AgentEntry(name="agent", last_modified="2026-01-02T00:00:00+00:00")])

    def delete_agent(self, name: str) -> StatusResponse:
        self.deleted.append(name)
        return StatusResponse(status="success")


def _hosted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "load_config", lambda: {"api_key": "personal-key"})
    monkeypatch.setattr(storage, "TrackerService", _Tracker)


async def test_hosted_push_streams_zip_through_presigned_post(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _hosted(monkeypatch)
    agent_path = tmp_path / "agent"
    agent_path.mkdir()
    (agent_path / "contract.yaml").write_text(
        "name: agent\ninstall_cmd: echo install\nrun_cmd: echo {problem_statement_path}\n"
    )
    uploaded: list[bytes] = []

    class Client:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> "Client":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def post(self, url: str, *, data: dict[str, str], files: dict[str, tuple[str, object, str]]) -> httpx.Response:
            assert url == "https://s3.test/upload"
            assert data == {"policy": "signed"}
            uploaded.append(files["file"][1].read())  # type: ignore[union-attr]
            return httpx.Response(204, request=httpx.Request("POST", url))

    monkeypatch.setattr(storage.httpx, "Client", Client)
    _Tracker.upload_sizes = []

    await storage.push_agent("agent", agent_path)

    assert _Tracker.upload_sizes == [len(uploaded[0])]
    with zipfile.ZipFile(io.BytesIO(uploaded[0])) as archive:
        assert "agent/contract.yaml" in archive.namelist()


async def test_hosted_list_and_remove_use_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    _hosted(monkeypatch)
    _Tracker.deleted = []

    agents = await storage.list_agents()
    await storage.remove_agent("agent")

    assert agents == [("agent", datetime(2026, 1, 2, tzinfo=timezone.utc))]
    assert _Tracker.deleted == ["agent"]


async def test_hosted_download_and_start_contract_use_tracker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _hosted(monkeypatch)
    zip_bytes = _agent_zip()

    class AsyncClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> "AsyncClient":
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

        async def get(self, url: str) -> httpx.Response:
            return httpx.Response(200, content=zip_bytes, request=httpx.Request("GET", url))

    monkeypatch.setattr(storage.httpx, "AsyncClient", AsyncClient)

    await storage.download_agent("agent", tmp_path / "download")
    contract = await storage.get_contract_from_s3("agent", AgentConfig())

    assert (tmp_path / "download" / "agent" / "README.md").read_text() == "agent docs"
    assert contract.name == "agent"
    assert contract.run_cmd == "echo {problem_statement_path}"


async def test_legacy_list_keeps_direct_s3_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "load_config", lambda: {"AWS_ACCESS_KEY_ID": "key"})
    monkeypatch.setattr(storage, "_fetch_bucket_name", lambda: "legacy-bucket")
    expected = [("legacy-agent", datetime(2026, 1, 2, tzinfo=timezone.utc))]
    captured: list[str] = []

    async def list_agents(_aws: object, bucket: str) -> list[tuple[str, datetime]]:
        captured.append(bucket)
        return expected

    monkeypatch.setattr(storage, "_aws_credentials", lambda: object())
    monkeypatch.setattr(storage, "list_s3_agents", list_agents)

    assert await storage.list_agents() == expected
    assert captured == ["legacy-bucket"]
