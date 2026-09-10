"""Agent management, GitHub checkout, and archive safety.

Run: uv run pytest tests/unit/sdk/test_agents_resource.py
"""

import io
import stat
import zipfile
from pathlib import Path

import httpx
import pytest

from tests.unit.sdk.conftest import ClientFactory
from valkyrie.sdk import agent_install
from valkyrie.sdk.agent_bundle import extract_agent_archive, get_agent_zip_stream

_CONTRACT = "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n"


def _archive(member: str = "demo/run.py", *, symlink: bool = False) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("demo/contract.yaml", _CONTRACT)
        info = zipfile.ZipInfo(member)
        if symlink:
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "agent content")

    return stream.getvalue()


def _unexpected_request(_request: httpx.Request) -> httpx.Response:
    pytest.fail("invalid input reached tracker")


class TestAgentsResource:
    """SDK library requests and external archive transfers."""

    async def test_list_returns_typed_agents(self, make_client: ClientFactory) -> None:
        def listing(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"agents": [{"name": "demo"}]})

        async with make_client(listing) as client:
            response = await client.agents.list()

        assert response.agents[0].name == "demo"

    @pytest.mark.parametrize("name", ["", " ", "agent one", ".", "..", "team/agent", "a\\b", "agent\n"])
    async def test_invalid_names_never_reach_tracker(self, make_client: ClientFactory, name: str) -> None:
        async with make_client(_unexpected_request) as client:
            for operation in (client.agents.download_url, client.agents.remove):
                with pytest.raises(ValueError, match="Invalid agent name"):
                    await operation(name)

    async def test_download_never_forwards_tracker_credentials(
        self,
        make_client: ClientFactory,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        async def download(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
            assert request.url.host == "download.test"
            assert "authorization" not in request.headers
            assert not any(header.startswith("x-harness") for header in request.headers)

            return httpx.Response(200, content=_archive())

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", download)

        def download_url(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "name": "demo",
                    "download_url": "https://download.test/demo.zip",
                    "expires_in": 300,
                },
            )

        async with make_client(download_url) as client:
            with pytest.raises(ValueError, match="max_archive_bytes"):
                await client.agents.download("demo", tmp_path, max_archive_bytes=1)
            assert not (tmp_path / "demo").exists()
            path = await client.agents.download("demo", tmp_path)

            assert (path / "run.py").read_text() == "agent content"
            with pytest.raises(FileExistsError):
                await client.agents.download("demo", tmp_path)
            (path / "obsolete").touch()
            await client.agents.download("demo", tmp_path, overwrite=True)

        assert not (path / "obsolete").exists()

    async def test_install_subfolder_pushes_override_without_rewriting_contract(
        self,
        make_client: ClientFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def git(_repository: Path | None, *arguments: str) -> None:
            if arguments[0] == "clone":
                folder = Path(arguments[-1]) / "agents" / "demo"
                folder.mkdir(parents=True)
                (folder / "contract.yaml").write_text(_CONTRACT)

        def upload(request: httpx.Request) -> httpx.Response:
            assert request.method == "PUT"
            assert request.url.path == "/agents/alias"
            assert request.headers["content-type"] == "application/zip"
            with zipfile.ZipFile(io.BytesIO(request.content)) as archive:
                assert archive.read("alias/contract.yaml").decode() == _CONTRACT

            return httpx.Response(200, json={"name": "alias"})

        monkeypatch.setattr(agent_install, "_run_git_command", git)
        async with make_client(upload) as client:
            response = await client.agents.install(
                "https://github.com/example/agents/tree/main/agents/demo", name="alias"
            )

        assert response.name == "alias"

    @pytest.mark.parametrize("nested_url", ["https://github.com/example/nested", "https://example.test/nested"])
    async def test_submodules_validate_each_level_before_fetch(
        self, monkeypatch: pytest.MonkeyPatch, nested_url: str
    ) -> None:
        repositories: list[Path] = []
        fetched: list[str] = []

        async def git(repository: Path | None, *arguments: str) -> str:
            if arguments[0] == "clone":
                root = Path(arguments[-1])
                root.mkdir()
                (root / ".gitmodules").touch()
                repositories.append(root)
                return ""
            assert repository is not None
            if arguments[:3] == ("config", "--file", ".gitmodules"):
                url = "../child" if repository == repositories[0] else nested_url
                return f"submodule.child.path\nchild\0submodule.child.url\n{url}\0"
            if arguments[0] == "submodule":
                fetched.append(str(repository))
                child = repository / "child"
                child.mkdir()
                if repository == repositories[0]:
                    (child / ".gitmodules").touch()
            return ""

        monkeypatch.setattr(agent_install, "_run_git_command", git)
        if nested_url.startswith("https://github.com/"):
            async with agent_install.checkout_agent("https://github.com/example/parent") as path:
                assert (path / "child" / "child").is_dir()
            assert len(fetched) == 2
        else:
            with pytest.raises(ValueError, match="HTTPS GitHub"):
                async with agent_install.checkout_agent("https://github.com/example/parent"):
                    pytest.fail("unvalidated submodule accepted")
            assert fetched == [str(repositories[0])]

    @pytest.mark.parametrize(
        "url",
        [
            "https://gitlab.com/example/agent",
            "https://github.com/example/repo/tree/main/../escape",
            "https://github.com/example/repo/tree/-bad/agents",
            "https://github.com/example/repo\n",
        ],
    )
    async def test_install_rejects_unsafe_urls(self, make_client: ClientFactory, url: str) -> None:
        async with make_client(_unexpected_request) as client:
            with pytest.raises(ValueError):
                await client.agents.install(url)


class TestAgentArchive:
    """Bundle exclusions and extraction without filesystem escapes."""

    def test_bundle_excludes_caches_and_rejects_symlinks(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        source.mkdir()
        (source / "run.py").write_text("run")
        (source / ".env").write_text("SECRET=excluded")
        (source / "__pycache__").mkdir()
        (source / "__pycache__" / "run.pyc").touch()
        with get_agent_zip_stream("alias", source) as stream, zipfile.ZipFile(stream) as archive:
            assert archive.namelist() == ["alias/run.py"]

        (source / "link").symlink_to(tmp_path)

        with pytest.raises(ValueError, match="symlinks"):
            with get_agent_zip_stream("alias", source):
                pytest.fail("symlink accepted")

    @pytest.mark.parametrize(
        "member",
        [
            "../escape",
            "/escape",
            "other/file",
            "demo/../escape",
            "demo\\escape",
            "demo/C:escape",
            "demo/contract.yaml",
            "demo/CONTRACT.yaml",
            "demo/./file",
            "demo//file",
        ],
    )
    def test_unsafe_archive_preserves_existing_target(self, tmp_path: Path, member: str) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")

        with pytest.raises(ValueError, match="Unsafe"):
            extract_agent_archive(io.BytesIO(_archive(member)), "demo", tmp_path, overwrite=True)

        assert (target / "keep").read_text() == "original"
        assert sorted(path.name for path in tmp_path.iterdir()) == ["demo"]

    def test_symlinks_and_corruption_are_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            extract_agent_archive(io.BytesIO(_archive(symlink=True)), "demo", tmp_path, overwrite=False)

        corrupted = _archive().replace(b"agent content", b"wrong content")

        with pytest.raises(zipfile.BadZipFile, match="CRC"):
            extract_agent_archive(io.BytesIO(corrupted), "demo", tmp_path, overwrite=False)

        assert not (tmp_path / "demo").exists()

    def test_existing_target_symlink_is_never_followed(self, tmp_path: Path) -> None:
        (tmp_path / "demo").symlink_to(tmp_path / "elsewhere", target_is_directory=True)

        with pytest.raises(FileExistsError):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True)

    @pytest.mark.parametrize("limit", ["max_archive_bytes", "max_expanded_bytes", "max_entries"])
    def test_limits_preserve_existing_target(self, tmp_path: Path, limit: str) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")

        with pytest.raises(ValueError, match=limit):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True, **{limit: 1})

        assert (target / "keep").read_text() == "original"
        assert list(tmp_path.iterdir()) == [target]

    @pytest.mark.parametrize("rollback_fails", [False, True])
    def test_failed_overwrite_preserves_previous_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rollback_fails: bool
    ) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")
        rename = Path.rename

        def fail_replacement(path: Path, destination: str | Path) -> Path:
            if path.name == "demo" and path.parent != tmp_path:
                if rollback_fails or not path.parent.name.startswith(".demo-backup-"):
                    raise OSError("rename failed")
            return rename(path, destination)

        monkeypatch.setattr(Path, "rename", fail_replacement)
        with pytest.raises(OSError):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True)

        if rollback_fails:
            backup = next(tmp_path.glob(".demo-backup-*/demo"))
            assert (backup / "keep").read_text() == "original"
        else:
            assert (target / "keep").read_text() == "original"
            assert list(tmp_path.iterdir()) == [target]

    def test_actual_expanded_bytes_are_bounded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def expanded(_source: zipfile.ZipExtFile, _size: int = -1) -> bytes:
            return b"x" * 1001

        monkeypatch.setattr(zipfile.ZipExtFile, "read", expanded)

        with pytest.raises(ValueError, match="max_expanded_bytes"):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=False, max_expanded_bytes=1000)

        assert not (tmp_path / "demo").exists()
