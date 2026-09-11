"""GitHub checkout support for agent installation."""

import asyncio
import re
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import AsyncGenerator
from urllib.parse import urljoin


async def _run_git_command(repo_path: Path | None, *args: str) -> str:
    command = [
        "git",
        "-c",
        "protocol.allow=never",
        "-c",
        "protocol.https.allow=always",
        "-c",
        "http.followRedirects=false",
        "-c",
        "submodule.recurse=false",
    ]
    if repo_path is not None:
        command.extend(["-C", str(repo_path)])
    process = await asyncio.create_subprocess_exec(
        *command,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(300):
            stdout, stderr = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise ChildProcessError(f"Git command failed: {stderr.decode(errors='replace')}")

    return stdout.decode()


async def _checkout_submodules(repository_path: Path, repository_url: str) -> None:
    """Validate each level's GitHub destinations before fetching its submodules."""
    if not await asyncio.to_thread((repository_path / ".gitmodules").is_file):
        return
    config = await _run_git_command(repository_path, "config", "--file", ".gitmodules", "--null", "--list")
    submodules: dict[str, dict[str, str]] = {}
    for entry in config.split("\0"):
        key, _, value = entry.partition("\n")
        match = re.fullmatch(r"submodule\.(.+)\.(path|url)", key)
        if match:
            name, field = match.groups()
            submodules.setdefault(name, {})[field] = value
    for name, fields in submodules.items():
        path, url = fields.get("path", ""), fields.get("url", "")
        if url.startswith(("./", "../")):
            url = urljoin(repository_url.rstrip("/") + "/", url)
        match = re.fullmatch(r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/?", url)
        if match is None or any(part in {".", ".."} for part in match.groups()):
            raise ValueError("Agent submodules must use HTTPS GitHub repository URLs")
        if "\\" in path or any(part in {"", ".", ".."} for part in path.split("/")):
            raise ValueError("Invalid agent submodule path")
        submodule_path = repository_path / path
        resolved = await asyncio.to_thread(submodule_path.resolve)
        if not resolved.is_relative_to(repository_path.resolve()):
            raise ValueError("Agent submodule path escapes repository")
        await _run_git_command(repository_path, "config", "--local", f"submodule.{name}.url", url)
        await _run_git_command(repository_path, "submodule", "update", "--init", "--checkout", "--", path)
        await _checkout_submodules(submodule_path, url)


@asynccontextmanager
async def checkout_agent(github_url: str) -> AsyncGenerator[Path, None]:
    """Check out a GitHub repository or tree URL, including submodules."""
    match = re.fullmatch(
        r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(?:/tree/([^/]+)/(.+?))?/?",
        github_url,
    )
    if match is None:
        raise ValueError("Invalid GitHub URL; expected a repository or /tree/branch/subfolder URL")
    owner, repository, branch, subfolder = match.groups()
    if owner in {".", ".."} or repository in {".", ".."} or (branch and branch.startswith("-")):
        raise ValueError("Invalid GitHub repository or branch")
    if subfolder and (
        "\\" in subfolder or subfolder.startswith("-") or any(part in {"", ".", ".."} for part in subfolder.split("/"))
    ):
        raise ValueError("Invalid GitHub subfolder")

    with TemporaryDirectory() as temporary:
        repository_path = Path(temporary) / "repository"
        clone_args = ["clone", "--no-recurse-submodules"]
        if subfolder:
            clone_args.extend(["--no-checkout", "--filter=blob:none"])
        clone_args.extend([f"https://github.com/{owner}/{repository}", str(repository_path)])
        await _run_git_command(None, *clone_args)
        if subfolder:
            await _run_git_command(repository_path, "sparse-checkout", "init", "--cone")
            await _run_git_command(repository_path, "sparse-checkout", "set", "--", subfolder)
        await _run_git_command(repository_path, "checkout", branch or "HEAD", "--")
        await _checkout_submodules(repository_path, f"https://github.com/{owner}/{repository}")
        agent_path = repository_path / subfolder if subfolder else repository_path
        if not agent_path.resolve().is_relative_to(repository_path.resolve()):
            raise ValueError("GitHub subfolder escapes repository")
        if not agent_path.is_dir():
            raise FileNotFoundError(f"Subfolder not found: {subfolder}")

        yield agent_path
