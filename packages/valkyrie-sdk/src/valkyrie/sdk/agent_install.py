"""GitHub checkout support for agent installation."""

import asyncio
import re
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import AsyncGenerator


async def _run_git_command(repo_path: Path | None, *args: str) -> None:
    command = ["git"]
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
            _, stderr = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise ChildProcessError(f"Git command failed: {stderr.decode(errors='replace')}")


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
        clone_args = ["clone"]
        if subfolder:
            clone_args.extend(["--no-checkout", "--filter=blob:none"])
        clone_args.extend([f"https://github.com/{owner}/{repository}", str(repository_path)])
        await _run_git_command(None, *clone_args)
        if subfolder:
            await _run_git_command(repository_path, "sparse-checkout", "init", "--cone")
            await _run_git_command(repository_path, "sparse-checkout", "set", "--", subfolder)
        await _run_git_command(repository_path, "checkout", branch or "HEAD", "--")
        await _run_git_command(repository_path, "submodule", "update", "--init", "--recursive")
        agent_path = repository_path / subfolder if subfolder else repository_path
        if not agent_path.resolve().is_relative_to(repository_path.resolve()):
            raise ValueError("GitHub subfolder escapes repository")
        if not agent_path.is_dir():
            raise FileNotFoundError(f"Subfolder not found: {subfolder}")

        yield agent_path
