"""Build two isolated revisions and run the same external continuity probe against each."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

HERE = Path(__file__).resolve().parent
UV = ["uvx", "--from", "uv==0.9.18", "uv"]


def git(repo: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True).strip()


def snapshot(repo: Path, revision: str, directory: Path) -> dict[str, object]:
    """Copy source without changing branches or copying untracked files and credentials."""
    commit = git(repo, "rev-parse", "--verify", f"{'HEAD' if revision == 'working-tree' else revision}^{{commit}}")
    directory.mkdir()
    if revision == "working-tree":
        names = subprocess.check_output(["git", "-C", str(repo), "ls-files", "-z"]).decode().split("\0")
        for name in filter(None, names):
            if Path(name).parts[0] == "tmp":
                continue
            source = repo / name
            if not source.exists():
                continue
            if source.is_symlink() or not source.is_file():
                raise ValueError(f"Working-tree snapshots require regular tracked files: {name}")
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    else:
        archive_path = directory.parent / "source.tar"
        subprocess.run(["git", "-C", str(repo), "archive", "--output", str(archive_path), commit], check=True)
        with tarfile.open(archive_path) as archive:
            archive.extractall(directory, filter="data")
        archive_path.unlink()

    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file() and "tmp" != path.relative_to(directory).parts[0]:
            digest.update(path.relative_to(directory).as_posix().encode() + b"\0")
            digest.update(path.read_bytes())

    return {
        "requested_revision": revision,
        "commit": commit,
        "working_tree": revision == "working-tree",
        "source_digest": digest.hexdigest(),
    }


def run_command(arguments: list[str], cwd: Path, log: Path, environment: dict[str, str], timeout: int = 1200) -> None:
    with log.open("wb") as output:
        with subprocess.Popen(
            arguments, cwd=cwd, env=environment, stdout=output, stderr=subprocess.STDOUT, start_new_session=True
        ) as process:
            try:
                code = process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            if code:
                raise subprocess.CalledProcessError(code, arguments)


def compare_exit(results: list[dict[str, object]]) -> int:
    if {result.get("label") for result in results} != {"before", "after"}:
        return 2
    if any(result.get("outcome") in {"setup_error", "probe_error", "cleanup_error"} for result in results):
        return 2
    if any(
        result.get("label") == "after"
        and result.get("outcome") != ("interrupted" if result.get("scenario") == "maintenance" else "survived")
        for result in results
    ):
        return 1
    if any(result.get("label") == "before" and result.get("outcome") != "interrupted" for result in results):
        return 3

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=HERE.parent)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True, help="Commit/ref, or working-tree for tracked local edits")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--aws-profile", required=True)
    parser.add_argument(
        "--scenarios", nargs="+", choices=["replacement", "maintenance"], default=["replacement", "maintenance"]
    )
    args = parser.parse_args()
    if sys.version_info < (3, 12):
        parser.error("Use Python 3.12: uv run --python 3.12 --no-project tmp/compare.py ...")
    repo = args.repo.resolve(strict=True)
    env_file = args.env_file.resolve(strict=True)
    output = HERE / "runs" / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    output.mkdir(parents=True, mode=0o700)
    harness = output / "harness"
    harness.mkdir()
    harness_digest = hashlib.sha256()
    for name in ("compare.py", "probe.py", "host.py", "service.py"):
        content = (HERE / name).read_bytes()
        (harness / name).write_bytes(content)
        harness_digest.update(name.encode() + b"\0" + content)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"}
    }
    results: list[dict[str, object]] = []
    print(f"Evidence: {output}", flush=True)
    for label, revision in [("before", args.before), ("after", args.after)]:
        directory = output / label
        directory.mkdir()
        source = directory / "source"
        stage = "snapshot"
        try:
            identity = snapshot(repo, revision, source)
            identity["harness_digest"] = harness_digest.hexdigest()
            (directory / "source.json").write_text(json.dumps(identity, indent=2) + "\n")
            tracker = source / "services/tracker"
            stage = "dependencies"
            print(f"{label}: installing {revision}", flush=True)
            run_command(
                [*UV, "sync", "--project", str(tracker), "--frozen", "--dev"],
                source,
                directory / "install.log",
                environment,
            )
            stage = "build"
            build_environment = {**environment, "PYTHONPATH": str(tracker / "src")}
            print(f"{label}: building executor", flush=True)
            run_command(
                [
                    *UV,
                    "run",
                    "--project",
                    str(tracker),
                    "--frozen",
                    "python",
                    "services/executor_artifact/build.py",
                    "--source-revision",
                    str(identity["commit"]),
                    "--output-directory",
                    str(directory / "artifact"),
                ],
                source,
                directory / "build.log",
                build_environment,
            )
            for scenario in args.scenarios:
                stage = scenario
                evidence = directory / scenario
                evidence.mkdir()
                print(f"{label}: testing {scenario}", flush=True)
                probe_environment = {
                    **environment,
                    "PYTHONPATH": os.pathsep.join([str(tracker / "src"), str(tracker), str(source)]),
                }
                run_command(
                    [
                        str(tracker / ".venv/bin/python"),
                        str(harness / "probe.py"),
                        "--source",
                        str(source),
                        "--artifact",
                        str(directory / "artifact/executor.pex"),
                        "--evidence",
                        str(evidence),
                        "--scenario",
                        scenario,
                        "--env-file",
                        str(env_file),
                        "--aws-profile",
                        args.aws_profile,
                    ],
                    tracker,
                    evidence / "probe.log",
                    probe_environment,
                    timeout=1000,
                )
                result = json.loads((evidence / "result.json").read_text())
                results.append({"label": label, "scenario": scenario, **identity, **result})
                print(f"{label}/{scenario}: {result['outcome']}", flush=True)
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            results.append(
                {
                    "label": label,
                    "scenario": stage,
                    "requested_revision": revision,
                    "outcome": "setup_error",
                    "error_type": type(error).__name__,
                    "logs": str(directory),
                }
            )
            print(f"{label}/{stage}: setup_error; inspect {directory}", flush=True)
        finally:
            (output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")

    code = compare_exit(results)
    print(f"Comparison exit={code}. Details: {output / 'comparison.json'}", flush=True)
    return code


def interrupted(_signal: int, _frame: object) -> None:
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
