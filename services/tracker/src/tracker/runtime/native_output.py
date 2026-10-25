"""Discover native files through the attached sandbox, without artifact storage."""

from dataclasses import dataclass
from pathlib import PurePosixPath
from shlex import quote

from benchmark_service import Sandbox, SandboxError


@dataclass(frozen=True)
class NativeFile:
    remote_path: str
    size_bytes: int


def valid_native_path(path: str) -> bool:
    return (
        bool(path)
        and "\\" not in path
        and "\0" not in path
        and all(part not in ("", ".", "..") for part in path.split("/"))
    )


async def discover_native_files(sandbox: Sandbox, output_path: str) -> dict[str, NativeFile] | None:
    """Discover regular files beneath the declared output path."""
    target = str(PurePosixPath(output_path))
    if not target.startswith("/"):
        target = f"./{target}"
    # POSIX ls exposes the byte size without reading file contents. Names are emitted
    # separately with NUL delimiters, so spaces and newlines cannot corrupt metadata.
    # A vanished candidate is omitted; suppress ls diagnostics to keep the NUL stream parseable.
    metadata = (
        "for path do "
        'metadata=$(LC_ALL=C ls -dn "$path" 2>/dev/null) || { '
        '  if [ ! -e "$path" ] && [ ! -L "$path" ] && [ -x "${path%/*}/" ]; then continue; fi; '
        "  exit 1; "
        "}; "
        "size=$(printf '%s\\n' \"$metadata\" | awk 'NR == 1 {print $5}') || exit 1; "
        'printf "%s\\0%s\\0" "$path" "$size"; '
        "done"
    )
    command = (
        f"target={quote(target)}; "
        'if [ -L "$target" ]; then exit 0; '
        'elif [ -f "$target" ]; then printf "f\\0"; '
        'elif [ -d "$target" ]; then printf "d\\0"; '
        "else exit 0; fi; "
        f'find "$target" -type f -exec sh -c {quote(metadata)} sh {{}} +'
    )
    # Modal applies timeout to the first word, so shell syntax must run inside one sh.
    result = await sandbox.exec(f"sh -c {quote(command)}", timeout=30)
    if result.exit_code != 0:
        raise SandboxError(f"Native output discovery failed: {result.output}")
    if not result.output:
        return None

    kind, *fields = result.output.split("\0")
    files: dict[str, NativeFile] = {}
    for remote_path, size in zip(fields[:-1:2], fields[1:-1:2], strict=True):
        relative = (
            PurePosixPath(remote_path).name
            if kind == "f"
            else str(PurePosixPath(remote_path).relative_to(PurePosixPath(target)))
        )
        files[relative] = NativeFile(remote_path=remote_path, size_bytes=int(size))

    return files
