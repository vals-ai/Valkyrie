#!/usr/bin/env python3
"""PreToolUse hook for Bash: block a pull request description that breaks the template rules.

Runs before `gh pr create`, before `gh pr edit` and `gh api .../pulls/N` calls that set a body,
and checks the body against .github/PULL_REQUEST_TEMPLATE.md:

- The headings appear in the template's order. Related Issues may be left out.
- Summary is 3 to 4 sentences.
- Changes is short bullets: at most 10, each at most 250 characters.
- Related Issues, when present, links an issue.
- The template's checklist sections (Type of Change, Testing, Checklist) hold only checklist
  items, and Type of Change has one checked.

A failing body exits 2, which blocks the call and shows Claude what to fix.
"""

import json
import os
import re
import sys
from pathlib import Path

OPTIONAL = {"Related Issues"}
SUMMARY_SENTENCES = (3, 4)
MAX_CHANGES = 10
MAX_BULLET_CHARS = 250

INVOCATION = re.compile(r"(?:^|&&|;|\|\||\n)\s*(?:\w+=\S+\s+)*gh\s+(pr\s+(?:create|edit)|api)\b(.*)", re.S)
CHECKBOX = re.compile(r"^- \[[ xX]\] \S")


def sections(text: str) -> list[tuple[str, list[str]]]:
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    found: list[tuple[str, list[str]]] = []
    for line in text.splitlines():
        if line.startswith("## "):
            found.append((line[3:].strip(), []))
        elif found:
            found[-1][1].append(line.rstrip())
    return found


VALUE = r"""(?:\s+|=)(?:"((?:[^"\\]|\\.)*)"|'([^']*)'|(\S+))"""


def _value(match: re.Match[str]) -> str:
    double, single, bare = match.group(1), match.group(2), match.group(3)
    if double is not None:
        return re.sub(r"\\([\"\\$`])", r"\1", double)
    return single if single is not None else bare


def extract_body(subcommand: str, args: str, cwd: Path) -> str | None:
    """Return the body the command sets, None when it sets one we can't read, or "" for none."""
    heredoc = re.search(r"<<-?\s*['\"]?(\w+)['\"]?\n(.*?)\n\s*\1\b", args, re.S)
    if subcommand == "api":
        field = re.search(r"(?:-F|-f|--field|--raw-field)\s+(?:\"body=([^\"]*)\"|'body=([^']*)'|body=(\S+))", args)
        if not field:
            return ""
        value = next(group for group in field.groups() if group is not None)
        return (cwd / value[1:]).read_text() if value.startswith("@") else value
    body_file = re.search(r"(?:^|\s)(?:--body-file|-F)" + VALUE, args)
    if body_file:
        path = _value(body_file)
        if path == "-":
            return heredoc.group(2) if heredoc else None
        return (cwd / path).read_text()
    body = re.search(r"(?:^|\s)(?:--body(?!-file)|-b)" + VALUE, args)
    if body:
        if heredoc and "$(" in _value(body):
            return heredoc.group(2)
        value = _value(body)
        return None if "$" in value else value
    return ""


def problems(body: str, template: str) -> list[str]:
    expected = sections(template)
    order = [name for name, _ in expected]
    checklists = {name for name, lines in expected if any(CHECKBOX.match(line) for line in lines)}
    found = sections(body)
    names = [name for name, _ in found]
    issues: list[str] = []

    if [n for n in names if n in order] != [n for n in order if n in names] or set(names) - set(order):
        issues.append(
            f"Use exactly these headings, in this order: {', '.join(order)} (Related Issues may be left out)."
        )
    for name in order:
        if name not in names and name not in OPTIONAL:
            issues.append(f"Add the ## {name} section.")

    content = {name: [line for line in lines if line.strip() and not line.startswith("🤖")] for name, lines in found}

    summary = " ".join(content.get("Summary", []))
    summary = re.sub(r"`[^`]*`", "code", re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", summary))
    count = len(re.findall(r"[.!?](?=\s|$)", summary))
    low, high = SUMMARY_SENTENCES
    if "Summary" in content and not low <= count <= high:
        issues.append(f"Summary has {count} sentences; write {low} to {high} covering what changed and why.")

    bullets: list[str] = []
    stray: list[str] = []
    for line in content.get("Changes", []):
        if line.startswith("- "):
            bullets.append(line[2:].strip())
        elif line.startswith("  ") and bullets:
            bullets[-1] += " " + line.strip()
        else:
            stray.append(line.strip())
    if stray:
        issues.append(f"Changes holds only bullets; remove {len(stray)} other lines, such as {stray[0][:60]!r}.")
    elif "Changes" in content and not bullets:
        issues.append("Changes needs at least one bullet.")
    if len(bullets) > MAX_CHANGES:
        issues.append(f"Changes has {len(bullets)} bullets; keep it to {MAX_CHANGES} or fewer core changes.")
    for bullet in bullets:
        if len(bullet) > MAX_BULLET_CHARS:
            issues.append(f"Shorten this Changes bullet to {MAX_BULLET_CHARS} characters or fewer: {bullet[:60]!r}...")

    related = " ".join(content.get("Related Issues", []))
    if "Related Issues" in content and not re.search(r"#\d+|/issues/\d+", related):
        issues.append(
            "Related Issues must link an issue. Search the issues first, and delete the section if none relate."
        )

    for name in checklists:
        lines = content.get(name, [])
        stray = [line for line in lines if not CHECKBOX.match(line)]
        if stray:
            issues.append(f"{name} holds only checklist items; remove: {stray[0].strip()[:60]!r}.")
    if "Type of Change" in checklists and not any(
        re.match(r"^- \[[xX]\]", line) for line in content.get("Type of Change", [])
    ):
        issues.append("Check at least one Type of Change box.")
    return issues


def main() -> int:
    event = json.load(sys.stdin)
    command = (event.get("tool_input") or {}).get("command") or ""
    match = INVOCATION.search(command)
    if not match:
        return 0
    subcommand, args = match.group(1).split()[-1], match.group(2)
    if subcommand == "api" and not re.search(r"pulls/\d+", args):
        return 0

    root = Path(os.environ.get("CLAUDE_PROJECT_DIR") or event.get("cwd") or ".")
    template_path = root / ".github" / "PULL_REQUEST_TEMPLATE.md"
    if not template_path.is_file():
        return 0
    cwd = Path(event.get("cwd") or root)
    try:
        body = extract_body(subcommand, args, cwd)
    except OSError as error:
        print(f"The PR format check couldn't read the body file: {error}.", file=sys.stderr)
        return 2
    if body == "":
        if subcommand != "create":
            return 0
        print(
            "Give the PR a description that follows .github/PULL_REQUEST_TEMPLATE.md, passed "
            "with --body-file or --body.",
            file=sys.stderr,
        )
        return 2
    if body is None:
        print(
            "Write the PR description to a file and pass it with --body-file, so the format "
            "check can read it. Follow .github/PULL_REQUEST_TEMPLATE.md.",
            file=sys.stderr,
        )
        return 2

    found = problems(body, template_path.read_text())
    if not found:
        return 0
    print("The PR description doesn't follow .github/PULL_REQUEST_TEMPLATE.md. Fix it and try again:", file=sys.stderr)
    for issue in found:
        print(f"- {issue}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
