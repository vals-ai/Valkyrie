"""Build run-level summaries from current task errors."""

import re
from difflib import get_close_matches

_ERROR_SIMILARITY_THRESHOLD = 0.75
_ERROR_UUID_PATTERN = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_ERROR_SANDBOX_ID_PATTERN = re.compile(r"\bsb-[a-z0-9-]+\b", re.IGNORECASE)
_ERROR_WHITESPACE_PATTERN = re.compile(r"\s+")


def _normalize_task_error(task_id: str, error_message: str) -> str:
    normalized_message = error_message.casefold().replace(task_id.casefold(), "<task>")
    normalized_message = _ERROR_UUID_PATTERN.sub("<id>", normalized_message)
    normalized_message = _ERROR_SANDBOX_ID_PATTERN.sub("<sandbox>", normalized_message)

    return _ERROR_WHITESPACE_PATTERN.sub(" ", normalized_message).strip()


def _task_error_groups(
    task_errors: dict[str, str],
) -> list[tuple[int, str]]:
    entries = [
        (task_id, error_message, _normalize_task_error(task_id, error_message))
        for task_id, error_message in sorted(task_errors.items())
    ]
    normalized_messages = [entry[2] for entry in entries]
    grouped_messages = {
        tuple(
            sorted(
                get_close_matches(
                    normalized_message,
                    normalized_messages,
                    n=len(normalized_messages),
                    cutoff=_ERROR_SIMILARITY_THRESHOLD,
                )
            )
        )
        for normalized_message in normalized_messages
    }

    groups: list[tuple[int, str, str]] = []
    for messages in grouped_messages:
        representative = min(
            (entry for entry in entries if entry[2] in messages),
            key=lambda entry: (len(entry[1]), entry[0]),
        )
        groups.append((len(messages), representative[0], representative[1]))

    return [
        (count, error_message) for count, _, error_message in sorted(groups, key=lambda group: (-group[0], group[1]))
    ]


def summarize_task_errors(task_errors: dict[str, str]) -> str:
    """Build a terminal run error with one representative per current error group.

    Arguments
    - task_errors: Latest error message keyed by task ID.

    Returns
    - Run-level error text containing one representative task error per group.
    """
    base_message = "No tasks were completed successfully."
    if not task_errors:
        return base_message

    groups = _task_error_groups(task_errors)
    group_label = "task error" if len(groups) == 1 else "task errors"
    summary_lines = [f"{base_message} {len(groups)} distinct {group_label}:"]
    summary_lines.extend(f"- {count}/{len(task_errors)} tasks: {error_message}" for count, error_message in groups)

    return "\n".join(summary_lines)
