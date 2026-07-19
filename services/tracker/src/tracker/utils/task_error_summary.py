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
    """Normalize changing identifiers and whitespace before comparing task errors.

    Arguments
    - task_id: Task identifier that may appear in the error message.
    - error_message: Raw task error text.

    Returns
    - Comparable error text with dynamic values replaced.
    """

    # Replace run-specific values so equivalent failures compare consistently.
    normalized_message = error_message.casefold().replace(task_id.casefold(), "<task>")
    normalized_message = _ERROR_UUID_PATTERN.sub("<id>", normalized_message)
    normalized_message = _ERROR_SANDBOX_ID_PATTERN.sub("<sandbox>", normalized_message)

    return _ERROR_WHITESPACE_PATTERN.sub(" ", normalized_message).strip()


def _task_error_groups(
    task_errors: dict[str, str],
) -> list[tuple[int, str]]:
    """Group similar task errors and select one representative from each group.

    Arguments
    - task_errors: Latest error message keyed by task ID.

    Returns
    - Frequency-ordered pairs containing the group size and representative error.
    """
    entries = [
        (task_id, error_message, _normalize_task_error(task_id, error_message))
        for task_id, error_message in sorted(task_errors.items())
    ]
    normalized_messages = [entry[2] for entry in entries]

    # Build undirected edges between similar normalized errors.
    adjacent_entry_indices = [set[int]() for _ in entries]
    for entry_index, normalized_message in enumerate(normalized_messages):
        close_matches = set(
            get_close_matches(
                normalized_message,
                normalized_messages,
                n=len(normalized_messages),
                cutoff=_ERROR_SIMILARITY_THRESHOLD,
            )
        )
        for matched_index, candidate_message in enumerate(normalized_messages):
            if candidate_message in close_matches:
                adjacent_entry_indices[entry_index].add(matched_index)
                adjacent_entry_indices[matched_index].add(entry_index)

    # Traverse each connected component so every task belongs to one group.
    components: list[set[int]] = []
    unassigned_entry_indices = set(range(len(entries)))
    while unassigned_entry_indices:
        pending_entry_indices = [min(unassigned_entry_indices)]
        component: set[int] = set()
        while pending_entry_indices:
            entry_index = pending_entry_indices.pop()
            if entry_index not in unassigned_entry_indices:
                continue
            unassigned_entry_indices.remove(entry_index)
            component.add(entry_index)
            pending_entry_indices.extend(
                sorted(adjacent_entry_indices[entry_index] & unassigned_entry_indices, reverse=True)
            )
        components.append(component)

    groups: list[tuple[int, str, str]] = []

    # Prefer the shortest error for a concise and deterministic summary.
    for component in components:
        representative = min(
            (entries[entry_index] for entry_index in component),
            key=lambda entry: (len(entry[1]), entry[0]),
        )
        groups.append((len(component), representative[0], representative[1]))

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
