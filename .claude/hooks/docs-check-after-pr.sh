#!/bin/sh
# PostToolUse hook for Bash. After `gh pr create` opens a pull request, asks Claude to run
# the updating-docs skill on it: the docs pages it changes must follow docs/AGENTS.md, no
# public page may still describe behavior it changes, and Vals AI-only material belongs
# under internal/.
#
# Skips pull requests whose changed files are all under infra/, internal/, .github/, or a
# tests directory, since those can't make the public docs stale.
set -eu

input=$(cat)

command=$(printf '%s' "$input" | jq -r '.tool_input.command // ""')
printf '%s' "$command" | grep -Eq '(^|[^[:alnum:]_-])gh +pr +create( |$)' || exit 0

url=$(printf '%s' "$input" | jq -r '(.tool_response // "") | tostring | try capture("(?<url>https://[^\\s\"\\\\]+/pull/[0-9]+)").url catch ""')
[ -n "$url" ] || exit 0

if files=$(gh pr view "$url" --json files --jq '.files[].path' 2>/dev/null) && [ -n "$files" ]; then
  printf '%s\n' "$files" | grep -Eqv '^(infra|internal|\.github)/|(^|/)tests/' || exit 0
fi

jq -n --arg url "$url" '{
  hookSpecificOutput: {
    hookEventName: "PostToolUse",
    additionalContext: ("A pull request was just opened: " + $url + ". Run the updating-docs skill on it now. Check that the docs/ pages it changes follow docs/AGENTS.md, that no public page, generated reference, or README still describes behavior it changes, and that any Vals AI-only material it adds is under internal/. Report the findings. Do not push fixes or edit the pull request until the user approves.")
  }
}'
