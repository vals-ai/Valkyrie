"""Aggregate decision for the required-ci gate.

Reads the relevance-selector result and, for every leaf job, whether the selector required
it (``required``) and the job's GitHub Actions ``result``. Fails the gate when:

* the selector job did not succeed, or
* a required leaf did not succeed (covers ``failure``, ``cancelled``, setup failure, and an
  unexpected ``skipped`` — e.g. a skipped prerequisite that suppressed a downstream matrix
  job), or
* a non-required leaf ended in ``failure``/``cancelled`` (defensive; a non-required leaf is
  expected to be ``skipped``).

A non-required leaf that is ``skipped`` is the only accepted skip. Publishes a summary of
selected / skipped / passed / failed / cancelled jobs to the step summary.
"""

from __future__ import annotations

import json
import os
import sys

_OK = "success"
_ACCEPTED_WHEN_NOT_REQUIRED = {"success", "skipped"}


def _summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    select_result = os.environ.get("SELECT_RESULT", "")
    jobs = json.loads(os.environ.get("JOBS_JSON", "[]"))

    failures: list[str] = []
    rows: list[str] = ["| job | required | result | verdict |", "| --- | --- | --- | --- |"]

    if select_result != _OK:
        failures.append(f"relevance selector did not succeed (result={select_result!r})")
    rows.append(f"| relevance-selector | (gate) | {select_result} | "
                f"{'PASS' if select_result == _OK else 'FAIL'} |")

    for job in jobs:
        name = job["name"]
        required = str(job.get("required", "false")).lower() == "true"
        result = job.get("result", "") or "(none)"

        if required:
            ok = result == _OK
            if not ok:
                failures.append(f"required leaf {name!r} did not succeed (result={result!r})")
        else:
            ok = result in _ACCEPTED_WHEN_NOT_REQUIRED
            if not ok:
                failures.append(f"non-required leaf {name!r} ended in {result!r}")

        rows.append(f"| {name} | {'yes' if required else 'no'} | {result} | "
                    f"{'PASS' if ok else 'FAIL'} |")

    _summary(["## required-ci aggregate", "", *rows])

    if failures:
        print("required-ci FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        _summary(["", "### Result: FAILED", *[f"- {failure}" for failure in failures]])
        return 1

    print("required-ci PASSED")
    _summary(["", "### Result: PASSED"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
