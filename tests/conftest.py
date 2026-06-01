"""Top-level conftest for CLI tests.

`valkyrie.cli.utils` transitively imports `tracker.config`, which refuses
to load without an explicit `AUTH_REQUIRED` env var. Set it early so test
collection succeeds without requiring a real Descope project.
"""

import os

os.environ.setdefault("AUTH_REQUIRED", "false")
