"""Run an isolated local Tracker on a non-default port (not shared dev/prod)."""
import argparse
from pathlib import Path

import uvicorn

from tracker.local import config
from tracker.serve import _register_source_release

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
args = parser.parse_args()
config.configure(args.config)
_register_source_release()
uvicorn.run("main:app", host="127.0.0.1", port=18100, log_config=None)
