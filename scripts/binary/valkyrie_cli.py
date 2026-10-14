# PyInstaller puts the entry script's directory on the import path, so the script can't live in
# src/valkyrie/cli, where queue.py would shadow the standard library's queue.
from valkyrie.cli.entry import main

main()
