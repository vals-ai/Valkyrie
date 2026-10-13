"""Run a provider-style shell in a fresh process with its own controlling terminal."""

import errno
import os
import pty
import select
import shlex
import signal
import sys
import time
from contextlib import suppress


def main(command: str) -> int:
    process_id, terminal = pty.fork()
    if process_id == 0:
        os.execl("/bin/bash", "bash", "--noprofile", "--norc", "-ic", f"sh -c {shlex.quote(command)}; exit $?")

    reaped = False
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            ready, _, _ = select.select([terminal], [], [], 0.05)
            if ready:
                try:
                    output = os.read(terminal, 65536)
                    os.write(sys.stdout.fileno(), output)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise

            finished, status = os.waitpid(process_id, os.WNOHANG)
            if finished:
                reaped = True
                return os.waitstatus_to_exitcode(status)

        raise TimeoutError("Terminal command did not exit within the test deadline")
    finally:
        with suppress(OSError):
            foreground_group = os.tcgetpgrp(terminal)
            if foreground_group > 0 and foreground_group != os.getpgrp():
                os.killpg(foreground_group, signal.SIGKILL)

        os.close(terminal)
        if not reaped:
            with suppress(ProcessLookupError):
                os.killpg(process_id, signal.SIGKILL)
            os.waitpid(process_id, 0)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
