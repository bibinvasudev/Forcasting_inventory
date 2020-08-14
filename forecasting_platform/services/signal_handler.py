import faulthandler
import os
import signal
import sys
from typing import Any

import psutil


def initialize_faulthandler() -> None:
    """Enable more output for debugging in case of unexpected errors from native code modules (e.g. pyodbc).

    See: https://docs.python.org/3/library/faulthandler.html#faulthandler.enable
    """
    faulthandler.enable(
        file=sys.__stderr__  # We force using stderr to avoid "io.UnsupportedOperation: fileno" errors with pytest
    )

    if sys.platform == "linux" or sys.platform == "darwin":
        # Print current traceback for all threads:
        # MacOS: Press CTRL+T or ``kill -SIGINFO <PID>``.
        # Linux: Send SIGUSR1 (e.g. via ``kill -SIGUSR1 <PID>``).
        # Windows: This is not supported by Python.
        if hasattr(signal, "SIGINFO"):
            debug_signal = signal.SIGINFO
        else:
            debug_signal = signal.SIGUSR1

        faulthandler.register(debug_signal, file=sys.__stderr__)


def initialize_subprocess_termination_signal() -> None:
    """Capture KeyboardInterrupt signals and propagate them to all parallel running processes."""

    def sig_int(signal_num: int, _: Any) -> None:  # pragma: no cover
        print(
            f"Captured signal (signal={signal_num}) for process termination (pid={os.getpid()}, parent={os.getppid()})"
        )
        parent = psutil.Process(os.getppid())
        for child in parent.children():
            if child.pid != os.getpid():
                print(f"Attempting to terminate child process (pid={child.pid}, parent={os.getppid()})")
                child.terminate()
        print(f"Attempting to terminate current process (pid={os.getpid()})")
        psutil.Process(os.getpid()).terminate()

    signal.signal(signal.SIGINT, sig_int)
