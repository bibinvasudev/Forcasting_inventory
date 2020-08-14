from .signal_handler import (
    initialize_faulthandler,
    initialize_subprocess_termination_signal,
)


def test_initialize_faulthandler__runs_multiple_times_without_errors() -> None:
    initialize_faulthandler()
    initialize_faulthandler()


def test_initialize_subprocess_termination_signal__runs_multiple_times_without_errors() -> None:
    initialize_subprocess_termination_signal()
    initialize_subprocess_termination_signal()
