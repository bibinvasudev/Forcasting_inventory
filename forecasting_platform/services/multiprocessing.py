import multiprocessing
from multiprocessing.context import SpawnContext


def initialize_multiprocessing() -> SpawnContext:
    """Ensure that we use the same multi-processing method on all platforms.

    ``spawn`` is the only method supported on Windows, and it can also be used on Unix systems.

    Another advantage is that ``spawn`` starts a fresh new Python environment,
    which gives us better control over the execution state of sub-processes.

    Note:
        https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
    """
    return multiprocessing.get_context("spawn")
