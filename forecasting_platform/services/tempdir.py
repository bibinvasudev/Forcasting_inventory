import os
import tempfile

TEMP_DIR_PREFIX = "forecast-tmp"


def initialize_tempdir() -> None:
    """Ensure that each process has their own temporary files directory.

    Problem is that h2o.H2OFrame uses ``tempfile.mkstemp(suffix=".csv")`` to create temporary files.
    There might be a race-condition when parallel worker processes generate the same "random" file names,
    based on the same random-seed.
    Different models might end up using the same temporary files, which could cause data inconsistencies.

    This issue is avoided by including the process-id in the temporary directory path.
    """
    if tempfile.tempdir:
        if TEMP_DIR_PREFIX in tempfile.tempdir:
            return  # Do not add prefix multiple times

    tempfile.tempdir = tempfile.mkdtemp(prefix=f"{TEMP_DIR_PREFIX}-{os.getpid()}-")
