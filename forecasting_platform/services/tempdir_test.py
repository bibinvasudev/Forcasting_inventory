import tempfile

from .tempdir import (
    TEMP_DIR_PREFIX,
    initialize_tempdir,
)


def test_initialize_tempdir_is_idempotent() -> None:
    initialize_tempdir()
    assert tempfile.tempdir is not None
    assert tempfile.tempdir.count(TEMP_DIR_PREFIX) == 1

    initialize_tempdir()
    assert tempfile.tempdir is not None
    assert tempfile.tempdir.count(TEMP_DIR_PREFIX) == 1
