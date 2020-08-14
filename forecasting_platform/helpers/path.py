from pathlib import Path
from typing import Union


def absolute_path(path: Union[str, Path]) -> Path:
    """Ensure absolute path, also on Windows (potential bug: https://bugs.python.org/issue36305).

    Args:
        path: Path to ensure is absolute.

    Returns:
        Path ensured to be absolute.

    """
    full_path = Path(path).resolve().absolute()
    assert full_path.is_absolute(), f"Expected path to be absolute: {full_path}"
    return full_path
