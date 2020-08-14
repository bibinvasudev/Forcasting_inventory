from __future__ import annotations

from pathlib import Path
from typing import (
    Dict,
    Tuple,
)

import pandas as pd
from forecasting_platform import master_config

_identifier_mapping = (
    pd.read_csv(Path(master_config.default_data_loader_location) / "00 Config/identifier.csv")
    .set_index("mapping")["value"]
    .to_dict()
)


class Identifier(str):
    """Provide seamless identifier mapping via an externally defined mapping file.

    This helps with both type-safety and flexibility in the underlying data sources.
    """

    def __new__(cls, name: str, pass_through: bool = False) -> Identifier:
        """Construct a new str, to decouple hard-coded values with a customizable identifier mapping."""
        if pass_through:
            return name  # type: ignore

        self: Identifier = str.__new__(cls, _identifier_mapping[name])  # type: ignore
        return self

    def __getnewargs_ex__(self) -> Tuple[Tuple[object], Dict[str, object]]:
        """Allow pickle of identifier to use with multiprocessing.

        Returns:
            Raw string, to avoid any double mapping or recursion.

        Note:
            https://docs.python.org/3/library/pickle.html#object.__getnewargs_ex__
        """
        return (str(self),), {"pass_through": True}


class AccountID(Identifier):
    """AccountID type, behaves like a regular string, but is preferred due to better static type checking."""

    pass


class ProjectID(Identifier):
    """ProjectID type, behaves like a regular string, but is preferred due to better static type checking."""

    pass


class ContractID(Identifier):
    """ContractID type, behaves like a regular string, but is preferred due to better static type checking."""

    pass


class AirframeID(Identifier):
    """AirframeID type, behaves like a regular string, but is preferred due to better static type checking."""

    pass
