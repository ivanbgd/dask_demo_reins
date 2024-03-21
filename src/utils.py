"""
File:       utils.py
Author:     Ivan Lazarević
Brief:      Utilities for the Dask demo.
"""
# Standard library imports
import sys
from typing import Any, NoReturn


def exit_program(msg: str) -> NoReturn:
    """Print message and exit program"""
    print("\n" + msg + "\nExiting program.")
    sys.exit(1)


def look_for_nan(dataframe: Any) -> bool:
    """Look for NaN or None in a Dask DataFrame"""
    has_nan: bool = False
    if dataframe.compute().isna().values.any():
        has_nan = True
    return has_nan


def drop_nans(dataframe: Any) -> Any:
    """Drop rows with a NaN or None and return the input DataFrame"""
    dataframe = dataframe.dropna(how="any")
    return dataframe
