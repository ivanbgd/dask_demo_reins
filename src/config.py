"""
File:       config.py
Author:     Ivan Lazarević
Brief:      Program configuration and constants.
"""
from pathlib import Path
from typing import Literal

# If True, enables checks for NaN and None in data, and their removal.
PROCESS_NAN: Literal[True] = True

# Data files
_DATA = Path("data")
CONTRACT = Path(_DATA / "contract.json")
DEALS = Path(_DATA / "deals.csv")
LOSSES = Path(_DATA / "losses.csv")

# Test files
_TEST_DATA = Path("tests/data")
LOSSES_TEST_LARGE = Path(_TEST_DATA / "losses_test_large.csv")
LOSSES_TEST_NAN = Path(_TEST_DATA / "losses_test_nan.csv")
