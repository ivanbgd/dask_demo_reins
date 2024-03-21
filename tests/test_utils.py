"""
File:       test_utils.py
Author:     Ivan Lazarević
Brief:      Automated unit tests for utilities for the Dask demo.
"""
# Standard library imports
import unittest
from typing import Any, Dict, List, Union

# Third party library imports
import dask.dataframe as dd  # type: ignore
import pandas as pd  # type: ignore
from numpy.core.numeric import NaN  # type: ignore

# Local modules imports
from src.utils import look_for_nan, drop_nans
from src.type_aliases import Deal, DealNone


class TestUtils(unittest.TestCase):
    """Class for automated testing of Dask Demo utilities"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @staticmethod
    def _test_look_for_nan_none(source_dict: DealNone) -> bool:
        """Helper function for multiple tests"""
        source_df = pd.DataFrame(source_dict, index=[0, 1, 4])
        dask_df: Any = dd.from_pandas(source_df, chunksize=3)
        return look_for_nan(dask_df)

    def test_look_for_nan_true(self) -> None:
        """Tests looking for an existing NaN in a Dask Dataframe."""
        source_dict: Deal = {
            "DealId": [1, 2, NaN],
            "Company": ["ClientA", "ClientA", "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        has_nan: bool = self._test_look_for_nan_none(source_dict)
        self.assertEqual(has_nan, True)

    def test_look_for_nan_false(self) -> None:
        """Tests looking for a non-existent NaN in a Dask Dataframe."""
        source_dict: Deal = {
            "DealId": [1, 2, 5],
            "Company": ["ClientA", "ClientA", "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        has_nan: bool = self._test_look_for_nan_none(source_dict)
        self.assertEqual(has_nan, False)

    def test_look_for_none(self) -> None:
        """Tests looking for a None in a Dask Dataframe."""
        source_dict: DealNone = {
            "DealId": [1, 2, 5],
            "Company": ["ClientA", None, "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        has_none: bool = self._test_look_for_nan_none(source_dict)
        self.assertEqual(has_none, True)

    @unittest.expectedFailure
    def test_look_for_none_expected_failure(self) -> None:
        """Tests looking for a None in a Dask Dataframe."""
        source_dict: Deal = {
            "DealId": [1, 2, 5],
            "Company": ["ClientA", "ClientA", "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        has_none: bool = self._test_look_for_nan_none(source_dict)
        self.assertEqual(has_none, True)

    def test_drop_nans(self) -> None:
        """Tests dropping of rows with an existing NaN or None in a Dask Dataframe."""
        source_dict: DealNone = {
            "DealId": [1, 2, NaN],
            "Company": ["ClientA", None, "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        source_df = pd.DataFrame(source_dict, index=[0, 1, 4])
        dask_df: Any = dd.from_pandas(source_df, chunksize=3)
        dask_df = drop_nans(dask_df)
        has_nan: bool = look_for_nan(dask_df)
        self.assertEqual(has_nan, False)
        has_none: bool = look_for_nan(dask_df)
        self.assertEqual(has_none, False)
        reference_dict: Dict[str, Union[List[int], List[str]]] = {
            "DealId": [1],
            "Company": ["ClientA"],
            "Peril": ["Earthquake"],
            "Location": ["USA"]
        }
        reference_df: Any = dd.from_pandas(pd.DataFrame(reference_dict, index=[0]), chunksize=1)
        self.assertEqual(str(dask_df.astype({"DealId": "int"}).compute()), str(reference_df.compute()))


if __name__ == "__main__":
    unittest.main(argv=[""], verbosity=2, exit=False)
