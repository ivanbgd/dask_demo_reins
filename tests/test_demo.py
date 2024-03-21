"""
File:       test_demo.py
Author:     Ivan Lazarević
Brief:      Automated unit tests for the Dask demo main file.
"""
# Standard library imports
import unittest
from typing import Any, cast

# Third party library imports
import dask.dataframe as dd  # type: ignore
import pandas as pd  # type: ignore

# Local modules imports
from src.config import LOSSES_TEST_LARGE, LOSSES_TEST_NAN
from src.dask_demo_reins import filter_deals, model_losses, read_json_contract
from src.type_aliases import Contract, Deal


class TestDemo(unittest.TestCase):
    """Class for automated testing of Dask Demo"""

    contract: Contract = {}
    covered_deals: Any = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        """Called once before all test cases."""
        contract: Contract = read_json_contract()
        cls.contract = contract

    def test_read_json_contract(self) -> None:
        """Assumes the original "contract.json" for comparison."""
        contract_reference: Contract = {
            "Coverage": [
                {
                    "Attribute": "Location",
                    "Include": [
                        "USA", "Canada"
                    ]
                },
                {
                    "Attribute": "Peril",
                    "Exclude": [
                        "Tornado"
                    ]
                }
            ],
            "MaxAmount": 3000
        }
        contract_from_file: Contract = read_json_contract()
        self.assertNotEqual(contract_from_file, {})
        self.assertDictEqual(contract_from_file, contract_reference)

    def test_filter_deals(self) -> None:
        """
        Tests filtering of a deal by using a contract.\n
        Assumes the original "deals.csv" and "contract.json".\n
        """
        source_dict: Deal = {
            "DealId": [1, 2, 5],
            "Company": ["ClientA", "ClientA", "ClientC"],
            "Peril": ["Earthquake", "Hailstone", "Hurricane"],
            "Location": ["USA", "Canada", "USA"]
        }
        source_df = pd.DataFrame(source_dict, index=[0, 1, 4])
        reference_output: Any = dd.from_pandas(source_df, chunksize=3).set_index("DealId")
        covered_deals: Any = filter_deals(TestDemo.contract)
        TestDemo.covered_deals = covered_deals.persist()
        self.assertEqual(str(reference_output.compute()), str(TestDemo.covered_deals.compute()))

    def test_get_max_amount(self) -> None:
        """Assumes the original "contract.json" for comparison."""
        self.assertEqual(TestDemo.contract["MaxAmount"], 3000)

    def test_model_losses(self) -> None:
        """Assumes the original "losses.csv" for comparison."""
        source_dict: Deal = {
            "Peril": ["Earthquake", "Hurricane"],
            "Loss": [3500, 3000]
        }
        source_df = pd.DataFrame(source_dict, index=[0, 1])
        source_df.set_index("Peril", inplace=True)
        reference_output: Any = dd.from_pandas(source_df, chunksize=2)
        modeled_losses: Any = model_losses(TestDemo.covered_deals, cast(int, TestDemo.contract["MaxAmount"]))
        reference_output = reference_output.rename(columns={"Loss": ""})
        modeled_losses = modeled_losses.rename(columns={"Loss": ""})
        self.assertEqual(str(reference_output.compute()), str(modeled_losses.compute()))

    def test_model_losses_large_values(self) -> None:
        """Uses "losses_test_large.csv" for comparison; contains large values, higher than maximum amount."""
        source_dict: Deal = {
            "Peril": ["Earthquake", "Hurricane"],
            "Loss": [6000, 3000]
        }
        source_df = pd.DataFrame(source_dict, index=[0, 1])
        source_df.set_index("Peril", inplace=True)
        reference_output: Any = dd.from_pandas(source_df, chunksize=2)
        modeled_losses: Any = model_losses(
            TestDemo.covered_deals, cast(int, TestDemo.contract["MaxAmount"]), LOSSES_TEST_LARGE
        )
        self.assertEqual(str(reference_output.compute()), str(modeled_losses.compute()))

    def test_model_losses_nan(self) -> None:
        """Uses "losses_test_nan.csv" for comparison; contains NaNs."""
        source_dict: Deal = {
            "Peril": ["Earthquake"],
            "Loss": [1500]
        }
        source_df = pd.DataFrame(source_dict, index=[0])
        source_df.set_index("Peril", inplace=True)
        reference_output: Any = dd.from_pandas(source_df, chunksize=1)
        modeled_losses: Any = model_losses(
            TestDemo.covered_deals, cast(int, TestDemo.contract["MaxAmount"]), LOSSES_TEST_NAN
        )
        self.assertEqual(str(reference_output.compute()), str(modeled_losses.astype({"Loss": "int"}).compute()))


if __name__ == "__main__":
    unittest.main(argv=[""], verbosity=2, exit=False)
