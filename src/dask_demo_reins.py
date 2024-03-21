"""
File:       dask_demo_reins.py
Author:     Ivan Lazarević
Brief:      Dask demo main file.
"""
# Standard library imports
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, cast, List, Union, ValuesView

# Third party library imports
import dask.dataframe as dd  # type: ignore
import numexpr  # type: ignore

# Local modules imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), r".."))
from src.config import CONTRACT, DEALS, LOSSES, PROCESS_NAN  # noqa
from src.type_aliases import Contract, Coverage  # noqa
from src.utils import drop_nans, exit_program, look_for_nan  # noqa


def read_json_contract(json_file_path: Path = CONTRACT) -> Contract:
    """Reads a file with contract in JSON format and returns the contents (as dictionary)."""
    with open(json_file_path, "rt") as contract_file:
        contract: Contract = json.load(contract_file)
    return contract


def filter_deals(contract: Contract, deals_file_path: Path = DEALS) -> Any:
    """
    Filters a *"deals"* CSV file by using a contract, in a fast and memory-efficient way.

    Returns a Dask `DataFrame` query object as a task that can be computed.
    """
    coverage: Coverage = cast(Coverage, contract["Coverage"])
    location_include: List[str] = []
    peril_exclude: List[str] = []

    # Contract (from a json file) is small, so looping over it (only once) is not a problem.
    for attribute in coverage:
        attribute_values: ValuesView[Union[str, List[str]]] = attribute.values()
        if "Location" in attribute_values and "Include" in attribute:
            location_include = cast(List[str], attribute["Include"])
        elif "Peril" in attribute_values and "Exclude" in attribute:
            peril_exclude = cast(List[str], attribute["Exclude"])
    if not location_include or not peril_exclude:
        exit_program("Location(s) to include or Peril(s) to exclude missing from contract.")

    deals_df = dd.read_csv(deals_file_path).set_index("DealId")

    # https://docs.dask.org/en/latest/dataframe-joins.html#large-to-small-joins
    deals_df = deals_df.repartition(npartitions=1)

    # `df.query()` is fast and memory-efficient.
    covered_deals: Any = deals_df.query(f"({'Location'} in {location_include}) and ({'Peril'} not in {peril_exclude})")
    return covered_deals


def get_max_amount(contract: Contract, contract_path: Path) -> int:
    """Tries to fetch maximum amount on any one event from a contract."""
    max_amount: int = 0
    try:
        max_amount = cast(int, contract["MaxAmount"])
    except KeyError:
        exit_program(f"Maximum amount missing from contract '{contract_path}'.")
    if max_amount < 0:
        exit_program(f"Maximum amount in the contract '{contract_path}' is negative: {max_amount}.")
    return max_amount


def model_losses(covered_deals: Any, max_amount: int, losses_file_name: Path = LOSSES) -> Any:
    """
    Simulates expected losses to model the risk, in a fast and memory-efficient way.\n
    Filters given deals by using a CSV file with losses events.

    Returns a Dask DataFrame query object as a task that can be computed.
    """
    losses_df = dd.read_csv(losses_file_name).set_index("DealId")

    # Limit any one event (loss) to maximum amount from contract.
    greater_than: Any = losses_df["Loss"] > max_amount  # dask.dataframe.core.Series of bool
    losses_df["Loss"] = losses_df["Loss"].mask(greater_than, max_amount)
    result = losses_df.merge(covered_deals, how="left", on=["DealId"])
    if PROCESS_NAN:
        result = drop_nans(result)
    result = result.groupby(["Peril"]).sum()
    try:
        result = result.drop(["EventId"], axis="columns", errors="raise")
    except KeyError as err:
        exit_program(str(err))

    return result


def main() -> None:
    """The program entry point"""
    parser = argparse.ArgumentParser(description="A Dask Demo Project")
    parser.add_argument("-p", "--path", type=str, help="path to a contract JSON file")
    args = parser.parse_args()
    print(args.path)

    # Pass "data/contract.json" in CLI --> Execute as (the double-quotes are not necessary; remove back-ticks):
    # `python src/dask_demo_reins.py -p "data/contract.json"` or
    # `python src/dask_demo_reins.py --path "data/contract.json"`.
    # Alternatively, don't use the "-p"/"--path" option and don't pass anything.
    contract_path: Path = Path(args.path) if args.path is not None else CONTRACT
    contract: Contract = read_json_contract(contract_path)
    covered_deals: Any = filter_deals(contract, DEALS)
    print(covered_deals.compute())
    if PROCESS_NAN:
        has_nan: bool = look_for_nan(covered_deals)
        if has_nan:
            exit_program("Covered deals data contain at least one NaN.")
    print()

    max_amount: int = get_max_amount(contract, contract_path)
    modeled_losses: Any = model_losses(covered_deals, max_amount, LOSSES)
    print(modeled_losses.compute())
    if PROCESS_NAN:
        has_nan = look_for_nan(modeled_losses)
        if has_nan:
            exit_program("Modeled losses data contain at least one NaN.")


if __name__ == "__main__":
    # From: https://docs.dask.org/en/latest/generated/dask.dataframe.DataFrame.query.html
    # So, it's best to set "numexpr" to use one thread, and this setting only applies to "numexpr", not to "Dask".
    numexpr.set_num_threads(1)

    main()
