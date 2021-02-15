from pathlib import Path
from typing import Dict, List, Optional, Union

import click
import pandas as pd

from geo_utils import do_concurrent, smartystreets_utils


@click.group()
def cli():
    """
    Command line tools for geo_utils
    """


@cli.command("smarty")
@click.argument("input_directory", type=click.Path(exists=False))
@click.argument("output_directory", type=click.Path(exists=False))
def smartystreets_geocode(
    input_directory: str,
    output_directory: str,
    smartystreets_auth_id: str,
    smartystreets_auth_token: str,
    max_workers: int = None,
):
    """
    Run SmartyStreets geocoding services with an input directory and output directory
    Args:
        input_directory (str): directory of input file
        output_directory (str): directory of output file
        smartystreets_auth_id (str):
        smartystreets_auth_token (str):
        max_workers (int): Number of workers for concurrent.futures. Defaults to None.
        (Note: None is the default for the concurrent package. Refer to

    Returns:

    """
    # Read from input_directory
    df = pd.read_csv(input_directory)
    # Group df by zipcode
    df_list = do_concurrent.df_group_by_zip(df)
    # Apply concurrent processes on the list of dfs
    output = do_concurrent.smarty_process_pool(
        df_list,
        smartystreets_auth_id,
        smartystreets_auth_token,
        max_workers=max_workers,
    )
    #
    output.to_csv(output_directory)


if __name__ == "__main__":
    cli()
