import click
from pathlib import Path
from typing import Dict, List, Optional, Union


@click.group()
def cli():
    """
    Command line tools for geo_utils
    """

if __name__ == "__main__":
    cli()
