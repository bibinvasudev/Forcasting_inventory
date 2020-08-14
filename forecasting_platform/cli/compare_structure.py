import sys
from pathlib import Path
from typing import List

import click
from forecasting_platform.helpers import (
    absolute_path,
    collect_files_with_extension,
    compare_csv_structure,
)


@click.command(hidden=True)
@click.argument("actual", nargs=1, type=click.Path(exists=True))
@click.option(
    "--suffix",
    "-s",
    "suffixes",
    type=str,
    default=["result_data.csv"],
    multiple=True,
    show_default=True,
    help="Suffix to filter relevant files. Can be provided multiple times.",
)
def compare_structure(actual: str, suffixes: List[str]) -> None:
    """Validate CSV files with default development forecasts.

    If ``actual`` is a directory, matching files are searched recursively.

    Args:
        actual: Actual results file or directory.
        suffixes: File suffixes to compare inside the directories.

    """
    actual_path = absolute_path(actual)
    actual_files = []
    for suffix in suffixes:
        actual_files += collect_files_with_extension(actual_path, suffix)

    if not actual_files:
        click.echo(click.style(f"No matching files found in {actual_path}", fg="red"))
        sys.exit(1)

    click.echo("Validating file structure against development forecast...")
    results_ok = [_compare_single_file(file) for file in actual_files]
    if not all(results_ok):
        click.echo(click.style("Some files fail the structure check.", fg="red"))
        sys.exit(1)

    click.echo(click.style("All files have a valid structure.", fg="green"))


def _compare_single_file(file: Path) -> bool:
    click.echo(f"Validating {file}")

    try:
        compare_csv_structure(file)
        click.echo(click.style("File with valid structure.", fg="green"))
        return True
    except AssertionError as e:
        click.echo(click.style("Invalid structure", fg="red"), err=True)
        click.echo(click.style(str(e), fg="red"), err=True)
        return False
