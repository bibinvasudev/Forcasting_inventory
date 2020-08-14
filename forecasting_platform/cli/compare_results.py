import re
import sys
from pathlib import Path
from typing import (
    List,
    Tuple,
)

import click
from forecasting_platform.helpers import (
    absolute_path,
    collect_files_with_extension,
    compare_csv,
)


@click.command(hidden=True)
@click.argument("expected", nargs=1, type=click.Path(exists=True), default="expected_results")
@click.argument("actual", nargs=1, type=click.Path(exists=True), default="08 Predictions")
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
def compare_results(expected: str, actual: str, suffixes: List[str]) -> None:
    """Compare CSV files with forecast results.

    If ``expected`` or ``actual`` are directories, matching files are searched recursively in these directories.

    Args:
        expected: Expected results file or directory.
        actual: Actual results file or directory.
        suffixes: File suffixes to compare inside the directories.

    """
    expected_path = absolute_path(expected)
    actual_path = absolute_path(actual)

    checks = []

    for suffix in suffixes:
        expected_files = collect_files_with_extension(expected_path, suffix)
        actual_files = collect_files_with_extension(actual_path, suffix)

        checks += _generate_comparisons(expected_files, actual_files)

    if not checks:
        click.echo(click.style(f"No comparisons found between {expected_path} and {actual_path}", fg="red"))
        sys.exit(1)

    click.echo("Comparing files...")
    results_ok = [_compare_single_result(expected, actual) for expected, actual in checks]
    if not all(results_ok):
        click.echo(click.style("Some files are not equal.", fg="red"))
        sys.exit(1)

    click.echo(click.style("All files are equal.", fg="green"))


def _generate_comparisons(expected_files: List[Path], actual_files: List[Path]) -> List[Tuple[Path, Path]]:
    """List all valid combinations of expected and actual files.

    Args:
        expected_files: List of expected result file paths.
        actual_files: List of actual result file paths.

    Returns:
        Tuple of matching actual and result file pairs.
    """
    combinations: List[Tuple[Path, Path]] = []
    for expected in expected_files:
        match = re.search(r"Account_\d+", str(expected))
        if not match:
            continue
        account = match[0]
        for actual in actual_files:
            delimiter = r"[_/\\\s?]"
            if re.search(delimiter + account + delimiter, str(actual)):
                combinations.append((expected, actual))

    return combinations


def _compare_single_result(expected_path: Path, actual_path: Path) -> bool:
    try:
        click.echo(f"{'Expected'.ljust(8)}:{expected_path}")
        click.echo(f"{'Actual'.ljust(8)}:{actual_path}")

        compare_csv(expected_path, actual_path)
        click.echo(click.style("Files are equal.", fg="green"))
        return True
    except AssertionError as e:
        click.echo(click.style(f"Files are not equal.\n{e}", fg="red"), err=True)
        return False
