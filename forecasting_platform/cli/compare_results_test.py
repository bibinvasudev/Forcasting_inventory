import re
from pathlib import Path
from typing import Tuple

from click.testing import CliRunner
from forecasting_platform.helpers.identifier import AccountID

from .compare_results import compare_results


class TestCliCompareResults:
    @staticmethod
    def _setup_test_files(tmp_path: Path) -> Tuple[Path, Path]:
        expected_results_csv = tmp_path / f"Forecast {AccountID('Account 10')}" / "expected.csv"
        expected_results_csv.parent.mkdir(exist_ok=True)

        actual_results_csv = tmp_path / AccountID("Account 10") / "actual.csv"
        actual_results_csv.parent.mkdir(exist_ok=True)

        return expected_results_csv, actual_results_csv

    def test_compare_results_equal(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        expected_results_csv, actual_results_csv = self._setup_test_files(tmp_path)

        expected_results_csv.write_text("0")
        actual_results_csv.write_text("0")

        result = cli_runner.invoke(compare_results, [str(expected_results_csv), str(actual_results_csv), "-s", "csv"])

        assert result.exit_code == 0
        assert re.search(r"Files are equal", result.output, re.MULTILINE)

    def test_compare_results_equal_directory(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        expected_results_csv, actual_results_csv = self._setup_test_files(tmp_path)

        expected_results_csv.write_text("0")
        actual_results_csv.write_text("0")

        result = cli_runner.invoke(
            compare_results, [str(expected_results_csv.parent), str(actual_results_csv.parent), "-s", "csv"]
        )

        assert result.exit_code == 0
        assert re.search(r"Files are equal", result.output, re.MULTILINE)

    def test_compare_results_different(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        expected_results_csv, actual_results_csv = self._setup_test_files(tmp_path)

        expected_results_csv.write_text("0")
        actual_results_csv.write_text("1")

        result = cli_runner.invoke(compare_results, [str(expected_results_csv), str(actual_results_csv), "-s", "csv"])

        assert result.exit_code == 1
        assert re.search(r"Files are not equal", result.output, re.MULTILINE)

    def test_compare_results_no_matching_files(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(compare_results, ["this-does-not-exist", "this-is-also-not-here"])

        assert result.exit_code == 2
        assert re.search(r"Path 'this-does-not-exist' does not exist", result.output, re.MULTILINE)

    def test_compare_results_no_matching_suffix(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        expected_results_csv, actual_results_csv = self._setup_test_files(tmp_path)

        result = cli_runner.invoke(
            compare_results, [str(expected_results_csv.parent), str(actual_results_csv.parent), "-s", "invalid"]
        )

        assert result.exit_code == 1
        assert re.search(r"No comparisons found between", result.output, re.MULTILINE)
