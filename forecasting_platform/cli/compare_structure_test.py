import re
from pathlib import Path

import numpy as np
import pandas as pd
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from forecasting_platform.helpers import compare_results
from forecasting_platform.static import DataFrameStructure

from .compare_structure import compare_structure


class TestCliCompareStructure:
    @staticmethod
    def _setup_tmp_csv(tmp_path: Path, monkeypatch: MonkeyPatch) -> Path:
        expected_structure = DataFrameStructure(
            columns=pd.Index(["0"], dtype="object"), dtypes=pd.Series([np.dtype("object")]), shape=(0, 1),
        )
        monkeypatch.setattr(compare_results, "get_expected_forecast_structure", lambda _: expected_structure)
        result_dir = tmp_path / "Forecast Account_0" / "202002" / "[TIMESTAMP]_202002_T9_P0_Contract_ID_Item_ID"
        result_dir.mkdir(parents=True)
        return result_dir / "actual_post.csv"

    def test_compare_structure_equal(self, cli_runner: CliRunner, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        actual_results_csv = self._setup_tmp_csv(tmp_path, monkeypatch)
        actual_results_csv.write_text("0")

        result = cli_runner.invoke(compare_structure, [str(actual_results_csv), "--suffix", "csv"])

        assert result.exit_code == 0
        assert re.search(r"File with valid structure", result.output, re.MULTILINE)

    def test_compare_structure_directory(self, cli_runner: CliRunner, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        self._setup_tmp_csv(tmp_path, monkeypatch).write_text("0")

        result = cli_runner.invoke(compare_structure, [str(tmp_path), "--suffix", "csv"])

        assert result.exit_code == 0
        assert re.search(r"File with valid structure", result.output, re.MULTILINE)

    def test_compare_structure_different(self, cli_runner: CliRunner, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        actual_results_csv = self._setup_tmp_csv(tmp_path, monkeypatch)
        actual_results_csv.write_text("1,2,3")

        result = cli_runner.invoke(compare_structure, [str(actual_results_csv), "--suffix", "csv"])

        assert result.exit_code == 1
        assert re.search(r"Invalid structure", result.output, re.MULTILINE)

    def test_compare_structure_invalid_file_path(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        actual_results_csv = tmp_path / "actual.csv"
        actual_results_csv.write_text("0")

        result = cli_runner.invoke(compare_structure, [str(actual_results_csv), "--suffix", "csv"])

        assert result.exit_code == 1
        assert re.search("Could not parse forecast parameters from file path:", result.output, re.MULTILINE)

    def test_compare_structure_missing_arguments(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(compare_structure)

        assert result.exit_code == 2
        assert re.search(r"Error: Missing argument", result.output, re.MULTILINE)

    def test_compare_structure_invalid_path(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(compare_structure, ["this-does-not-exist"])

        assert result.exit_code == 2
        assert re.search(r"Path 'this-does-not-exist' does not exist.", result.output, re.MULTILINE)
