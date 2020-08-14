import json
import re
from pathlib import Path
from typing import (
    Any,
    Optional,
)
from unittest.mock import Mock

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from click.testing import (
    CliRunner,
    Result,
)
from forecasting_platform import master_config
from forecasting_platform.__main__ import cli
from forecasting_platform.master_config import default_output_location  # Import be it is mocked by pytest fixture
from forecasting_platform.model_config_scripts import ModelConfigAccount10
from forecasting_platform.services import (
    Orchestrator,
    RuntimeConfig,
)
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    EngineRunType,
)
from forecasting_platform.test_utils import insert_cleaned_data_for_database_test


def run_command(cli_runner: CliRunner, engine_run_type: EngineRunType, *args: str) -> Result:
    return cli_runner.invoke(cli, args=(engine_run_type.value, *args))


@pytest.mark.parametrize(
    "engine_run_type", [EngineRunType.backward, EngineRunType.development, EngineRunType.production]
)
class TestCliEngineRunType:
    def test_engine_run_type_command(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch
    ) -> None:
        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type)

        mocked_run.assert_called_once()
        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    def test_forecast_periods_option(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        optional_forecast_periods = 1
        expected_test_periods = optional_forecast_periods if engine_run_type == EngineRunType.backward else 0
        expected_predict_periods = optional_forecast_periods if engine_run_type != EngineRunType.backward else 0

        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type, "--forecast-periods", str(optional_forecast_periods))

        mocked_run.assert_called_once()
        assert f"Runtime config test_periods = {expected_test_periods}" in caplog.messages
        assert f"Runtime config predict_periods = {expected_predict_periods}" in caplog.messages
        assert f"Runtime config output_path = {default_output_location}" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    @pytest.mark.parametrize("invalid_forecast_periods", [0, 51, 1.23, "this_is_not_a_valid_integer"])  # type: ignore
    def test_forecast_periods_invalid_option(
        self, engine_run_type: EngineRunType, invalid_forecast_periods: Any, cli_runner: CliRunner
    ) -> None:
        result = run_command(cli_runner, engine_run_type, "--forecast-periods", str(invalid_forecast_periods))

        assert result.exit_code == 2
        assert re.search(r"Error: Invalid value for '--forecast-periods':", result.output, re.MULTILINE,)

    def test_forecast_location_option(
        self,
        engine_run_type: EngineRunType,
        cli_runner: CliRunner,
        monkeypatch: MonkeyPatch,
        caplog: LogCaptureFixture,
        tmp_path: Path,
    ) -> None:
        optional_output_location = tmp_path
        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type, "--output-location", str(optional_output_location))

        mocked_run.assert_called_once()

        assert f"Runtime config output_path = {optional_output_location}" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    @pytest.mark.parametrize("output_format", ["csv", "xlsx"])  # type: ignore
    def test_output_format(
        self,
        engine_run_type: EngineRunType,
        cli_runner: CliRunner,
        monkeypatch: MonkeyPatch,
        caplog: LogCaptureFixture,
        output_format: str,
    ) -> None:
        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type, "--output-format", output_format)

        mocked_run.assert_called_once()
        assert f"Runtime config output_format = OutputFormat.{output_format}" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    def test_prediction_month_option(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(
            cli_runner,
            engine_run_type,
            "--prediction-end-month" if engine_run_type is EngineRunType.backward else "--prediction-start-month",
            "200001",
        )

        mocked_run.assert_called_once()
        assert "Runtime config prediction_month = 200001" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    @pytest.mark.parametrize(
        "prediction_month", ["199912", "210001", "this_is_not_a_valid_time_format"]
    )  # type: ignore
    def test_prediction_month_invalid_option(
        self, engine_run_type: EngineRunType, prediction_month: Any, cli_runner: CliRunner
    ) -> None:
        start_month_option = (
            "--prediction-end-month" if engine_run_type is EngineRunType.backward else "--prediction-start-month"
        )
        result = run_command(cli_runner, engine_run_type, start_month_option, str(prediction_month),)

        assert result.exit_code == 2
        assert re.search(f"Error: Invalid value for '{start_month_option}':", result.output, re.MULTILINE,)

    def test_prediction_start_month_future_date(self, engine_run_type: EngineRunType, cli_runner: CliRunner) -> None:
        start_month_option = (
            "--prediction-end-month" if engine_run_type is EngineRunType.backward else "--prediction-start-month"
        )
        prediction_month = (
            pd.Timestamp.today().replace(day=1)
            + pd.DateOffset(months=(1 if engine_run_type == EngineRunType.backward else 2))
        ).strftime(PREDICTION_MONTH_FORMAT)
        result = run_command(cli_runner, engine_run_type, start_month_option, str(prediction_month),)

        assert result.exit_code == 2
        assert re.search(f"Error: Invalid value for '{start_month_option}':", result.output, re.MULTILINE,)

    def test_forecast_options_default_value(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        expected_test_periods = (
            master_config.default_forecast_periods if engine_run_type == EngineRunType.backward else 0
        )
        expected_predict_periods = (
            master_config.default_forecast_periods if engine_run_type != EngineRunType.backward else 0
        )
        expected_prediction_start_month = pd.Timestamp.today().strftime(PREDICTION_MONTH_FORMAT)

        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)
        result = run_command(cli_runner, engine_run_type)

        mocked_run.assert_called_once()

        assert f"Runtime config test_periods = {expected_test_periods}" in caplog.messages
        assert f"Runtime config predict_periods = {expected_predict_periods}" in caplog.messages
        assert f"Runtime config output_path = {default_output_location}" in caplog.messages
        assert f"Runtime config prediction_month = {expected_prediction_start_month}" in caplog.messages
        assert "Runtime config output_format = OutputFormat.csv" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    def test_forecast_combined_options(
        self,
        engine_run_type: EngineRunType,
        cli_runner: CliRunner,
        monkeypatch: MonkeyPatch,
        caplog: LogCaptureFixture,
        tmp_path: Path,
    ) -> None:
        optional_forecast_periods = 1
        expected_test_periods = optional_forecast_periods if engine_run_type == EngineRunType.backward else 0
        expected_predict_periods = optional_forecast_periods if engine_run_type != EngineRunType.backward else 0

        optional_output_location = tmp_path
        optional_output_format = "xlsx"
        mocked_run = Mock()
        monkeypatch.setattr(master_config, "model_configs", [])
        monkeypatch.setattr(Orchestrator, "run", mocked_run)
        result = run_command(
            cli_runner,
            engine_run_type,
            "--output-location",
            str(optional_output_location),
            "--forecast-periods",
            str(optional_forecast_periods),
            "--output-format",
            str(optional_output_format),
        )
        mocked_run.assert_called_once()

        assert f"Runtime config test_periods = {expected_test_periods}" in caplog.messages
        assert f"Runtime config predict_periods = {expected_predict_periods}" in caplog.messages
        assert f"Runtime config output_path = {optional_output_location}" in caplog.messages
        assert f"Runtime config output_format = OutputFormat.{optional_output_format}" in caplog.messages

        assert result.exit_code == 0
        assert re.search(r"Successfully ran forecast for 0 account\(s\)", result.output, re.MULTILINE)

    def test_output_location_non_existing_directory(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner
    ) -> None:
        result = run_command(cli_runner, engine_run_type, "--output-location", "non-existing-directory")
        assert result.exit_code == 2
        assert re.search(
            "Invalid value for '--output-location': Path 'non-existing-directory' does not exist.", result.output
        )

    def test_output_format_invalid_option(self, engine_run_type: EngineRunType, cli_runner: CliRunner) -> None:
        result = run_command(cli_runner, engine_run_type, "--output-format", "foo")
        assert result.exit_code == 2
        assert re.match("(.|\n)*Invalid value for '--output-format': Value must be 'csv' or 'xlsx'.", result.output)

    def test_only_model_config(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        mocked_run = Mock()
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type, "--only-model-config", "ModelConfigAccount1")

        mocked_run.assert_called_once()

        assert result.exit_code == 0
        assert "Runtime config model_configs = ['ModelConfigAccount1']" in caplog.messages
        assert re.search(r"Successfully ran forecast for 1 account\(s\)", result.output, re.MULTILINE)

    def test_only_model_config_invalid(self, engine_run_type: EngineRunType, cli_runner: CliRunner) -> None:
        result = run_command(cli_runner, engine_run_type, "--only-model-config", "ModelConfigAccount")
        assert result.exit_code == 2
        assert re.search("Invalid value for '--only-model-config': Invalid model config name", result.output)

    def test_exclude_model_config(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        mocked_run = Mock()
        monkeypatch.setattr(Orchestrator, "run", mocked_run)

        result = run_command(cli_runner, engine_run_type, "--exclude-model-config", "ModelConfigAccount1")

        mocked_run.assert_called_once()

        assert result.exit_code == 0

        expected_configs = master_config.model_configs.copy()
        expected_configs.remove("ModelConfigAccount1")

        expected_configs_str = ", ".join(f"'{c}'" for c in expected_configs)
        assert f"Runtime config model_configs = [{expected_configs_str}]" in caplog.messages

        assert re.search(
            rf"Successfully ran forecast for {len(expected_configs)} account\(s\)", result.output, re.MULTILINE
        )

    def test_exclude_model_config_invalid(self, engine_run_type: EngineRunType, cli_runner: CliRunner) -> None:
        result = run_command(cli_runner, engine_run_type, "--exclude-model-config", "ModelConfigAccount")
        assert result.exit_code == 2
        assert re.search("Invalid value for '--exclude-model-config': Invalid model config name", result.output)

    @pytest.mark.testlogging  # type: ignore
    def test_model_config_options_mutually_exclusive(
        self, engine_run_type: EngineRunType, cli_runner: CliRunner
    ) -> None:
        result = run_command(
            cli_runner,
            engine_run_type,
            "--exclude-model-config",
            "ModelConfigAccount1",
            "--only-model-config",
            "ModelConfigAccount2",
        )
        assert result.exit_code == 1
        assert re.search("--only-model-config and --exclude-model-config cannot be used together", result.output)


@pytest.mark.account  # type: ignore
def test_optimize_hyperparameters(cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture) -> None:
    test_dummy_cleaning_id = 123456

    monkeypatch.setattr(ModelConfigAccount10, "OPTIMIZE_HYPER_PARAMETERS_N_CALLS", 1)
    monkeypatch.setattr(
        ModelConfigAccount10,
        "_OPTIMIZE_BAYES_GP_ARGS",
        {"n_random_starts": 1, "n_restarts_optimizer": 1, "n_points": 1},  # Try to make the test run a bit faster
    )
    monkeypatch.setattr(RuntimeConfig, "includes_cleaning", False)
    monkeypatch.setattr(Orchestrator, "_determine_cleaned_data_run_id", lambda _: test_dummy_cleaning_id)

    with insert_cleaned_data_for_database_test(ModelConfigAccount10, test_dummy_cleaning_id):
        result = run_command(
            cli_runner,
            EngineRunType.backward,
            "--only-model-config",
            "ModelConfigAccount10",
            "--prediction-end-month",
            "202003",
            "--forecast-periods",
            "1",
            "--optimize-hyperparameters",
        )

    def log_exists(prefix: str) -> Optional[str]:
        matches = (message for message in caplog.messages if message.startswith(prefix))
        return next(matches, None)

    assert log_exists("Starting hyper-parameter optimization for Account_10")
    assert log_exists("Finished hyper-parameter optimization for Account_10")

    assert (output_path_log := log_exists("Forecasting Account_10 to"))
    assert (path_match := re.search('Forecasting Account_10 to "(.+)"', output_path_log))
    directory_path = Path(path_match[1])
    json_path = directory_path / "optimized_hyperparameters.json"

    params_json = json.loads(json_path.read_text())
    params_json_floats = {key: value for key, value in params_json.items() if isinstance(value, float)}
    params_json_other = {key: value for key, value in params_json.items() if not isinstance(value, float)}

    assert params_json_floats == pytest.approx(
        {
            "col_sample_rate": 0.992726562,
            "learn_rate": 0.0238266504,
            "learn_rate_annealing": 0.985708767,
            "min_split_improvement": 0.000812168,
            "sample_rate": 0.151041679,
            "stopping_tolerance": 0.003905378,
        }
    )

    assert params_json_other == {
        "max_depth": 20,
        "min_rows": 23,
        "nbins": 291,
        "nfolds": 10,
        "ntrees": 144,
        "seed": 1234,
        "stopping_metric": "MAE",
        "stopping_rounds": 4,
    }

    assert result.exit_code == 0
    assert re.search(r"Successfully ran forecast for 1 account\(s\)", result.output, re.MULTILINE)
