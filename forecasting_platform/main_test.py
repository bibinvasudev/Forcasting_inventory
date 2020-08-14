import re
import subprocess

from click.testing import CliRunner

from .__main__ import cli


def test_main_can_be_executed() -> None:
    """Ensure that there are no issues when running __main__ as its own process (e.g. circular imports)."""
    result = subprocess.run(["python", "-m", "forecasting_platform", "--help"])
    assert result.returncode == 0


class TestCli:
    def test_main(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(cli)
        assert result.exit_code == 0
        assert re.search("H2O cluster uptime", result.output)

    def test_info(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(cli, ["info"])
        assert result.exit_code == 0
        assert re.search("H2O cluster uptime", result.output)

    def test_help(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(cli, ["-h"])
        assert result.exit_code == 0
        assert re.search("Show this message and exit", result.output)

    def test_unknown_command(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(cli, ["unknown-command"])
        assert result.exit_code == 2
        assert re.search("No such command 'unknown-command'", result.output)
