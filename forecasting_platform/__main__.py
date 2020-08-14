import click

from .cli import (
    backward,
    compare_results,
    compare_structure,
    compare_structure_database_command,
    development,
    info,
    production,
    setup_database,
)


@click.group(invoke_without_command=True, context_settings=dict(help_option_names=["-h", "--help"]))
# The click group is setup to allow overriding `prog_name`
@click.pass_context
def cli(ctx: click.Context) -> None:  # noqa: D103  # Using a docstring would appear in the "--help" output.
    # Handle the case when the program is called without any parameters or commands.
    if ctx.invoked_subcommand is None:
        ctx.forward(info)


cli.add_command(backward)
cli.add_command(compare_results)
cli.add_command(compare_structure)
cli.add_command(compare_structure_database_command)
cli.add_command(production)
cli.add_command(development)
cli.add_command(info)
cli.add_command(setup_database)

if __name__ == "__main__":
    cli(prog_name="forecasting_platform")
