.. _Usage:

Platform Usage
==============

To run the platform, you first specify the command that should be executed. This can
be a forward looking run for testing or production or a backward looking run. By
default, the platform will only output some basic environment status. Depending on the
type of run you choose, there are different additional options/parameters you can
specify.

.. warning::
    Running multiple ``forecasting_platform`` commands concurrently is strongly
    discouraged and not supported by the platform.

    Starting a new forecast might cause a still running forecast command to fail,
    when the cleaned data in the internal database is replaced.

    Please always either wait for command completion, cancel unwanted runs via
    ``CTRL-C`` on the CLI, or stop all Python processes via the Task Manager.

.. click:: forecasting_platform.__main__:cli
  :prog: forecasting_platform

.. click:: forecasting_platform.cli:production
  :prog: forecasting_platform production

.. click:: forecasting_platform.cli:development
  :prog: forecasting_platform development

.. click:: forecasting_platform.cli:backward
  :prog: forecasting_platform backward

.. click:: forecasting_platform.cli:setup_database
  :prog: forecasting_platform setup-database
  :show-nested:
