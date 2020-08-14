Platform Setup
===============

This document explains the process to set up the forecasting platform locally for Windows, Linux and MacOS.

The following aspects of the installation are covered:

   - :ref:`Setup Python <setup_python_windows>`

      - :ref:`Windows <setup_python_windows>`
      - :ref:`Linux / MacOS <setup_python_Linux_MacOS>`
   - :ref:`Setup Poetry (all platforms) <setup_poetry>`
   - :ref:`Setup pre-commit hooks (all platforms) <setup_precommit_hooks>`
   - :ref:`Setup Docker (Linux / MacOS) <setup_docker>`
   - :ref:`Setup ODBC connection <setup_ODBC_windows>`

      - :ref:`Windows <setup_ODBC_windows>`
      - :ref:`Linux / MacOS <setup_ODBC_Linux_MacOS>`
   - :ref:`Setup H2O <setup_h2o>`
   - :ref:`Test & Run the platform <tests_run>`

.. _setup_python_windows:

Setup Python (Windows)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install `Miniconda <https://docs.conda.io/en/latest/miniconda.html>`_ in the Python 3.8 version.

To be able to run the shell scripts provided by this repository, we recommend
using `Git Bash <https://gitforwindows.org/>`_ to execute all commands. To make Git Bash
work with your miniconda installation, first check that the file `.bash_profile` exists.
You should be able to find it in your user directory.
For this, type the following into Git Bash:

.. code:: bash

   ~/.bash_profile

If it does not exist, create the `.bash_profile` file,
which you can do with the following command in Git Bash:

.. code:: bash

   touch .bash_profile

Once the `.bash_profile` file exists in your user directory, add to it the following line,
replacing ``<folder to miniconda install>`` with your installation path of miniconda.

.. code:: bash

   # Open the .bash_profile file
   notepad .bash_profile

   # Write to this file
   source <folder to miniconda install>/Scripts/activate

If the Miniconda3 installation was not done for all users, but rather only for your user,
this command might be the path to your Miniconda3 installation:

.. code:: bash

   source ~/Miniconda3/Scripts/activate


The next step is to create a conda environment. For this, open a new instance of Git Bash and
execute the following commands to create a new Python environment for development. You can
choose any name you want for ``<env>``.

.. code:: bash

   conda create --name <env> python=3.8.1

Finally, activate this new environment using the following command. You have to repeat this
every time you open a new Git Bash terminal.

.. code:: bash

   conda activate <env>

.. _setup_poetry:

Setup Poetry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two key steps in this process:

- Install Poetry: Follow the `windows powershell installation instructions <https://python-poetry.org/docs/#installation>`_.

- Set up Poetry. For this, you only need to run the following script.

.. code:: bash

   ./setup_poetry.sh

- Package installation: On an Incora Windows machine, run:

.. code:: bash

   # output should be python 3.8.1
   python -V

   # output should be "All set!"
   poetry check

   # This will install all python dependencies
   poetry install

.. _setup_precommit_hooks:

Setup pre-commit hooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use `pre-commit <https://pre-commit.com/>`__ to enforce code quality when
committing new code. To set up the commit hooks run following in your local repository.
This will create two files ``.git/hooks/commit-msg`` and ``.git/hooks/pre-commit``.

.. code:: bash

   poetry run pre-commit install
   poetry run pre-commit install --hook-type commit-msg

.. note::
  From now on, pre-commit will run automatically on each commit and check only the staged files.

In case you want to run it manually, you can execute:

.. code:: bash

   poetry run pre-commit

And to run the pre-commit on all files in the repository (not just staged ones), pass the
``--all-files`` flag to the above command.

.. code:: bash

   poetry run pre-commit run --all-files

.. _setup_ODBC_windows:

Setting up ODBC connection (Windows)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this section we:

- Setup ODBC Driver 17 for SQL Server
- Setup MSSQL
- Setup ODBC connections between our local environment and the SQL Server

   - ML_Internal_DB
   - ML_DSX_write
   - ML_DSX_read

Setup ODBC Driver 17 for SQL Server
"""""""""""""""""""""""""""""""""""""

In case the ODBC Driver 17 for SQL Server is not installed, follow this link and complete its
installation on your machine: `ODBC Driver for SQL Server <https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15>`_.

Setup MSSQL
"""""""""""""""""""""""""""""""""""""

Download and install
`SQL Server Management Studio <https://docs.microsoft.com/de-de/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver15>`_.
If everything worked, you should be able to connect to the staging or to the production
databases:

- Staging: ``TXAU-SQL1901``
- Production: ``TXAU-SQL1902\DSX1``

Setup ODBC connections between our local environment and the SQL Server
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

.. warning::
  Before adding any connection with production systems, please ensure you understand the risk and
  impact of accessing and modifying the production database from a local environment. General recommendation
  is that only the **dedicated production environment** should access and write to the production database.

Note: If the platform is being installed with a local SQL Server, first install `SQLExpress <https://www.microsoft.com/en-us/sql-server/sql-server-downloads>`_.
Additionally, in the expressions below, modify the `Server` name to ``localhost/SQLExpress``
and do not provide a `Database` name. In this case, make sure you have the ``DSX_anonymized_input.csv.gz``
file (see :ref:`Test & Run the platform <tests_run>`) to populate the `ML_DSX_read` DB.

There are three connections needed between our local environment and the SQL Server:

- ML_Internal_DB:

.. code:: bash

   # For testing on Incora systems:
   Add-OdbcDsn -Name "ML_Internal_DB" -DriverName "ODBC Driver 17 for SQL Server" -DsnType "User" -SetPropertyValue @("Server=TXAU-SQL1901", "Trusted_Connection=Yes","Database=Inc_ML_Internal")

   # For production on Incora systems:
   Add-OdbcDsn -Name "ML_Internal_DB" -DriverName "ODBC Driver 17 for SQL Server" -DsnType "User" -SetPropertyValue @("Server=TXAU-SQL1902\DSX1", "Trusted_Connection=Yes","Database=ML_Prod")


- ML_DSX_read

.. code:: bash

   Add-OdbcDsn -Name "ML_DSX_read" -DriverName "ODBC Driver 17 for SQL Server" -DsnType "User" -SetPropertyValue @("Server=TXAU-SQL1902\DSX1", "Trusted_Connection=Yes","Database=ML_CUSTOM")

- ML_DSX_write (only for production)

.. code:: bash

   Add-OdbcDsn -Name "ML_DSX_write" -DriverName "ODBC Driver 17 for SQL Server" -DsnType "User" -SetPropertyValue @("Server=TXAU-SQL1902\DSX1", "Trusted_Connection=Yes","Database=DSX_PROD")



.. _setup_h2o:

Setting up H2O
~~~~~~~~~~~~~~

If you do not want to run the H2O server via a Docker container, it is recommended
to start a server via a separate Python process during development.
This avoids unnecessary H2O restarts when running unit and integration tests with pytest.

To do this, start Python on your Git Bash by typing:

.. code:: python

   # Start python
   ipython

   # Import and initiate h2o
   import h2o
   h2o.init(port=54321)

When you are finished close the H2O instance and Python with:

.. code:: python

   h2o.cluster().shutdown()
   exit()

.. _tests_run:

Testing & running the platform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run these tests remember to navigate with Git Bash to the folder connected
to the repository ``[..]/DSX_ML_Forecasting``

Set-up of data files (interim)
"""""""""""""""""""""""""""""""

The following interim steps have to be taken to make the development set-up work
on a local machine, creating directories where missing. Download the files from "Releases" on GitHub.

- Download ``identifier.csv`` and move into ``00 Config``
- Download ``anonymized_data_dsx.zip`` and unpack
  into folder ``03 Processed data\monthly_process\202002\``
- Download ``DSX_anonymized_input.zip`` and unpack into folder ``01 Raw data``
- Download ``expected_results.zip`` and move into root folder

Run the following commands to set-up the database for the pytest integration tests (both required):

.. code:: bash

   # Set up the tables for the internal DB
   poetry run python -m forecasting_platform setup-database internal

   # Set up the tables where the output will be stored for development and production runs
   poetry run python -m forecasting_platform setup-database dsx-write


Test
""""

Tests can be started by running the following command in the project root directory:

.. code:: bash

   poetry run pytest

Note that for performance reasons, tests are parallelized to speed-up the whole test-suite.
If you want to run individual tests faster, without parallelization, run:

.. code:: bash

   poetry run pytest --dist=no -n0


Run
"""

After successfully testing the platform, we are ready for the last aspects of the setup to be able to
run the forecasting platform freely.

For this, first you need to:

- Download ``DSX_exogenous_features.csv`` and ``DSX_anonymized_input.csv.gz``,
  and move both into ``forecasting_platform_persistent_data``.
  This folder should be located in the parent directory of the folder where the Git repository was downloaded.

Next, open Git Bash, activate the conda environment, navigate to the repository folder and checkout the staging branch:

.. code:: bash

   cd <relative path to repository folder>
   conda activate <name>
   git checkout staging

Now, everything is ready to run the forecasting platform. Remember that the forecasting platform
is intended to be used via CLI. Detailed usage information and parameters can be found in :ref:`Usage`.

Note the ``-h`` flag will display available commands and help.

.. code:: bash

   forecasting_platform -h

   # In case the `forecasting_platform` shortcut cannot be found, try the longer command:
   poetry run python -m forecasting_platform -h


Linux and MacOS specific commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _setup_python_Linux_MacOS:

Setup Python (Linux/MacOS)
""""""""""""""""""""""""""

First you need to install ``pyenv`` and ``pyenv-virtualenv`` on your machine. For Linux
we recommend using the official package manager / installation guidelines for your
distribution.

On MacOS we recommend the installation via `Homebrew <https://brew.sh/>`_.

.. code:: bash

   brew install pyenv pyenv-virtualenv

Add the following to your shell startup script (e.g. ``~/.bashrc`` or
``~/.zshrc``) and restart your terminal for this change to take effect.

.. code:: bash

   eval "$(pyenv init -)"
   if which pyenv-virtualenv-init > /dev/null; then eval "$(pyenv virtualenv-init -)"; fi

A new virtualenv matching the Python version specified in ``.python-version`` needs to
be setup for this repository. To do this run the following script.

.. code:: bash

   ./setup_pyenv.sh


.. _setup_ODBC_Linux_MacOS:

Setting up ODBC connection (MacOS)
""""""""""""""""""""""""""""""""""

Install Microsoft ODBC driver on MacOS via Homebrew using the following commands
(more details can be found in the official
`Microsoft documentation for MacOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>`_.

.. code:: bash

   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
   brew update
   HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql17 mssql-tools

Edit ``/usr/local/etc/odbc.ini`` as

.. code:: ini

   [ML_Internal_DB]
   Driver      = ODBC Driver 17 for SQL Server
   Description = Connect to local Docker SQL Server
   Trace       = No
   Server      = 127.0.0.1

Check connection with:

``isql ML_Internal_DB sa Password1``


Setting up ODBC connection (Linux)
""""""""""""""""""""""""""""""""""

Details for Linux can be found in the `Microsoft documentation for Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`_

An example setup for Ubuntu can be found in ``mssql/setup_odbc_ubuntu.sh``.
This is also used in the ``Dockerfile`` to setup the ODBC connection for the dockerized development environment.
