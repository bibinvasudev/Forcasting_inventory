forecasting_platform.services
==============================

.. currentmodule:: forecasting_platform.services

``services`` Overview
---------------------

Services are used by the platform for meaningful operations within the system environment:

- accessing and modifying the runtime environment
- reading and writing files
- loading and storing information in databases
- accessing external services (e.g. H2O)

Services are involved in the platform startup and shutdown procedure.

Services are created centrally with the :py:func:`.initialize` context-manager.

The collection of :py:class:`.Services` returned by :py:func:`.initialize` provides ready-to-use service objects.

All machine learning and processing of the platform happens within this :py:class:`.Services` context.

To see actual usage, take a look at :py:func:`~forecasting_platform.cli.run_forecast`.

``services`` Reference
----------------------

.. autosummary::
  initialize
  initialize_subprocess
  initialize_h2o_connection
  initialize_warnings
  initialize_random_seed
  initialize_faulthandler
  initialize_logging
  Services
  Orchestrator
  RuntimeConfig
  DataLoader
  DataOutput
  Database

.. automodule:: forecasting_platform.services
