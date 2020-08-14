.. _setup_docker:

Docker Development Setup
~~~~~~~~~~~~~~~~~~~~~~~~

H2O and SQL server can be setup locally via Docker (recommended for Linux/MacOS, possible also on Windows).

Install `Docker Engine <https://docs.docker.com/get-started/#set-up-your-docker-environment>`_
and `Docker Compose <https://docs.docker.com/compose/install/>`_. Note for MacOS, Docker
Desktop for Mac includes Docker Compose already.


Starting Docker containers
""""""""""""""""""""""""""

.. warning::

   If you are a Windows user, make sure your git has the automatic replacement of line
   endings disabled to avoid issues with running the Docker scripts. Run the following
   command before checking out the repository. If you checked out the repository with
   this setting enabled, you need to delete all files and check-out the repository again.

   .. code:: bash

      git config --global core.autocrlf false

   Add the following line to your `.bash_profile` to make sure SSH is working with Docker.

   .. code:: bash

      eval `ssh-agent` > /dev/null

To start up your containers for the first time run the following from the repository
root directory. This can take a few minutes.

.. code:: bash

   ./build_docker_image.sh
   docker-compose up --build

``forecasting-platform`` will connect to the h2o-server and output the status of the
server in the console (example below).

.. code:: bash

   forecasting-platform_1  | --------------------------  ------------------------------------------------------------------
   forecasting-platform_1  | H2O cluster uptime:         01 secs
   forecasting-platform_1  | H2O cluster timezone:       Etc/UTC
   forecasting-platform_1  | H2O data parsing timezone:  UTC
   forecasting-platform_1  | H2O cluster version:        3.28.1.2
   forecasting-platform_1  | H2O cluster version age:    15 days
   forecasting-platform_1  | H2O cluster name:           root
   forecasting-platform_1  | H2O cluster total nodes:    1
   forecasting-platform_1  | H2O cluster free memory:    910 Mb
   forecasting-platform_1  | H2O cluster total cores:    6
   forecasting-platform_1  | H2O cluster allowed cores:  6
   forecasting-platform_1  | H2O cluster status:         accepting new members, healthy
   forecasting-platform_1  | H2O connection url:         http://h2o-server:54321
   forecasting-platform_1  | H2O connection proxy:
   forecasting-platform_1  | H2O internal security:      False
   forecasting-platform_1  | H2O API Extensions:         Amazon S3, XGBoost, Algos, AutoML, Core V3, TargetEncoder, Core V4
   forecasting-platform_1  | Python version:             3.8.1 final
   forecasting-platform_1  | --------------------------  ------------------------------------------------------------------

If you want explore the containerized h2o server navigate to http://localhost:54321/ to
open h2o-server web interface.

In the future, to restart the containers without rebuilding them, run:

.. code:: bash

   docker-compose up

To start a shell within the docker-compose container, run:

.. code:: bash

   docker-compose exec forecasting-platform bash

Shut down the docker-compose once you are done running:

.. code:: bash

   docker-compose down

Local Jenkins
"""""""""""""

There is a local Jenkins instance, which can be enabled in the ``docker-compose.yaml`` file.
After running ``docker-compose up`` you can open http://localhost:8080 to access the local Jenkins.

For the initial setup, use recommended plugins during initial setup and default settings.

The default admin password for this local Jenkins can be found here:

.. code:: bash

   cat jenkins_home/secrets/initialAdminPassword

Create a new Jenkins “Multibranch Pipeline” on the local Jenkins website with name
“jenkinsfile”. Select ”Add Source" > “Git” under “Branch Sources”, set “Project
Repository” to ``/ow-forecasting-platform``. “Save” new pipeline.

Check http://localhost:8080/job/jenkinsfile/indexing/console for status and click
“Scan Multibranch Pipeline Now” to trigger a refresh of your local repository from Jenkins.
