#!/usr/bin/env bash

set -eu
# install odbc driver for sql server (see https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15)
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17

apt-get install -y unixodbc-dev
odbcinst -i -s -f mssql/odbc_dsn_ubuntu.ini -ly
