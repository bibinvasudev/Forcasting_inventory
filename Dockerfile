# syntax=docker/dockerfile:experimental

FROM python:3.8.1

WORKDIR /ow-forecasting-platform

COPY mssql/ ./mssql

RUN sed -i -e 's/localhost/mssql-server/g' mssql/odbc_dsn_ubuntu.ini && \
    mssql/setup_odbc_ubuntu.sh

RUN pip install poetry==1.0.5

COPY pyproject.toml poetry.lock setup_owforecasting.sh ./

RUN --mount=type=ssh \
    mkdir -p ~/.ssh/ && ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts && \
    ./setup_owforecasting.sh && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

COPY ["00 Config", "00 Config"]
COPY ["01 Raw data", "01 Raw data"]
COPY ["03 Processed data", "03 Processed data"]

COPY forecasting_platform/ ./forecasting_platform/

CMD ["/bin/bash"]
