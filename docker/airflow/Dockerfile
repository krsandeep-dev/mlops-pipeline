FROM apache/airflow:3.3.1-python3.11

# Airflow publishes a constraints file per release and Python version, pinning the whole
# dependency tree it was tested against. Installing through it stops an added package from
# dragging Airflow's own dependencies to versions it never saw.
#
# The base image is the python3.11 variant on purpose. Airflow publishes constraints
# per interpreter, and the whole stack -- host, MLflow server, this image -- runs 3.11,
# so one dependency resolution applies everywhere. On the default 3.13 base the
# constraints pinned numpy 2.5.1 and scipy 1.18.0, which require Python >=3.12 and so
# could never be installed on the 3.11 host; both then needed hand-pinning. On 3.11
# the constraints land on numpy 2.4.6 and scipy 1.17.1 by themselves -- the versions
# the host lockfile already holds -- and the deviation list halves.
#
# Two of its 728 pins remain dropped, both because MLflow 3.15.1 cannot coexist:
#   cryptography  constraints pin 50.0.0, mlflow requires <50
#   pandas        constraints pin 3.0.5,  mlflow requires <3
#
# Every other pin still applies. Prefer conforming the host to this file over adding
# to this list -- each filtered pin is a deviation from a tested set that we then own.
# Recheck when Airflow or MLflow moves.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN curl -sSL "${CONSTRAINTS}" -o /tmp/constraints-airflow.txt \
    && grep -viE '^(cryptography|pandas)==' /tmp/constraints-airflow.txt > /tmp/constraints-filtered.txt \
    && pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
         --constraint /tmp/constraints-filtered.txt
