FROM apache/airflow:3.3.1

# Airflow publishes a constraints file per release and Python version, pinning the whole
# dependency tree it was tested against. Installing through it stops an added package from
# dragging Airflow's own dependencies to versions it never saw.
#
# Four of its 692 pins are dropped, for two different reasons.
#
# MLflow 3.15.1 cannot coexist with these two:
#   cryptography  constraints pin 50.0.0, mlflow requires <50
#   pandas        constraints pin 3.0.5,  mlflow requires <3
#
# These two are capped by the host interpreter, not by a package:
#   numpy         constraints pin 2.5.1,  which requires Python >=3.12
#   scipy         constraints pin 1.18.0, which requires Python >=3.12
# The host is pinned to Python 3.11 to match the MLflow server image, so it cannot
# resolve either. Both are pinned down in requirements-airflow.txt to the versions
# uv.lock holds, because a LightGBM model serialised on the host is loaded in this
# image from step 2.4 onward and numpy/scipy are direct LightGBM dependencies.
#
# Every other pin still applies. Prefer conforming the host to this file over adding
# to this list -- each filtered pin is a deviation from a tested set that we then own.
# Recheck when Airflow, MLflow, or the host Python version moves.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.13
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN curl -sSL "${CONSTRAINTS}" -o /tmp/constraints-airflow.txt \
    && grep -viE '^(cryptography|pandas|numpy|scipy)==' /tmp/constraints-airflow.txt > /tmp/constraints-filtered.txt \
    && pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
         --constraint /tmp/constraints-filtered.txt
