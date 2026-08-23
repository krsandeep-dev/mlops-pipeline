FROM apache/airflow:3.3.1

# Airflow publishes a constraints file per release and Python version, pinning the whole
# dependency tree it was tested against. Installing through it stops an added package from
# dragging Airflow's own dependencies to versions it never saw.
#
# Two of its 692 pins are dropped because MLflow 3.15.1 cannot coexist with them:
#   cryptography  constraints pin 50.0.0, mlflow requires <50
#   pandas        constraints pin 3.0.5,  mlflow requires <3
# Both ceilings are MLflow's, and the tracking client is why this image exists, so MLflow
# wins and pip resolves those two itself. Every other pin still applies. Recheck this list
# when either Airflow or MLflow moves.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.13
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN curl -sSL "${CONSTRAINTS}" -o /tmp/constraints-airflow.txt \
    && grep -viE '^(cryptography|pandas)==' /tmp/constraints-airflow.txt > /tmp/constraints-filtered.txt \
    && pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
         --constraint /tmp/constraints-filtered.txt
