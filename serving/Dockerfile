# Multi-stage: the builder holds the toolchain, the runtime holds only what serves.
# Build context is the repo root (the image needs both serving/ and src/mlops_pipeline):
#   docker build -f serving/Dockerfile -t mlops-serving:<sha> .

FROM python:3.11-slim AS builder

# Python 3.11 everywhere -- host, MLflow server, Airflow image, and here. The champion
# model records python 3.11.15 in its python_env.yaml, so the serving interpreter
# matches the one that trained it.
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY serving/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt


FROM python:3.11-slim AS runtime

# Links the GHCR package to this repository. GitHub reads it at publish time, so it has
# to be present on the FIRST push -- adding it later does not retroactively link an
# already-published package.
LABEL org.opencontainers.image.source="https://github.com/krsandeep-dev/mlops-pipeline" \
      org.opencontainers.image.description="FastAPI serving of the MLflow @champion trip-duration model" \
      org.opencontainers.image.licenses="MIT"

# LightGBM links the GNU OpenMP runtime dynamically and no wheel bundles it, so a slim
# base fails at import with "libgomp.so.1: cannot open shared object file". Same reason
# the Airflow image installs it; the macOS equivalent is `brew install libomp`.
RUN apt-get update \
    && apt-get install --no-install-recommends -y libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Non-root: the app needs no privileges, and MLflow writes only to its own cache.
RUN useradd --create-home --uid 10001 serving

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
# The serving app and the pipeline package it reuses (registry.client()). PYTHONPATH
# mirrors the Airflow containers rather than installing the project, so none of the
# pipeline's heavy dependencies (dvc, airflow) land in this image.
COPY serving /app/serving
COPY src /app/src

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app:/app/src" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/serving \
    MLFLOW_TRACKING_URI=http://mlflow:5000 \
    MODEL_URI=models:/taxi-trip-duration@champion

USER serving
EXPOSE 8000

# One uvicorn worker per pod; scale with replicas, not workers.
CMD ["uvicorn", "serving.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
