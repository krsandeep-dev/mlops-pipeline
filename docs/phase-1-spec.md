# Phase 1 spec — steps 1.1 and 1.2

Scope: repo scaffold + core local services (MinIO, Postgres, MLflow) in Docker Compose.
Implement every file exactly as written here — no redesigns, no version changes.
Airflow, Terraform, DVC, and the ingestion DAG are later steps; do not add them now.

## Definition of done
- [ ] Repo matches the target tree below
- [ ] `docker compose ps`: postgres, minio, mlflow running/healthy; minio-init exited (0)
- [ ] MLflow UI reachable at http://localhost:5001
- [ ] Smoke test passes; run "wiring-check" visible in MLflow with `hello.txt` artifact
- [ ] The artifact object exists in the `mlflow-artifacts` bucket (MinIO console, http://localhost:9001)
- [ ] `.env` exists locally but is NOT tracked by git

## Step 1.1 — Scaffold

Run from the repo root (uv is already installed via Homebrew; install with `brew install uv` if missing):

```bash
git init
uv init --bare --python 3.11
uv add mlflow==3.15.1 boto3
uv add --dev ruff pytest
mkdir -p dags src/mlops_pipeline docker/mlflow docker/postgres infra/terraform scripts tests
touch src/mlops_pipeline/__init__.py
```

Note: MLflow is pinned to 3.15.1 in both the client (pyproject.toml) and the server image
(Dockerfile below). Client/server version mismatch causes unexpected behavior — keep them
identical.

### Target tree after 1.1–1.2

```
mlops-pipeline/
├── CLAUDE.md
├── dags/                      # populated in 1.3+
├── docs/
│   └── phase-1-spec.md        # this file
├── src/mlops_pipeline/
│   └── __init__.py
├── docker/
│   ├── mlflow/Dockerfile
│   └── postgres/init-db.sh
├── infra/terraform/           # populated in 1.4
├── scripts/
│   └── smoke_mlflow.py
├── tests/
├── docker-compose.yml
├── .env                       # local only, gitignored
├── .env.example
├── .gitignore
└── pyproject.toml
```

### `.gitignore`

```
.env
.venv/
__pycache__/
*.pyc
mlruns/
.terraform/
*.tfstate*
```

## Step 1.2 — Core services

### `.env.example`

Commit this file. Then copy it to `.env` and replace every `change_me_locally` with a
generated local-only password (these are local dev secrets, not production secrets).

```bash
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=change_me_locally
POSTGRES_USER=postgres
POSTGRES_PASSWORD=change_me_locally
MLFLOW_DB_USER=mlflow
MLFLOW_DB_PASSWORD=change_me_locally
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=change_me_locally
```

### `docker/postgres/init-db.sh`

One Postgres instance, two databases. The `airflow` database is provisioned now so step 1.3
can use it without recreating the volume. This script only runs on a fresh (empty) volume;
to re-run it use `docker compose down -v` (destroys all local data).

```bash
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER ${MLFLOW_DB_USER} WITH PASSWORD '${MLFLOW_DB_PASSWORD}';
    CREATE DATABASE mlflow OWNER ${MLFLOW_DB_USER};
    CREATE USER ${AIRFLOW_DB_USER} WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER ${AIRFLOW_DB_USER};
EOSQL
```

### `docker/mlflow/Dockerfile`

```dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir mlflow==3.15.1 psycopg2-binary boto3
EXPOSE 5000
```

### `docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      MLFLOW_DB_USER: ${MLFLOW_DB_USER}
      MLFLOW_DB_PASSWORD: ${MLFLOW_DB_PASSWORD}
      AIRFLOW_DB_USER: ${AIRFLOW_DB_USER}
      AIRFLOW_DB_PASSWORD: ${AIRFLOW_DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init-db.sh:/docker-entrypoint-initdb.d/init-db.sh:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD &&
      mc mb --ignore-existing local/mlflow-artifacts local/dvc-data
      "

  mlflow:
    build: ./docker/mlflow
    depends_on:
      postgres:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully
    environment:
      MLFLOW_S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
    command: >
      mlflow server
      --backend-store-uri postgresql+psycopg2://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@postgres:5432/mlflow
      --artifacts-destination s3://mlflow-artifacts
      --host 0.0.0.0
      --port 5000
    ports:
      - "5001:5000"

volumes:
  postgres_data:
  minio_data:
```

Host port is 5001 on purpose: macOS AirPlay Receiver listens on port 5000 and silently
answers with 403s. Do not remap to 5000.

### `scripts/smoke_mlflow.py`

Proves the whole chain: client → MLflow server → Postgres (metadata) → MinIO (artifact).
Note it needs no S3 credentials — the server proxies artifact traffic.

```python
import mlflow

mlflow.set_tracking_uri("http://localhost:5001")
mlflow.set_experiment("smoke-test")

with mlflow.start_run(run_name="wiring-check") as run:
    mlflow.log_param("phase", 1)
    mlflow.log_metric("answer", 42)
    with open("/tmp/hello.txt", "w") as f:
        f.write("artifact stored in MinIO\n")
    mlflow.log_artifact("/tmp/hello.txt")
    print(f"OK — run_id: {run.info.run_id}")
```

## Run and verify

```bash
cp .env.example .env       # then set real local passwords
docker compose up -d --build
docker compose ps          # all healthy; minio-init exited (0)
uv run python scripts/smoke_mlflow.py
```

Then check the definition-of-done list at the top.

## Production notes (add to README under "Production gaps")

- `.env` is the local stand-in for secrets. Production: AWS Secrets Manager / SSM
  Parameter Store, K8s Secrets in-cluster. Formalized in Phase 6.
- The smoke test needed no storage credentials because the MLflow server proxies artifact
  traffic — clients never hold S3 creds. This is the correct production pattern.
- MinIO root credentials are reused for the MLflow service locally; production uses a
  scoped service account / IAM role per service.
- `minio/minio:latest` is acceptable locally; production pins image digests.
