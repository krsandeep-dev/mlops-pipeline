# Phase 1 spec — step 1.3: Airflow into the Compose stack

Scope: add Apache Airflow 3.3.1 to the **existing** `docker-compose.yml` from step 1.2.
Nothing from 1.2 gets rewritten — postgres, minio, minio-init and mlflow stay exactly as
they are. Terraform, DVC and the real ingestion DAG are steps 1.4–1.6; do not add them now.

---

## Part 0 — What you are building and why (read before implementing)

### Airflow 3 is five processes, not one

| Component | Job | Required? |
| --- | --- | --- |
| **api-server** | Serves the web UI **and** the REST API **and** the Task Execution API that running tasks call | yes |
| **scheduler** | Decides which task runs when; runs the executor in-process | yes |
| **dag-processor** | Parses the Python files in `dags/` into serialized DAGs in the DB | yes (standalone in Airflow 3) |
| **triggerer** | Runs deferrable/async operators | practically yes |
| **metadata DB** | Stores DAGs, runs, task state | yes — we reuse our Postgres |

Two Airflow 3 changes drive this design:

1. **The dag-processor is its own process.** In Airflow 2 the scheduler parsed DAG files
   itself, meaning arbitrary user code ran inside the scheduler. Airflow 3 separates
   them so DAG-parsing code can't touch the scheduler or the metadata DB directly.
2. **Tasks no longer talk to the metadata database.** They call the Task Execution API on
   the api-server instead. That's why every component needs
   `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, and why a broken api-server cascades into
   dag-processor hangs. If something is wrong, debug the api-server first.

### Decisions taken (and the reasoning to keep for interviews)

- **LocalExecutor, not CeleryExecutor.** Celery adds Redis plus worker containers to
  buy horizontal scale we don't need locally. LocalExecutor runs tasks as subprocesses of
  the scheduler — right for one machine. Phase 3+ shows the Kubernetes serving path;
  KubernetesExecutor would be the natural production step and is worth naming in an
  interview as the alternative you consciously didn't need.
- **Reuse the existing Postgres, separate database.** One engine, two logical DBs
  (`airflow`, `mlflow`) — already provisioned by `init-db.sh` in 1.2. Separate DBs keep
  the schemas independent; a shared engine keeps local resource use sane.
- **Custom Airflow image, not `_PIP_ADDITIONAL_REQUIREMENTS`.** That env var installs
  packages at container start on every boot — convenient, slow, and unreproducible.
  Building an image pins dependencies into a versioned artifact, which is what a real
  deployment does.
- **SimpleAuthManager.** Airflow 3's default. Enough for local work, and it exposes the
  `/auth/token` JWT flow we'll need in Phase 5 to trigger retraining via the REST API.

---

## Part 0b — Required fix to the 1.2 MLflow service (do this first)

MLflow 3.5.0 and later ship security middleware that validates the HTTP `Host` header to
block DNS rebinding attacks. When the server binds to `0.0.0.0`, only a default allowlist
of localhost-style hosts is accepted. Your browser hitting `localhost:5001` passes; the
Airflow container calling `http://mlflow:5000` sends `Host: mlflow:5000`, which is not on
that list, and the request is rejected with *"Invalid Host header - possible DNS rebinding
attack detected"*.

This is a genuine security control, not a bug — the correct fix is to allow the specific
host, not to disable the middleware.

### `docker-compose.yml`, `mlflow` service — before

```yaml
    command: >
      mlflow server
      --backend-store-uri postgresql+psycopg2://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@postgres:5432/mlflow
      --artifacts-destination s3://mlflow-artifacts
      --host 0.0.0.0
      --port 5000
```

### After

```yaml
    command: >
      mlflow server
      --backend-store-uri postgresql+psycopg2://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@postgres:5432/mlflow
      --artifacts-destination s3://mlflow-artifacts
      --host 0.0.0.0
      --port 5000
      --allowed-hosts "localhost,localhost:*,127.0.0.1,127.0.0.1:*,mlflow,mlflow:*"
```

Then `docker compose up -d mlflow` and re-run `scripts/smoke_mlflow.py` to confirm the
browser path still works before moving on.

Three things to understand:

- **`--allowed-hosts` replaces the defaults, it does not extend them.** Drop the localhost
  entries and your browser at `:5001` breaks instead.
- **Both `mlflow` and `mlflow:*` are listed on purpose.** The matcher compares the raw
  `Host` header, including the port, against each pattern without stripping it — an entry
  of just `mlflow` will not match `mlflow:5000`. This has caused real confusion in
  Kubernetes deployments where a service DNS name arrives with a port attached.
- **There is a `--disable-security-middleware` style escape hatch. Don't use it.** Turning
  off a security control to make a container talk to another container is exactly the
  reflex the Teaching-mode rule about root causes exists to prevent. The equivalent
  production answer is a reverse proxy terminating TLS with an explicit allowlist, which
  belongs in the README's production gaps.
- These options only work with MLflow's default FastAPI/uvicorn server — they are ignored
  under `--gunicorn-opts` or `--waitress-opts`.

---

## Part 1 — Change: `.env.example` and `.env`

Airflow needs two secrets, plus your host UID so container-written files aren't owned by
root.

**Before** (end of `.env.example`):

```bash
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=change_me_locally
```

**After:**

```bash
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=change_me_locally

# Airflow secrets — generate real values into .env, never commit them
AIRFLOW_FERNET_KEY=change_me_locally
AIRFLOW_JWT_SECRET=change_me_locally
AIRFLOW_UID=50000
```

Generate the real values into `.env` (not `.env.example`):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
openssl rand -hex 32
echo $(id -u)
```

What each one does:

- `AIRFLOW_FERNET_KEY` — symmetric key encrypting connection passwords and variables at
  rest in the metadata DB. Lose it and every stored credential becomes unreadable.
- `AIRFLOW_JWT_SECRET` — signs the tokens tasks use against the Task Execution API. Must
  be **identical across all Airflow components**, or tasks fail authentication.
- `AIRFLOW_UID` — the UID inside the container; matching your host UID stops
  `logs/` filling up with root-owned files.

---

## Part 2 — Change: directories and `.gitignore`

```bash
mkdir -p logs plugins config
```

`.gitignore` **before**:

```
.env
.venv/
__pycache__/
*.pyc
mlruns/
.terraform/
*.tfstate*
```

**After** (four lines added):

```
.env
.venv/
__pycache__/
*.pyc
mlruns/
.terraform/
*.tfstate*
logs/
plugins/
config/simple_auth_manager_passwords.json
!config/.gitkeep
```

Then `touch config/.gitkeep plugins/.gitkeep` so the directories survive a clone.

---

## Part 3 — New file: `docker/airflow/Dockerfile`

```dockerfile
FROM apache/airflow:3.3.1

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
```

## Part 4 — New file: `docker/airflow/requirements-airflow.txt`

```
mlflow==3.15.1
boto3==1.40.11
```

Only what step 1.3 needs. DVC, pandas and LightGBM get added in 1.5–2.x, deliberately, so
each addition has a visible reason. Keep the MLflow version identical to the server and
the client — three places, one version.

> Note: never `pip install apache-airflow` into this image or pin a conflicting version
> of it. The base image already has Airflow; adding it to requirements can silently
> upgrade or break the install.

---

## Part 5 — Change: `docker-compose.yml`

### 5a. Add a YAML anchor above `services:`

All four Airflow containers run the same image with the same config and differ only in
their `command`. A YAML anchor defines that shared block once.

**Before** — file starts with:

```yaml
services:
  postgres:
```

**After** — insert this block above `services:`:

```yaml
x-airflow-common: &airflow-common
  build: ./docker/airflow
  image: mlops-pipeline/airflow:3.3.1
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: "true"
    PYTHONPATH: /opt/airflow/src
    MLFLOW_TRACKING_URI: http://mlflow:5000
    MLFLOW_S3_ENDPOINT_URL: http://minio:9000
    AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
    AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
    - ./src:/opt/airflow/src
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
```

Line-by-line notes worth understanding:

- `&airflow-common` defines the anchor; `*airflow-common` below reuses it.
- `MLFLOW_TRACKING_URI: http://mlflow:5000` — **container port 5000, not 5001**. Inside
  the Compose network, containers reach each other by service name on the container port;
  `5001` is only the host-side mapping for your browser.
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` pointed at MinIO: the S3 SDK doesn't care
  that it isn't AWS, which is exactly the property that makes the cloud switch cheap
  later.
- `PYTHONPATH: /opt/airflow/src` + the `./src` mount lets DAGs `import mlops_pipeline`,
  so pipeline logic lives in a testable package instead of inside DAG files.
- `LOAD_EXAMPLES: "false"` — the ~50 bundled example DAGs otherwise bury yours.

### 5b. Append five services at the end of `services:`

Insert after the `mlflow:` service, before the top-level `volumes:` key:

```yaml
  airflow-init:
    <<: *airflow-common
    command: db migrate
    restart: "no"

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/version"]
      interval: 10s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      postgres:
        condition: service_healthy
      airflow-apiserver:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    depends_on:
      postgres:
        condition: service_healthy
      airflow-apiserver:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    depends_on:
      postgres:
        condition: service_healthy
      airflow-apiserver:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: unless-stopped
```

Notes:

- `<<: *airflow-common` merges the anchor block; each service overrides only `command`
  and its own `depends_on`/`healthcheck`.
- `airflow-init` runs `airflow db migrate` (creates/upgrades the schema), then exits.
  Everything else waits for `service_completed_successfully` — ordering, not luck.
- Scheduler, dag-processor and triggerer wait for the api-server to be **healthy**, not
  merely started, because of the Task Execution API dependency described in Part 0.
- `$${HOSTNAME}` — the doubled `$` escapes Compose's own variable substitution so the
  shell inside the container expands it.

---

## Part 6 — New file: `dags/hello_stack.py`

A deliberately tiny DAG whose only job is to prove Airflow can reach MLflow — the exact
network path Phase 2's training DAG depends on.

```python
"""Smoke-test DAG: proves Airflow can reach the MLflow tracking server."""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_stack",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke"],
)
def hello_stack():
    @task
    def check_mlflow() -> str:
        import mlflow

        experiments = mlflow.search_experiments()
        names = [e.name for e in experiments]
        print(f"MLflow reachable — experiments: {names}")
        return f"{len(names)} experiments"

    check_mlflow()


hello_stack()
```

Three things to notice:

- `from airflow.sdk import dag, task` — Airflow 3's Task SDK. The old
  `airflow.decorators` import still works but is the 2.x style; use the SDK.
- `schedule=None` — triggered manually only. Phase 5 will trigger a DAG exactly this way,
  over the REST API.
- `mlflow.set_tracking_uri(...)` is absent on purpose: MLflow reads
  `MLFLOW_TRACKING_URI` from the environment we set in the anchor. Config through
  environment, not hardcoding, is what makes the same DAG run unchanged in the cloud.

---

## Part 7 — Run and verify

```bash
docker compose up -d --build          # first build takes a few minutes
docker compose ps                     # airflow-init exited (0), the rest healthy
cat config/simple_auth_manager_passwords.json   # your generated admin password
```

Open http://localhost:8080, log in as `admin` with that password, un-pause `hello_stack`,
trigger it, and open the task log.

### Definition of done

- [ ] `docker compose ps`: postgres, minio, mlflow, airflow-apiserver, airflow-scheduler,
      airflow-dag-processor, airflow-triggerer all healthy; airflow-init exited (0)
- [ ] Airflow UI reachable at http://localhost:8080 and login works
- [ ] `hello_stack` visible with no import errors, and **no example DAGs** listed
- [ ] Triggered run succeeds; task log prints `MLflow reachable — experiments: [...]`
      including `smoke-test` from step 1.2
- [ ] Step 1.2 still works: MLflow UI at :5001, MinIO console at :9001
- [ ] `git status` shows no `.env`, no `logs/`, no generated password file

### If something breaks

| Symptom | Cause and fix |
| --- | --- |
| Task fails with "Invalid Host header" | `--allowed-hosts` missing or incomplete on the mlflow service — see Part 0b. |
| dag-processor unhealthy or SIGKILLed | api-server not up. `docker compose logs airflow-apiserver` first, always. |
| Task fails with a 401/403 | `AIRFLOW_JWT_SECRET` differs between components — check the anchor is actually merged into all four. |
| DAG missing from the UI | Import error. `docker compose logs airflow-dag-processor` shows the traceback. |
| `db migrate` fails on connection | The `airflow` DB/user only exists if `init-db.sh` ran on a fresh volume (step 1.2). |
| Login rejected | Re-read `config/simple_auth_manager_passwords.json`; it regenerates if deleted. |

---

## Part 8 — Production notes (README, "Production gaps")

- Fernet key and JWT secret live in `.env` locally; production pulls them from AWS Secrets
  Manager / SSM or K8s Secrets, and rotates them.
- SimpleAuthManager is local-only. Production uses the FAB auth manager with SSO/OIDC and
  real RBAC roles.
- LocalExecutor runs tasks on the scheduler host — no isolation, no horizontal scale.
  Production: KubernetesExecutor (task-per-pod) or CeleryExecutor with a broker.
- The api-server is exposed unencrypted on :8080. Production sits behind TLS termination
  with authenticated ingress.
- `logs/` on a bind mount is single-host only. Production ships task logs to S3 via
  remote logging so they survive pod restarts.
- One Postgres serving Airflow and MLflow is a local convenience and a shared blast
  radius. Production separates them (managed RDS instances) with backups.
