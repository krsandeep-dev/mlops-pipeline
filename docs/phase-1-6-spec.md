# Phase 1 spec — step 1.6: the ingestion DAG

Scope: the DAG that turns a DVC-pinned dataset into a validated, sampled artifact the rest
of the project builds on. This closes Phase 1.

Before-blocks are **excerpts showing where to anchor an edit**, not full-file
reproductions.

---

## Part 0 — What this DAG is for

Three tasks, and each one exists for a reason that shows up later:

| Task | Does | Why it matters downstream |
| --- | --- | --- |
| `resolve_dataset` | Turns the committed `.dvc` pointer into a concrete object URL and hash | The reproducibility claim: this run used *these* bytes |
| `validate_raw` | Schema, row count, null and range checks; fails the DAG on violation | A quality gate before bad data reaches training |
| `write_reference_sample` | Deterministic sample written to the landing bucket | Phase 2 trains on it; **Phase 5 uses it as the drift reference** |

The third task is the one to understand. Evidently detects drift by comparing incoming
production data against a *reference distribution*. That reference has to come from
somewhere, and it has to be the same data the model was trained on, or the comparison is
meaningless. Producing it here — pinned, hashed, reproducible — is what makes Phase 5
honest rather than a demo. It also keeps training under 30 seconds, since 3.07M rows would
blow that budget.

### Decision: how Airflow reaches DVC-pinned data

The problem: `.dvc/config` sets the MinIO endpoint to `http://localhost:9000`, which is
correct on your Mac and wrong inside a container, where the same service is `minio:9000`.
This is the `5001` vs `5000` lesson from 1.3 in a new costume.

The fix is a **third remote for the in-container network view**:

| Remote | Endpoint | Used by |
| --- | --- | --- |
| `minio` (default) | `http://localhost:9000` | you, on the host |
| `minio-docker` | `http://minio:9000` | Airflow tasks |
| `aws` | S3 | the cloud switch |

Same bucket, same objects, same content hashes — only the route differs. Credentials for
`minio-docker` are deliberately *not* in the config: DVC falls back to
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, which the Airflow anchor already supplies.

And the DAG **resolves** the URL rather than pulling the file. `dvc.api.get_url` returns
the object's remote location without downloading it, so pandas streams the parquet straight
from object storage. No second copy, no cache in the container, no writes into your repo.

---

## Part 1 — Change: `docker-compose.yml`

### 1a. Airflow needs to see the DVC metadata

`x-airflow-common` volumes — before:

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
    - ./src:/opt/airflow/src
```

After:

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
    - ./src:/opt/airflow/src
    - ./.dvc:/opt/airflow/repo/.dvc
    - ./.git:/opt/airflow/repo/.git:ro
    - ./data:/opt/airflow/repo/data:ro
```

Three narrow mounts rather than the whole repo, on purpose — but be precise about what that
buys. `.env` stays out of the container filesystem, and so does everything else in the repo
root. It is **not** a credential-free mount: `.dvc/config.local` carries the MinIO access
key and secret. That's no new exposure, since the anchor already injects the same values as
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, but "narrow" here means smaller blast radius,
not zero secrets.

`.git` and `data` are read-only — the DAG reads pointers, it must never rewrite your
repository. `.dvc` is writable because DVC creates `.dvc/tmp` during resolution and fails on
a read-only path; its cache is already gitignored.

One latent trap: `config.local` also sets `profile = mlops` on the `aws` remote, and that
profile does not exist inside the container. Harmless while every task passes
`remote="minio-docker"` explicitly — which is exactly why that must be a rule rather than a
habit (Part 8).

### 1b. Landing bucket

`minio-init` entrypoint — before:

```yaml
      mc mb --ignore-existing local/mlflow-artifacts local/dvc-data
```

After:

```yaml
      mc mb --ignore-existing local/mlflow-artifacts local/dvc-data local/landing
```

Separate bucket, not a prefix inside `dvc-data`: that bucket is DVC's content-addressed
store and application data does not belong mixed into it. On AWS the equivalent is a
`landing/` prefix in the Terraform bucket.

Unlike Postgres's `init-db.sh`, which only executes against an empty data directory,
`minio-init` is a one-shot service that re-runs on every `docker compose up`. Editing the
entrypoint is therefore sufficient — the bucket appears on the next start. Creating it
directly is harmless belt-and-braces if you'd rather not wait:

```bash
docker compose exec minio mc mb --ignore-existing local/landing
```

---

## Part 2 — Change: `docker/airflow/requirements-airflow.txt`

Filtering `pandas` and `cryptography` out of Airflow's constraints in 1.5 left both without
an exact pin. MLflow's `<3` and `<50` ceilings still bound them, so this is drift risk
rather than an unbounded range — but a rebuild three months from now can silently install
different versions, which defeats the point of using a constraints file at all.

Replace both pins by hand, exactly, and add the new dependencies:

```
# Filtered from Airflow's constraints (they conflict with mlflow's ceilings);
# re-pinned here by hand. Keep in sync with uv.lock.
pandas==2.3.3
cryptography==49.0.0
pyarrow==25.0.0
```

Use the exact versions currently resolved on the host, not ranges. Two reasons: the whole
value of a constraints file is a reproducible image, and host/container parity matters once
Phase 2 starts serializing models — a frame pickled by one pandas and loaded by another is a
class of bug you do not want to debug through a container boundary. This is the same rule
1.5 applied to `boto3`: whatever `uv.lock` resolves, the image mirrors.

Add the same versions to `pyproject.toml` (`uv add "pandas==2.3.3" "pyarrow==25.0.0"`) —
the validation code is unit-tested on the host in Part 5, so the host needs them too.
`s3fs` arrived with `dvc[s3]`, so parquet-over-S3 already works.

---

## Part 3 — New DVC remote

```bash
dvc remote add minio-docker s3://dvc-data
dvc remote modify minio-docker endpointurl http://minio:9000
git add .dvc/config && git commit -m "Add in-container DVC remote"
```

No `--local` here and none needed — a service name and an endpoint are not secrets. Confirm
`.dvc/config` still contains no credentials before committing.

---

## Part 4 — New file: `src/mlops_pipeline/data.py`

Logic lives in the package, not in the DAG file. DAG files are hard to unit-test and get
re-parsed constantly by the dag-processor; a plain module is importable by both Airflow and
pytest. This is what the `PYTHONPATH: /opt/airflow/src` line from 1.3 was for.

```python
"""Dataset resolution, validation, and sampling for the NYC taxi pipeline."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "fare_amount",
)

MIN_EXPECTED_ROWS = 1_000_000
MAX_DROP_FRACTION = 0.005


@dataclass(frozen=True)
class ValidationReport:
    row_count: int
    column_count: int
    pickup_min: str
    pickup_max: str
    null_fraction: dict[str, float]


class DataValidationError(Exception):
    """Raised when the raw dataset fails a quality gate."""


def validate_raw(df: pd.DataFrame) -> ValidationReport:
    """Fail loudly if the raw dataset is not what downstream code assumes."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise DataValidationError(f"missing required columns: {missing}")

    if len(df) < MIN_EXPECTED_ROWS:
        raise DataValidationError(
            f"expected at least {MIN_EXPECTED_ROWS} rows, got {len(df)}"
        )

    pickup = pd.to_datetime(df["tpep_pickup_datetime"])

    return ValidationReport(
        row_count=len(df),
        column_count=df.shape[1],
        pickup_min=str(pickup.min()),
        pickup_max=str(pickup.max()),
        null_fraction={
            c: round(float(df[c].isna().mean()), 6) for c in REQUIRED_COLUMNS
        },
    )


@dataclass(frozen=True)
class CleaningReport:
    input_rows: int
    dropped_non_positive_duration: int
    output_rows: int

    @property
    def drop_fraction(self) -> float:
        return self.dropped_non_positive_duration / self.input_rows


def clean_raw(
    df: pd.DataFrame, max_drop_fraction: float = MAX_DROP_FRACTION
) -> tuple[pd.DataFrame, CleaningReport]:
    """Drop rows whose target is unusable, and refuse if too many are.

    A trip that ends before it starts, or takes zero seconds, has no usable duration
    label. Both are known defects in this dataset at a tiny rate. They are removed and
    counted — but if the rate ever jumps, that is a data incident, not dirt, and the
    run fails.
    """
    pickup = pd.to_datetime(df["tpep_pickup_datetime"])
    dropoff = pd.to_datetime(df["tpep_dropoff_datetime"])
    duration_min = (dropoff - pickup).dt.total_seconds() / 60.0

    keep = duration_min > 0
    dropped = int((~keep).sum())
    fraction = dropped / len(df)

    if fraction > max_drop_fraction:
        raise DataValidationError(
            f"dropped {dropped} of {len(df)} rows ({fraction:.4%}) for non-positive "
            f"duration, above the {max_drop_fraction:.4%} threshold"
        )

    cleaned = df.loc[keep].copy()
    cleaned["trip_duration_min"] = duration_min[keep]

    return cleaned, CleaningReport(
        input_rows=len(df),
        dropped_non_positive_duration=dropped,
        output_rows=len(cleaned),
    )


def build_reference_sample(
    df: pd.DataFrame, n_rows: int = 200_000, seed: int = 42
) -> pd.DataFrame:
    """Deterministic sample used for training and as the Phase 5 drift reference."""
    if len(df) <= n_rows:
        return df.copy()
    return df.sample(n=n_rows, random_state=seed).reset_index(drop=True)
```

Points worth understanding:

- **The validator raises rather than returns a flag.** An exception fails the task, which
  fails the DAG run, which is visible in the UI and alertable. A boolean that nobody checks
  is not a quality gate.
- **`random_state=seed`** — without it, every run produces a different reference
  distribution, and Phase 5's drift signal becomes noise about your own sampling.
- **Validation and cleaning are separate concerns, and this is the distinction worth
  taking into an interview.** Validation asks "is this structurally the data I expect?" —
  missing columns or a collapsed row count mean something broke upstream, and the run must
  stop. Cleaning asks "which individual records are unusable?" — real datasets always carry
  some dirt, and a gate that fails on 3 bad rows in 3 million is a gate that gets disabled
  within a week. `clean_raw` drops and counts; the threshold is what keeps it a gate. Three
  bad rows is dirt; 40% is an incident, and the same code catches both.
- **Where the boundary sits.** Ingestion removes records whose *target* is unusable —
  nothing else. Outlier trimming, fare filters and passenger-count rules are modelling
  decisions: they belong in Phase 2's preprocessing, where they can be logged to MLflow as
  parameters and changed per experiment. Silently cleaning them here would hide modelling
  choices inside infrastructure.
- **The sample carries `trip_duration_min`.** Phase 5 detects target drift as well as
  feature drift, and it can only do that if the reference distribution includes the target.

---

## Part 5 — New file: `tests/test_data.py`

```python
import pandas as pd
import pytest

from mlops_pipeline.data import (
    DataValidationError,
    build_reference_sample,
    clean_raw,
    validate_raw,
)


def _frame(n: int = 1_000_001) -> pd.DataFrame:
    pickup = pd.date_range("2023-01-01", periods=n, freq="s")
    return pd.DataFrame(
        {
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": pickup + pd.Timedelta(minutes=10),
            "trip_distance": 1.0,
            "PULocationID": 1,
            "DOLocationID": 2,
            "passenger_count": 1.0,
            "fare_amount": 10.0,
        }
    )


def test_validate_raw_accepts_a_good_frame():
    report = validate_raw(_frame())
    assert report.row_count > 1_000_000
    assert report.null_fraction["fare_amount"] == 0.0


def test_validate_raw_rejects_missing_columns():
    df = _frame().drop(columns=["fare_amount"])
    with pytest.raises(DataValidationError, match="missing required columns"):
        validate_raw(df)


def test_clean_raw_drops_non_positive_durations():
    df = _frame()
    df.loc[0, "tpep_dropoff_datetime"] = df.loc[0, "tpep_pickup_datetime"] - pd.Timedelta(
        minutes=5
    )
    df.loc[1, "tpep_dropoff_datetime"] = df.loc[1, "tpep_pickup_datetime"]
    cleaned, report = clean_raw(df)
    assert report.dropped_non_positive_duration == 2
    assert report.output_rows == len(df) - 2
    assert (cleaned["trip_duration_min"] > 0).all()


def test_clean_raw_fails_above_threshold():
    df = _frame(1_000)
    df["tpep_dropoff_datetime"] = df["tpep_pickup_datetime"]
    with pytest.raises(DataValidationError, match="above the"):
        clean_raw(df)


def test_reference_sample_is_deterministic():
    df = _frame(50_000)
    a = build_reference_sample(df, n_rows=1_000, seed=42)
    b = build_reference_sample(df, n_rows=1_000, seed=42)
    pd.testing.assert_frame_equal(a, b)
```

Run with `uv run pytest -q`. These are the first tests in the repo and Phase 4's CI job
will run exactly this command — a lint-and-test workflow with nothing to test is theatre.

---

## Part 6 — New file: `dags/ingest_taxi_data.py`

```python
"""Resolve the DVC-pinned raw dataset, validate it, and write the reference sample."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.sdk import dag, task

DVC_REPO = "/opt/airflow/repo"
DATA_PATH = "data/raw/yellow_tripdata_2023-01.parquet"
LANDING_URI = "s3://landing/reference/yellow_tripdata_2023-01_sample.parquet"


def _storage_options() -> dict:
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["MLFLOW_S3_ENDPOINT_URL"]},
    }


@dag(
    dag_id="ingest_taxi_data",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    tags=["phase-1", "ingestion"],
)
def ingest_taxi_data():
    @task
    def resolve_dataset() -> str:
        import dvc.api

        url = dvc.api.get_url(DATA_PATH, repo=DVC_REPO, remote="minio-docker")
        print(f"Resolved {DATA_PATH} -> {url}")
        return url

    @task
    def validate(url: str) -> dict:
        import pandas as pd

        from mlops_pipeline.data import validate_raw

        df = pd.read_parquet(url, storage_options=_storage_options())
        report = validate_raw(df)
        print(
            f"rows={report.row_count} cols={report.column_count} "
            f"pickup range {report.pickup_min} .. {report.pickup_max}"
        )
        return {"row_count": report.row_count}

    @task
    def write_reference_sample(url: str, _validation: dict) -> str:
        import pandas as pd

        from mlops_pipeline.data import build_reference_sample, clean_raw

        df = pd.read_parquet(url, storage_options=_storage_options())
        cleaned, cleaning = clean_raw(df)
        print(
            f"cleaning: dropped {cleaning.dropped_non_positive_duration} of "
            f"{cleaning.input_rows} rows ({cleaning.drop_fraction:.4%}) for "
            f"non-positive duration"
        )
        sample = build_reference_sample(cleaned)
        sample.to_parquet(
            LANDING_URI, index=False, storage_options=_storage_options()
        )
        print(f"Wrote {len(sample)} rows to {LANDING_URI}")
        return LANDING_URI

    resolved = resolve_dataset()
    validation = validate(resolved)
    write_reference_sample(resolved, validation)


ingest_taxi_data()
```

Three details:

- **Imports inside tasks, not at module top.** The dag-processor re-parses every DAG file
  on a short interval; a top-level `import pandas` pays that cost every time and slows
  parsing for every DAG in the instance. Airflow-specific imports stay at the top, heavy
  ones move inside.
- **`write_reference_sample` takes `_validation` it never uses.** That argument *is* the
  dependency edge — it forces validation to succeed before anything is written. Expressing
  order through data flow is the TaskFlow idiom; `>>` chaining would work too but this
  makes the reason explicit.
- **The URL travels through XCom**, not the dataframe. XCom is metadata storage backed by
  the Airflow database; putting a 3M-row frame through it would be a serious anti-pattern.
  Re-reading the parquet in the second task is the correct trade — object storage is cheap,
  the metadata DB is not.

---

## Part 7 — Run and verify

```bash
docker compose up -d --build
docker compose exec minio mc mb --ignore-existing local/landing
uv run pytest -q
```

Trigger `ingest_taxi_data` from the UI at :8080, then confirm the object exists:

```bash
docker compose exec minio mc ls local/landing/reference/
```

### Definition of done

- [ ] `uv run pytest -q` — five tests pass
- [ ] `uv run ruff check .` clean
- [ ] `ingest_taxi_data` appears with no import errors; `hello_stack` still present
- [ ] All three tasks succeed
- [ ] `resolve_dataset` log shows an `s3://dvc-data/files/md5/...` URL whose hash matches
      the `md5` in `data/raw/yellow_tripdata_2023-01.parquet.dvc`
- [ ] `validate` log reports ~3,066,766 rows and a January 2023 pickup range
- [ ] The sample object exists in the `landing` bucket and is ~200k rows, and includes a
      `trip_duration_min` column with no non-positive values
- [ ] `write_reference_sample` log reports the cleaning drop count and fraction — expect 3
      rows dropped for reversed timestamps plus the zero-duration trips, well under 0.5%
- [ ] Deliberate failure check: temporarily **raise** `MIN_EXPECTED_ROWS` above the real
      row count (~3,066,766), re-trigger, confirm the DAG **fails** — then revert. Do the
      same for `MAX_DROP_FRACTION` set to `0.0`, which must fail in `clean_raw`. An
      untested gate is an assumption.
- [ ] Everything from 1.2–1.5 still healthy

### If something breaks

| Symptom | Cause and fix |
| --- | --- |
| `dvc.api` errors on a read-only path | `.dvc` must be mounted writable — DVC creates `.dvc/tmp`. |
| Connection refused resolving the URL | Task used the `minio` remote (localhost) instead of `minio-docker`. |
| `NoCredentialsError` | The `minio-docker` remote has no credentials by design; check the anchor still exports `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. |
| `ModuleNotFoundError: mlops_pipeline` | `PYTHONPATH: /opt/airflow/src` missing, or `./src` not mounted. |
| NoSuchBucket on write | `landing` bucket not created — `minio-init` only runs on a fresh volume. |

---

## Part 8 — README and CLAUDE.md changes

### README — Roadmap, Phase 1 row

Mark Phase 1 complete and Phase 2 as the current one.

### README — new bullet under "Design decisions"

```markdown
- **Airflow resolves pinned data, it does not pull it.** Tasks use `dvc.api.get_url` to
  turn a committed `.dvc` pointer into an object URL and stream the parquet directly from
  storage — no second copy, no container-side cache, and the orchestrator never writes to
  the repository. A separate `minio-docker` remote exists solely because the same object
  store has a different address inside the Compose network.
```

### CLAUDE.md — Status

```markdown
## Status
Phase 1 complete: Compose stack, Airflow, AWS foundation, DVC + dataset, ingestion DAG.
Next: Phase 2 — preprocessing and training DAGs, MLflow tracking, model registry.
```

### CLAUDE.md — Conventions, add

```markdown
- DVC remotes are environment-specific: `minio` (host), `minio-docker` (containers), `aws`
  (cloud). Any code running inside a container must pass `remote="minio-docker"`
  explicitly — the default remote points at `localhost`, and the `aws` remote references a
  named AWS profile that does not exist in the image.
- Container dependency pins mirror `uv.lock` exactly. `pandas` and `cryptography` are
  filtered out of Airflow's constraints file and re-pinned by hand in
  `requirements-airflow.txt`; if either moves on the host, move it there too.
```

### CLAUDE.md — Working rules, add

```markdown
- Never pipe a build, test, or verification command through `tail`/`head` without
  `set -o pipefail` — the pipeline returns the last command's exit code and a failure
  reads as success. Check exit codes explicitly and confirm containers were actually
  recreated.
```

---

## Part 9 — Production notes

- **Validation depth.** Hand-rolled assertions are fine at this size; production uses Great
  Expectations or Pandera, with the expectation suite versioned alongside the data and a
  validation report published per run.
- **Ingestion is manual.** `schedule=None` is right while a human triggers runs. Real
  pipelines are event-driven — an S3 notification or a sensor on file arrival — and Phase 5
  adds the drift-triggered path.
- **The reference sample is a snapshot, not a contract.** When the model is retrained on
  newer data, the drift reference must move with it, or Phase 5 will keep comparing against
  a distribution nothing is trained on any more. Worth a README line: retraining updates
  the reference.
- **Idempotency.** Re-running overwrites the same landing object. Production writes a
  run-scoped path (`reference/{data_version}/…`) so a rerun cannot destroy the artifact an
  earlier model was trained against.
- **Secrets.** MinIO root credentials still reach tasks as environment variables. In
  Kubernetes this becomes a mounted Secret or, better, IRSA — a service account assuming an
  IAM role, with no static credentials anywhere.
