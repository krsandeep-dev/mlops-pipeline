# Phase 2 spec — steps 2.1 and 2.2: features and tracked training

Scope: the feature-engineering module, its tests, and a training script that records
everything in MLflow. Training runs on the **host** via `uv run` for now — a fast loop
beats a DAG trigger while you're iterating on a model. Step 2.3 adds the registry and the
promotion gate; 2.4 wraps it all in an Airflow DAG.

Before-blocks are **excerpts showing where to anchor an edit**, not full-file
reproductions.

---

## Part 0 — Design decisions

### The prediction contract, and why it excludes `trip_distance`

Before choosing features, state what the model is for: **at the moment a ride is
requested, how long will the trip take?** That fixes what is legal to use.

| Column | Known at request time? | Verdict |
| --- | --- | --- |
| `tpep_pickup_datetime` | yes | feature |
| `PULocationID` | yes | feature |
| `DOLocationID` | yes — the rider states a destination | feature |
| `passenger_count` | yes | feature |
| `tpep_dropoff_datetime` | no | **defines the target** |
| `trip_distance` | no — it's the *metered* distance | **leakage** |
| `fare_amount`, `total_amount`, `tip_amount`, `payment_type` | no | **leakage** |

`trip_distance` is the one that catches people. It is the strongest predictor in the
dataset and it is not available when the prediction is needed — the meter measures it
*during* the trip. Using it produces a model with an excellent validation score and no
deployment path, because in production you'd have to know the answer to compute the input.
This is the single most common way a portfolio model turns out to be worthless, and being
able to explain why you excluded your best feature is worth more in an interview than the
metric it would have bought you.

Expect a worse-looking MAE than tutorials that include it. That's the correct trade.
`LEAKY_COLUMNS` is declared explicitly in code and asserted in a test, so the exclusion is
enforced rather than remembered.

**Legitimate extension worth noting in the README:** the TLC zone lookup gives centroid
coordinates per `LocationID`, so a straight-line distance between pickup and dropoff zones
*is* computable at request time. That's a real feature; the metered distance is not.

### Temporal split, not random

Split by pickup time — earliest 80% trains, latest 20% validates. A random split lets the
model learn from Friday evening to predict Friday afternoon, which inflates the score and
tells you nothing about how it behaves on data it hasn't seen yet. Since production always
means "predict the future from the past", the validation split should mimic that.

### MAE as the promotion metric, and as the training objective

Report MAE, RMSE and R². Promote on **MAE in minutes**: it's what a rider experiences, and
it's robust to the long tail of 90-minute airport crawls that RMSE would let dominate. Then
set LightGBM's objective to `regression_l1`, which optimises MAE directly — optimising one
thing and reporting another is a quiet source of confusion.

### A baseline run, logged like any other

The first run predicts the training mean for every trip. It exists so every later number
has a reference point: a model that can't beat "always guess 14 minutes" isn't a model.
Logging it as a proper MLflow run rather than a comment means the comparison is in the
tracking server where the promotion gate in 2.3 can reach it.

### Training on the reference sample

200k rows, not 3.07M — training stays inside the 30-second budget, and, more importantly,
the model is trained on exactly the distribution Phase 5 will use as its drift reference.
If those two ever diverge, drift detection becomes noise about sampling.

---

## Part 1 — Dependencies

```bash
uv add lightgbm scikit-learn
```

### Fix the numpy/scipy parity break first

Airflow's constraints pin `numpy==2.5.1` and `scipy==1.18.0`; the host lockfile resolves
2.4.6 and 1.17.1. That divergence has been silent since 1.6 because nothing crossed the
boundary — Phase 2 changes that, since a LightGBM model serialised on the host gets loaded
in a container from 2.4 onward, and both libraries are direct LightGBM dependencies.

**Conform the host to the container, not the other way round:**

```bash
uv lock --upgrade-package numpy --upgrade-package scipy
uv run python -c "import numpy, scipy; print(numpy.__version__, scipy.__version__)"
```

If that lands on 2.5.1 and 1.18.0, parity is restored with no change to
`requirements-airflow.txt` — the constraints file already pins both — and the deviation
list stays at two packages.

If some dependency caps numpy below 2.5, that's a genuine conflict rather than a stale
lock entry. Then, and only then, pin numpy and scipy down to the host versions in
`requirements-airflow.txt` as filtered constraints alongside `pandas` and `cryptography`,
and record which package forced it in the Dockerfile comment.

**The rule this establishes, which supersedes the wording from 1.6:** Airflow's constraints
file is a tested, coherent set, and every package we filter out of it is a deviation we own
and have to maintain. So the container's constraint set is the reference and the host
conforms to it — hand-pinning in `requirements-airflow.txt` is the fallback for genuine
conflicts, not the default. `pandas` and `cryptography` are filtered because MLflow's
ceilings force it; nothing forces numpy or scipy.

Verify `uv lock --check` passes and record the resolved LightGBM and scikit-learn versions
— 2.4 will need them. Nothing changes in the Airflow image yet; training is host-side until
the DAG exists.

---

## Part 2 — New file: `src/mlops_pipeline/features.py`

```python
"""Feature engineering for the taxi trip-duration model.

The prediction contract is: given what is known when a ride is requested, estimate the
trip duration. Anything measured during or after the trip is leakage and is listed in
LEAKY_COLUMNS so the exclusion is testable rather than remembered.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

TARGET = "trip_duration_min"
PICKUP_COLUMN = "tpep_pickup_datetime"

FEATURE_COLUMNS = (
    "pickup_hour",
    "pickup_weekday",
    "is_weekend",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
)

CATEGORICAL_COLUMNS = (
    "pickup_hour",
    "pickup_weekday",
    "PULocationID",
    "DOLocationID",
)

LEAKY_COLUMNS = (
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "tip_amount",
    "tolls_amount",
    "payment_type",
)

MIN_DURATION_MIN = 1.0
MAX_DURATION_MIN = 120.0
VALID_FRACTION = 0.2


@dataclass(frozen=True)
class PreprocessingParams:
    min_duration_min: float
    max_duration_min: float
    rows_in: int
    rows_out: int
    passenger_count_imputed: int

    def as_mlflow_params(self) -> dict[str, float | int]:
        return {
            "min_duration_min": self.min_duration_min,
            "max_duration_min": self.max_duration_min,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "passenger_count_imputed": self.passenger_count_imputed,
        }


def temporal_split(
    df: pd.DataFrame, valid_fraction: float = VALID_FRACTION
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Earliest trips train, latest trips validate — never a random split."""
    ordered = df.sort_values(PICKUP_COLUMN)
    cut = int(len(ordered) * (1.0 - valid_fraction))
    return ordered.iloc[:cut].copy(), ordered.iloc[cut:].copy()


def build_features(
    df: pd.DataFrame,
    min_duration_min: float = MIN_DURATION_MIN,
    max_duration_min: float = MAX_DURATION_MIN,
) -> tuple[pd.DataFrame, pd.Series, PreprocessingParams]:
    """Trim implausible durations and derive request-time features only."""
    if TARGET not in df.columns:
        raise ValueError(f"expected the target column {TARGET!r}; run ingestion first")

    rows_in = len(df)
    frame = df.loc[
        df[TARGET].between(min_duration_min, max_duration_min, inclusive="both")
    ].copy()

    pickup = pd.to_datetime(frame[PICKUP_COLUMN])
    frame["pickup_hour"] = pickup.dt.hour
    frame["pickup_weekday"] = pickup.dt.weekday
    frame["is_weekend"] = (frame["pickup_weekday"] >= 5).astype("int8")
    imputed = int(frame["passenger_count"].isna().sum())
    frame["passenger_count"] = frame["passenger_count"].fillna(1).astype("int16")

    features = frame[list(FEATURE_COLUMNS)].copy()
    for column in CATEGORICAL_COLUMNS:
        features[column] = features[column].astype("category")

    params = PreprocessingParams(
        min_duration_min=min_duration_min,
        max_duration_min=max_duration_min,
        rows_in=rows_in,
        rows_out=len(features),
        passenger_count_imputed=imputed,
    )
    return features, frame[TARGET], params
```

Notes:

- **Duration trimming lives here, not in ingestion.** This is the boundary drawn in 1.6:
  a 3-hour trip is real data, and excluding it is a *modelling* choice that gets logged to
  MLflow as a parameter and can differ between experiments. Ingestion only removed rows
  with no usable target at all.
- **Native categoricals.** LightGBM splits on `category` dtype directly, so 260 taxi zones
  need no one-hot expansion. Label-encoding them as plain integers would imply zone 5 sits
  between zones 4 and 6, which is meaningless.
- **`build_features` is stateless.** No fitted encoder, no scaler, so calling it separately
  on train and validation cannot leak. The moment you add a target encoder or an imputer
  fitted on data, that changes — and the correct shape becomes a scikit-learn `Pipeline`
  fitted on train only. Worth knowing why you don't need one *yet*.

---

## Part 3 — New file: `tests/test_features.py`

```python
import pandas as pd
import pytest

from mlops_pipeline.features import (
    FEATURE_COLUMNS,
    LEAKY_COLUMNS,
    build_features,
    temporal_split,
)


def _frame(n: int = 1_000) -> pd.DataFrame:
    pickup = pd.date_range("2023-01-01", periods=n, freq="min")
    return pd.DataFrame(
        {
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": pickup + pd.Timedelta(minutes=10),
            "trip_distance": 2.5,
            "fare_amount": 12.0,
            "PULocationID": 100,
            "DOLocationID": 200,
            "passenger_count": 1.0,
            "trip_duration_min": 10.0,
        }
    )


def test_features_contain_no_leaky_columns():
    features, _, _ = build_features(_frame())
    assert set(features.columns) == set(FEATURE_COLUMNS)
    assert not set(features.columns) & set(LEAKY_COLUMNS)


def test_durations_outside_bounds_are_trimmed():
    df = _frame()
    df.loc[0, "trip_duration_min"] = 0.2
    df.loc[1, "trip_duration_min"] = 500.0
    features, target, params = build_features(df)
    assert params.rows_in == len(df)
    assert params.rows_out == len(df) - 2
    assert target.between(1.0, 120.0).all()
    assert len(features) == len(target)


def test_temporal_split_puts_the_future_in_validation():
    train, valid = temporal_split(_frame(), valid_fraction=0.2)
    assert len(valid) == 200
    assert train["tpep_pickup_datetime"].max() <= valid["tpep_pickup_datetime"].min()


def test_build_features_requires_the_target():
    with pytest.raises(ValueError, match="run ingestion first"):
        build_features(_frame().drop(columns=["trip_duration_min"]))
```

The first test is the important one: it turns the leakage contract from a paragraph in a
README into something CI enforces on every commit.

---

## Part 4 — New file: `src/mlops_pipeline/train.py`

```python
"""Train the trip-duration model and record the run in MLflow."""

from __future__ import annotations

import os
import time

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error

from mlops_pipeline.features import (
    CATEGORICAL_COLUMNS,
    PICKUP_COLUMN,
    build_features,
    temporal_split,
)

EXPERIMENT_NAME = "taxi-trip-duration"
REFERENCE_URI = "s3://landing/reference/yellow_tripdata_2023-01_sample.parquet"
DATA_PATH = "data/raw/yellow_tripdata_2023-01.parquet"

MODEL_PARAMS = {
    "objective": "regression_l1",
    "n_estimators": 300,
    "learning_rate": 0.1,
    "num_leaves": 63,
    "min_child_samples": 50,
    "random_state": 42,
    "n_jobs": -1,
}


def storage_options() -> dict:
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["MLFLOW_S3_ENDPOINT_URL"]},
    }


def dataset_url() -> str:
    """Content-addressed URL of the raw dataset this model derives from."""
    import dvc.api

    remote = os.environ.get("DVC_REMOTE", "minio")
    return dvc.api.get_url(DATA_PATH, repo=os.environ.get("DVC_REPO", "."), remote=remote)


def evaluate(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def main() -> None:
    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )
    mlflow.set_experiment(EXPERIMENT_NAME)

    reference = pd.read_parquet(REFERENCE_URI, storage_options=storage_options())
    train_df, valid_df = temporal_split(reference)
    x_train, y_train, prep = build_features(train_df)
    x_valid, y_valid, _ = build_features(valid_df)

    data_url = dataset_url()
    common_tags = {
        "data_url": data_url,
        "reference_uri": REFERENCE_URI,
        "split": "temporal",
        "train_pickup_range": (
            f"{train_df[PICKUP_COLUMN].min()} .. {train_df[PICKUP_COLUMN].max()}"
        ),
        "valid_pickup_range": (
            f"{valid_df[PICKUP_COLUMN].min()} .. {valid_df[PICKUP_COLUMN].max()}"
        ),
    }

    with mlflow.start_run(run_name="mean-baseline"):
        mlflow.set_tags({**common_tags, "model_type": "baseline"})
        prediction = np.full(len(y_valid), y_train.mean())
        baseline_metrics = evaluate(y_valid, prediction)
        mlflow.log_metrics(baseline_metrics)
        print(f"baseline MAE: {baseline_metrics['mae']:.3f} min")

    with mlflow.start_run(run_name="lightgbm"):
        mlflow.set_tags({**common_tags, "model_type": "lightgbm"})

        started = time.perf_counter()
        model = lgb.LGBMRegressor(**MODEL_PARAMS)
        model.fit(
            x_train,
            y_train,
            categorical_feature=list(CATEGORICAL_COLUMNS),
        )
        train_seconds = time.perf_counter() - started

        prediction = model.predict(x_valid)
        metrics = evaluate(y_valid, prediction)

        mlflow.log_params(
            {
                **MODEL_PARAMS,
                **prep.as_mlflow_params(),
                "n_train": len(x_train),
                "n_valid": len(x_valid),
                "features": ",".join(x_train.columns),
            }
        )
        mlflow.log_metrics({**metrics, "train_seconds": train_seconds})
        mlflow.lightgbm.log_model(
            model,
            name="model",
            signature=infer_signature(x_valid, prediction),
            input_example=x_valid.head(5),
        )

        print(
            f"lightgbm MAE: {metrics['mae']:.3f} min "
            f"(baseline {baseline_metrics['mae']:.3f}), trained in {train_seconds:.1f}s"
        )


if __name__ == "__main__":
    main()
```

Details that matter:

- **`name="model"`, not `artifact_path="model"`.** MLflow 3 renamed the parameter;
  `artifact_path` still works but emits a deprecation warning and belongs to the 2.x API.
- **The signature is the contract.** `infer_signature` records input column names, dtypes
  and the output shape into the model artifact. In Phase 3 the FastAPI service validates
  requests against it, and a payload missing a column fails loudly instead of silently
  predicting nonsense. It's also your leakage check made visible: open the signature in the
  UI and there should be exactly six inputs.
- **`input_example`** gives MLflow a concrete payload to store, which makes the model page
  self-documenting and gives Phase 3 a ready-made test request.
- **`data_url` as a tag** closes the reproducibility loop: git commit → code, tag → exact
  data bytes by content hash, run → metrics and model. Every MLflow run can now be traced
  to the dataset that produced it.
- **`DVC_REMOTE` from the environment** — on the host it defaults to `minio`; the DAG in
  2.4 sets `minio-docker`, per the convention from 1.6.
- **Manual logging, not `mlflow.autolog()`.** Autolog would capture most of this in one
  line, and it's the right call in a mature codebase. Doing it by hand once teaches you
  what a run actually consists of, and it keeps the parameter list meaningful rather than
  a dump of every LightGBM default.

---

## Part 5 — Run and verify

Training reads MinIO from the host, so pass the same credentials Compose uses:

```bash
set -a && source .env && set +a
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000

uv run pytest -q
uv run ruff check .
time uv run python -m mlops_pipeline.train
```

Then open <http://localhost:5001>, select the `taxi-trip-duration` experiment, and compare
the two runs.

### Definition of done

- [ ] `uv run pytest -q` — nine tests pass (five from 1.6, four new)
- [ ] Host and container agree on numpy and scipy, or the fallback pins are in
      `requirements-airflow.txt` with the blocking package named in the comment
- [ ] Both runs carry `train_pickup_range` and `valid_pickup_range` tags, so the split
      boundary is visible rather than inferred
- [ ] `uv run ruff check .` clean
- [ ] Training completes in under 30 seconds wall clock
- [ ] Both runs appear in the `taxi-trip-duration` experiment with MAE, RMSE and R²
- [ ] The LightGBM MAE is meaningfully better than the baseline MAE — if it isn't, stop and
      investigate rather than proceeding
- [ ] The model artifact has a signature listing **exactly six inputs**, none of them in
      `LEAKY_COLUMNS`
- [ ] The run's `data_url` tag matches the md5 in
      `data/raw/yellow_tripdata_2023-01.parquet.dvc`
- [ ] The model artifact is browsable in MinIO under `mlflow-artifacts`
- [ ] Phase 1 still healthy — `docker compose ps`, and `ingest_taxi_data` still runs green

---

## Part 6 — README and CLAUDE.md changes

### README — new bullet under "Design decisions"

```markdown
- **The model excludes `trip_distance`.** It is the metered distance, known only after the
  trip, so using it to predict trip duration is leakage: a strong validation score with no
  deployment path. Features are restricted to what is known when a ride is requested, and a
  unit test enforces the exclusion list on every commit. Validation uses a temporal split
  for the same reason — production always means predicting forward.
```

### CLAUDE.md — Status

```markdown
## Status
Phase 2 in progress: 2.1 features and 2.2 tracked training complete.
Next: 2.3 model registry and promotion gate, then 2.4 the training DAG.
```

### CLAUDE.md — Conventions, add

```markdown
- Model features are request-time only. `LEAKY_COLUMNS` in `features.py` is the contract;
  never add a column to the feature set without checking it is knowable at prediction time.
- Promotion metric is MAE in minutes, and LightGBM's objective matches it
  (`regression_l1`).
- Dependency parity direction (supersedes the 1.6 wording): Airflow's constraints file is
  the reference and the host conforms to it. Hand-pinning in `requirements-airflow.txt` is
  the fallback for genuine conflicts only — currently `pandas` and `cryptography`, both
  forced by MLflow's ceilings. Every filtered package is a deviation we own.
```

---

## Part 7 — Production notes

- **Implausible timestamps survive into training.** The reference sample's pickup range
  runs from 2008 to February 2023 — a handful of rows carry impossible dates. They are
  harmless at this volume (they sort to the front of the temporal split and the model
  ignores them), and they aren't removed because a 2008 pickup with a valid duration still
  has a usable target: filtering it is a date-range *expectation*, which belongs in the
  ingestion quality gate. Production expresses it as a Great Expectations rule per file,
  parameterised by the month the file claims to cover.
- **MLflow records the training environment.** The logged model carries a
  `requirements.txt` capturing the exact library versions it was trained under. Phase 3's
  serving image should install from that rather than from a hand-written list — it's the
  mechanism that makes artifact-crossing safe, and the reason the parity work above is
  belt-and-braces rather than the only defence.
- **Training/serving skew.** Features are computed here and will be recomputed in the
  FastAPI service in Phase 3. Two implementations of the same logic drift apart — which is
  what a feature store (Feast) exists to prevent. Our mitigation is cheaper and worth
  stating: serving imports `build_features` from the same package, and the MLflow signature
  fails the request if the columns don't match.
- **Hyperparameters are hand-picked.** Production tunes with Optuna, logging each trial as
  a nested MLflow run under a parent. Deliberately skipped: it would add search time to a
  pipeline whose point is the automation around the model.
- **No dataset lineage object.** `mlflow.data` can log the dataset itself as a first-class
  entity rather than a tag. The tag is enough here; the API is worth knowing exists.
- **Single validation split.** Cross-validation gives a variance estimate a single split
  can't. With a temporal problem the honest version is rolling-origin
  (`TimeSeriesSplit`), not k-fold.
- **No model card.** Production models ship documented intended use, training data,
  limitations and known failure modes. A short `docs/model-card.md` is a cheap and unusually
  visible addition to a portfolio.
