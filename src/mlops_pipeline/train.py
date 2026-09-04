"""Train the trip-duration model and record the run in MLflow."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

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


@dataclass(frozen=True)
class TrainingResult:
    run_id: str
    model_uri: str
    metrics: dict[str, float]
    baseline_metrics: dict[str, float]
    data_url: str


def load_splits() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reference sample, split temporally. Shared by training and the promotion gate."""
    reference = pd.read_parquet(REFERENCE_URI, storage_options=storage_options())
    return temporal_split(reference)


def configure_tracking() -> None:
    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )
    mlflow.set_experiment(EXPERIMENT_NAME)


def verify_pyfunc_round_trip(
    model_uri: str, x_sample: pd.DataFrame, expected: np.ndarray
) -> None:
    """Fail training if the logged model cannot be served.

    The promotion gate and Phase 3's FastAPI service both load through pyfunc, and
    neither can switch loaders. A model that only works via the flavor-specific
    loader is unservable, so catch it at log time rather than at promotion time.
    """
    actual = mlflow.pyfunc.load_model(model_uri).predict(x_sample)
    actual = np.asarray(actual, dtype=float)
    if actual.shape != expected.shape or not np.allclose(actual, expected, atol=1e-9):
        raise RuntimeError(
            f"pyfunc round-trip mismatch for {model_uri}: predictions differ from the "
            f"in-memory model (max abs diff "
            f"{np.abs(actual - expected).max() if actual.shape == expected.shape else 'shape mismatch'})"
        )


def main() -> TrainingResult:
    configure_tracking()
    train_df, valid_df = load_splits()
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
        model_info = mlflow.lightgbm.log_model(
            model,
            name="model",
            signature=infer_signature(x_valid, prediction),
            input_example=x_valid.head(5),
        )
        print(f"logged model: {model_info.model_uri}")

        sample = x_valid.head(256)
        verify_pyfunc_round_trip(
            model_info.model_uri, sample, np.asarray(model.predict(sample), dtype=float)
        )
        print(f"pyfunc round-trip verified on {len(sample)} rows")

        print(
            f"lightgbm MAE: {metrics['mae']:.3f} min "
            f"(baseline {baseline_metrics['mae']:.3f}), trained in {train_seconds:.1f}s"
        )

        return TrainingResult(
            run_id=mlflow.active_run().info.run_id,
            model_uri=model_info.model_uri,
            metrics=metrics,
            baseline_metrics=baseline_metrics,
            data_url=data_url,
        )


if __name__ == "__main__":
    print(main())
