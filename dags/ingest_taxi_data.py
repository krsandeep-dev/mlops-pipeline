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

        from mlops_pipeline.data import build_reference_sample

        df = pd.read_parquet(url, storage_options=_storage_options())
        sample = build_reference_sample(df)
        sample.to_parquet(
            LANDING_URI, index=False, storage_options=_storage_options()
        )
        print(f"Wrote {len(sample)} rows to {LANDING_URI}")
        return LANDING_URI

    resolved = resolve_dataset()
    validation = validate(resolved)
    write_reference_sample(resolved, validation)


ingest_taxi_data()
