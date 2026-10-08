"""Resolve the DVC-pinned raw dataset, validate it, and write the reference sample."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.sdk import dag, task

DVC_REPO = "/opt/airflow/repo"
DEFAULT_MONTH = "2023-01"


def data_path(month: str) -> str:
    return f"data/raw/yellow_tripdata_{month}.parquet"


def landing_uri(month: str) -> str:
    return f"s3://landing/reference/yellow_tripdata_{month}_sample.parquet"


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
    def resolve_dataset(**context) -> str:
        import dvc.api

        month = (context["dag_run"].conf or {}).get("month", DEFAULT_MONTH)
        path = data_path(month)
        url = dvc.api.get_url(path, repo=DVC_REPO, remote="minio-docker")
        print(f"month={month}  resolved {path} -> {url}")
        return url

    @task
    def validate(url: str) -> dict:
        from mlops_pipeline.data import read_raw, validate_raw

        df = read_raw(url, storage_options=_storage_options())
        report = validate_raw(df)
        print(
            f"rows={report.row_count} cols={report.column_count} "
            f"pickup range {report.pickup_min} .. {report.pickup_max}"
        )
        return {"row_count": report.row_count}

    @task
    def write_reference_sample(url: str, _validation: dict, **context) -> str:
        import gc

        from mlops_pipeline.data import build_reference_sample, clean_raw, read_raw

        df = read_raw(url, storage_options=_storage_options())
        cleaned, cleaning = clean_raw(df)
        # clean_raw holds the input and its cleaned copy at once; dropping our reference
        # lets the input be collected before the 200k sample is drawn.
        del df
        gc.collect()
        print(
            f"cleaning: dropped {cleaning.dropped_non_positive_duration} of "
            f"{cleaning.input_rows} rows ({cleaning.drop_fraction:.4%}) for "
            f"non-positive duration"
        )
        sample = build_reference_sample(cleaned)
        month = (context["dag_run"].conf or {}).get("month", DEFAULT_MONTH)
        target = landing_uri(month)
        sample.to_parquet(target, index=False, storage_options=_storage_options())
        print(f"Wrote {len(sample)} rows to {target}")
        return target

    resolved = resolve_dataset()
    validation = validate(resolved)
    write_reference_sample(resolved, validation)


ingest_taxi_data()
