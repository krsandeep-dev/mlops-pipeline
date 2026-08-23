import pandas as pd
import pytest

from mlops_pipeline.data import (
    DataValidationError,
    build_reference_sample,
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


def test_validate_raw_rejects_reversed_timestamps():
    df = _frame()
    df.loc[0, "tpep_dropoff_datetime"] = df.loc[0, "tpep_pickup_datetime"] - pd.Timedelta(
        minutes=5
    )
    with pytest.raises(DataValidationError, match="ending before"):
        validate_raw(df)


def test_reference_sample_is_deterministic():
    df = _frame(50_000)
    a = build_reference_sample(df, n_rows=1_000, seed=42)
    b = build_reference_sample(df, n_rows=1_000, seed=42)
    pd.testing.assert_frame_equal(a, b)
