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
