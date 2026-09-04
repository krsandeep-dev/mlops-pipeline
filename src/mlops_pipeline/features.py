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

# Declared to LightGBM at fit time via `categorical_feature`, NOT cast to pandas
# `category` dtype. A category column makes infer_signature record the field as
# `long` while the booster stores a pandas_categorical mapping and demands the
# category dtype back at predict time -- so the logged signature describes a frame
# the model cannot consume, and pyfunc (the path serving uses) fails both ways:
# category input is rejected by signature enforcement, and an int cast is rejected
# by LightGBM. Integer columns plus a fit-time declaration keep the categorical
# splits and make both loaders agree.
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

    params = PreprocessingParams(
        min_duration_min=min_duration_min,
        max_duration_min=max_duration_min,
        rows_in=rows_in,
        rows_out=len(features),
        passenger_count_imputed=imputed,
    )
    return features, frame[TARGET], params
