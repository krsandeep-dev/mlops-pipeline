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
    dropoff = pd.to_datetime(df["tpep_dropoff_datetime"])
    if (dropoff < pickup).any():
        raise DataValidationError("found trips ending before they started")

    return ValidationReport(
        row_count=len(df),
        column_count=df.shape[1],
        pickup_min=str(pickup.min()),
        pickup_max=str(pickup.max()),
        null_fraction={
            c: round(float(df[c].isna().mean()), 6) for c in REQUIRED_COLUMNS
        },
    )


def build_reference_sample(
    df: pd.DataFrame, n_rows: int = 200_000, seed: int = 42
) -> pd.DataFrame:
    """Deterministic sample used for training and as the Phase 5 drift reference."""
    if len(df) <= n_rows:
        return df.copy()
    return df.sample(n=n_rows, random_state=seed).reset_index(drop=True)
