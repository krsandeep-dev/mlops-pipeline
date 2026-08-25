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
