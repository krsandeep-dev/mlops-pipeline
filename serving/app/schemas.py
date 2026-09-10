"""Request and response models.

`TripRecord`'s fields mirror the logged model signature exactly -- same names, same
order, integer types throughout. tests/test_serving_schema.py asserts that parity
against the live signature so a training-side change cannot silently desync serving.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# Declared in signature order; the coercion step builds the frame in this order.
FEATURE_FIELDS: tuple[str, ...] = (
    "pickup_hour",
    "pickup_weekday",
    "is_weekend",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
)


class TripRecord(BaseModel):
    """One prediction request row: what is known when a ride is requested."""

    model_config = ConfigDict(extra="forbid")

    pickup_hour: int
    pickup_weekday: int
    is_weekend: int
    PULocationID: int
    DOLocationID: int
    passenger_count: int


class PredictRequest(BaseModel):
    records: list[TripRecord] = Field(min_length=1)


class ModelInfo(BaseModel):
    name: str
    version: str
    run_id: str


class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    predictions: list[float]
    model: ModelInfo
