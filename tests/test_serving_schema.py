"""Serving's request schema must not drift from the training feature contract.

Two levels of guard:

* Offline (always runs) -- the pydantic fields are checked against
  `features.FEATURE_COLUMNS`, the training-side source of truth. Adding a feature there
  without updating serving fails here immediately, with no network and no cluster.
* In-network (opt-in via SERVING_SIGNATURE_CHECK=1) -- the same fields are checked
  against the live logged signature, including dtypes. This one needs the compose
  network, because MLflow's artifact download only resolves there.
"""

import os

import pytest

from mlops_pipeline.features import FEATURE_COLUMNS
from serving.app.schemas import FEATURE_FIELDS, TripRecord


def test_declared_field_order_matches_the_pydantic_model():
    """FEATURE_FIELDS drives coercion order, so it must track the model itself."""
    assert tuple(TripRecord.model_fields) == FEATURE_FIELDS


def test_request_schema_matches_the_training_feature_contract():
    """The exact columns training builds are the exact fields serving accepts."""
    assert tuple(TripRecord.model_fields) == FEATURE_COLUMNS


def test_every_request_field_is_an_integer():
    """The signature records integer/long throughout; a float field would silently
    widen the frame and trip schema enforcement at predict time."""
    for name, field in TripRecord.model_fields.items():
        assert field.annotation is int, f"{name} must be int, got {field.annotation}"


def test_unknown_fields_are_rejected():
    """extra='forbid' turns a typo or a stale client into a 422 rather than a
    silently ignored column."""
    with pytest.raises(ValueError):
        TripRecord(
            pickup_hour=8,
            pickup_weekday=1,
            is_weekend=0,
            PULocationID=138,
            DOLocationID=236,
            passenger_count=1,
            trip_distance=4.2,
        )


@pytest.mark.skipif(
    os.environ.get("SERVING_SIGNATURE_CHECK") != "1",
    reason="needs the compose network; run via `make signature-check`",
)
def test_request_schema_matches_the_live_signature():
    """Same assertion `make signature-check` runs in-network -- one implementation."""
    from scripts.check_signature_parity import check

    dtypes = check()
    assert tuple(dtypes) == FEATURE_FIELDS
