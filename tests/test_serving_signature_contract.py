"""The committed signature contract must match the pydantic request schema.

This is the check CI can actually run. `m1-verify` gates on the in-network parity check,
which loads the model and therefore needs the artifact path; on a hosted runner it would
skip, which is exactly the failure the gate exists to prevent -- a contract test that
never runs. So the signature is committed to serving/signature.json and this test compares
it to the schema with no network at all.

Drift is caught from both directions: change the schema without re-exporting and this
fails; re-export without updating the schema and this fails too. The pair must move
together in one PR, which is what makes the contract diff a review signal.
"""

import json
from pathlib import Path

from serving.app.schemas import FEATURE_FIELDS, TripRecord

CONTRACT = json.loads((Path(__file__).parent.parent / "serving" / "signature.json").read_text())


def test_contract_columns_match_the_request_schema_in_order():
    """Order matters: it is the column order the coercion step builds the frame with."""
    assert tuple(col["name"] for col in CONTRACT["inputs"]) == FEATURE_FIELDS


def test_contract_columns_match_the_pydantic_fields():
    assert tuple(col["name"] for col in CONTRACT["inputs"]) == tuple(TripRecord.model_fields)


def test_every_contract_dtype_is_an_integer_type():
    """The schema declares int for every field; a float column would silently coerce."""
    for col in CONTRACT["inputs"]:
        assert col["numpy_dtype"].startswith("int"), f"{col['name']} is {col['numpy_dtype']}"


def test_contract_records_the_mixed_int_widths():
    """int32/int64 is the distinction the whole coercion step exists for, so pin it."""
    widths = {col["name"]: col["numpy_dtype"] for col in CONTRACT["inputs"]}
    assert widths["PULocationID"] == "int64"
    assert widths["DOLocationID"] == "int64"
    assert widths["pickup_hour"] == "int32"


def test_output_is_a_one_dimensional_float_tensor():
    assert CONTRACT["output"]["dtype"] == "float64"
    assert CONTRACT["output"]["shape"] == [-1]
