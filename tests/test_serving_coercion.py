"""The coercion step is what stops pyfunc schema enforcement from rejecting a frame.

JSON numbers land in pandas as float64. The logged signature mixes widths -- int32 for
the derived fields, int64 for the location ids -- so serving must restore each column's
exact recorded type before predict.
"""

import pandas as pd

from serving.app.model import coerce

DTYPES = {
    "pickup_hour": "int32",
    "pickup_weekday": "int32",
    "is_weekend": "int32",
    "PULocationID": "int64",
    "DOLocationID": "int64",
    "passenger_count": "int32",
}

RECORD = {
    "pickup_hour": 8,
    "pickup_weekday": 1,
    "is_weekend": 0,
    "PULocationID": 138,
    "DOLocationID": 236,
    "passenger_count": 1,
}


def test_restores_every_signature_dtype():
    frame = coerce([RECORD], DTYPES)
    assert {c: str(d) for c, d in frame.dtypes.items()} == DTYPES


def test_preserves_the_int32_int64_split():
    """The two location ids are int64 and the derived fields int32 -- not interchangeable."""
    frame = coerce([RECORD], DTYPES)
    assert str(frame["PULocationID"].dtype) == "int64"
    assert str(frame["pickup_hour"].dtype) == "int32"


def test_coerces_json_floats_back_to_integers():
    """A JSON body parsed straight into pandas gives float64; coercion must undo that."""
    as_floats = {key: float(value) for key, value in RECORD.items()}
    assert str(pd.DataFrame([as_floats]).dtypes.iloc[0]) == "float64"

    frame = coerce([as_floats], DTYPES)
    assert {c: str(d) for c, d in frame.dtypes.items()} == DTYPES
    assert frame.loc[0, "PULocationID"] == 138


def test_orders_columns_by_signature_not_payload():
    """Signature order is the contract; a reordered payload must not reorder the frame."""
    reversed_record = dict(reversed(list(RECORD.items())))
    frame = coerce([reversed_record], DTYPES)
    assert list(frame.columns) == list(DTYPES)


def test_handles_a_batch():
    frame = coerce([RECORD, RECORD, RECORD], DTYPES)
    assert len(frame) == 3
    assert {c: str(d) for c, d in frame.dtypes.items()} == DTYPES
