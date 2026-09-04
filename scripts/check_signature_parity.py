"""Assert the serving request schema still matches the live logged signature.

Needs the compose network -- MLflow's artifact download only resolves there -- so this
cannot run in the plain host test suite. `make signature-check` runs it inside the
network; tests/test_serving_schema.py calls the same function when
SERVING_SIGNATURE_CHECK=1 so there is one implementation, not two.
"""

from __future__ import annotations

import os
import socket
import sys

socket.setdefaulttimeout(float(os.environ.get("PROBE_SOCKET_TIMEOUT", "20")))


def check() -> dict[str, str]:
    """Return the live signature dtypes, raising AssertionError on any drift."""
    import mlflow

    from serving.app.model import signature_dtypes
    from serving.app.schemas import FEATURE_FIELDS

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    model = mlflow.pyfunc.load_model(
        os.environ.get("MODEL_URI", "models:/taxi-trip-duration@champion")
    )
    dtypes = signature_dtypes(model)

    assert tuple(dtypes) == FEATURE_FIELDS, (
        f"signature columns {tuple(dtypes)} != request schema {FEATURE_FIELDS}"
    )
    for column, dtype in dtypes.items():
        assert dtype.startswith("int"), f"{column} is {dtype}; the schema declares int"
    return dtypes


if __name__ == "__main__":
    result = check()
    for column, dtype in result.items():
        print(f"  {column:16s} {dtype}")
    print("SIGNATURE PARITY OK")
    sys.exit(0)
