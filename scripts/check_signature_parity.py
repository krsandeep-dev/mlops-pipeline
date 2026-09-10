"""Assert the committed signature contract still matches the live champion.

Needs the compose network -- MLflow's artifact download only resolves there -- so this
cannot run in the plain host test suite. `make signature-check` runs it inside the
network; tests/test_serving_schema.py calls the same function when
SERVING_SIGNATURE_CHECK=1 so there is one implementation, not two.

This is the second of three checks on one contract:
  1. CI, pure, every PR   -- contract vs the pydantic schema (no network)
  2. here, in-network      -- contract vs the LIVE champion signature
  3. the opt-in pytest test -- calls this same function
Check 1 alone would let the committed file drift from the registry; this closes that.
"""

from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

socket.setdefaulttimeout(float(os.environ.get("PROBE_SOCKET_TIMEOUT", "20")))

CONTRACT_PATH = Path(__file__).resolve().parent.parent / "serving" / "signature.json"


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    return json.loads(path.read_text())


def check() -> dict[str, str]:
    """Return the live signature dtypes, raising AssertionError on any drift."""
    import mlflow

    from serving.app.model import signature_dtypes
    from serving.app.schemas import FEATURE_FIELDS

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    model = mlflow.pyfunc.load_model(
        os.environ.get("MODEL_URI", "models:/taxi-trip-duration@champion")
    )
    live = signature_dtypes(model)

    contract = load_contract()
    expected = {col["name"]: col["numpy_dtype"] for col in contract["inputs"]}

    assert tuple(live) == tuple(expected), (
        f"live signature columns {tuple(live)} != contract {tuple(expected)}; "
        f"run `make export-signature` and review the diff"
    )
    for column, dtype in live.items():
        assert dtype == expected[column], (
            f"{column} is {dtype} live but {expected[column]} in the contract; "
            f"run `make export-signature` and review the diff"
        )

    # The contract is only useful if it also matches what serving actually accepts.
    assert tuple(expected) == FEATURE_FIELDS, (
        f"contract {tuple(expected)} != request schema {FEATURE_FIELDS}"
    )
    return live


if __name__ == "__main__":
    result = check()
    for column, dtype in result.items():
        print(f"  {column:16s} {dtype}")
    print("SIGNATURE PARITY OK (live == contract == request schema)")
    sys.exit(0)
