"""Export the champion model's signature to the committed contract file.

Runs inside the Compose network (`make export-signature`) because reading a signature
means loading the model, and MLflow's presigned artifact URLs only resolve there. That is
the whole reason the contract exists: CI cannot reach the registry, so the signature has
to be committed and checked against the pydantic schema without a network.

Deliberately writes NO provenance -- no version, no run id, no timestamp. The contract
describes the *signature*, not which version happens to be champion. Promotions here are
frequent and often numerically identical, so recording the version would produce a diff on
every promotion and bury the signal a real signature change is supposed to send. The
source version is printed to stderr instead, where a developer sees it and git does not.

JSON goes to stdout; everything else goes to stderr, so the Makefile can redirect cleanly.
"""

from __future__ import annotations

import json
import os
import socket
import sys

socket.setdefaulttimeout(float(os.environ.get("PROBE_SOCKET_TIMEOUT", "20")))

MLFLOW_TYPE_TO_NUMPY = {"integer": "int32", "long": "int64", "double": "float64"}


def export() -> dict:
    import mlflow

    from mlops_pipeline.registry import CHAMPION_ALIAS, MODEL_NAME, client
    from serving.app.model import resolve_version

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    model_uri = os.environ.get("MODEL_URI", f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
    mlflow.set_tracking_uri(tracking_uri)

    version = resolve_version(model_uri, client())
    print(f"exporting from {model_uri} -> v{version.version} (run {version.run_id})", file=sys.stderr)

    model = mlflow.pyfunc.load_model(f"models:/{version.name}/{version.version}")
    schema = model.metadata.get_input_schema()
    if schema is None:
        raise RuntimeError("logged model has no input schema; nothing to export")

    inputs = []
    for col in schema.inputs:
        mlflow_type = col.type.name
        inputs.append(
            {
                "name": col.name,
                "type": mlflow_type,
                "numpy_dtype": MLFLOW_TYPE_TO_NUMPY.get(mlflow_type, col.type.to_numpy().name),
            }
        )

    output_schema = model.metadata.get_output_schema()
    output = None
    if output_schema is not None:
        spec = output_schema.inputs[0]
        output = {"type": "tensor", "dtype": str(spec.type), "shape": list(spec.shape)}

    return {"model_name": version.name, "inputs": inputs, "output": output}


if __name__ == "__main__":
    contract = export()
    json.dump(contract, sys.stdout, indent=2)
    sys.stdout.write("\n")
    print(f"exported {len(contract['inputs'])} input columns", file=sys.stderr)
