"""Startup model loading, alias pinning, and signature-exact dtype coercion."""

from __future__ import annotations

import logging
import socket
import threading
from contextlib import contextmanager
from dataclasses import dataclass

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient

from mlops_pipeline.registry import client as registry_client
from serving.app.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadedModel:
    """A pinned, ready-to-serve model plus the metadata /model reports."""

    pyfunc: mlflow.pyfunc.PyFuncModel
    name: str
    version: str
    run_id: str
    dtypes: dict[str, str]

    @property
    def columns(self) -> list[str]:
        return list(self.dtypes)


def resolve_version(model_uri: str, mlflow_client: MlflowClient):
    """Turn a models:/ URI into a concrete registry version.

    An alias is read once and pinned immediately -- the same rule the promotion gate
    follows, so a promotion mid-startup cannot leave the app serving one version while
    reporting another.
    """
    if not model_uri.startswith("models:/"):
        raise ValueError(f"expected a models:/ URI, got {model_uri!r}")

    reference = model_uri.removeprefix("models:/")
    if "@" in reference:
        name, alias = reference.split("@", 1)
        return mlflow_client.get_model_version_by_alias(name, alias)
    name, version = reference.rsplit("/", 1)
    return mlflow_client.get_model_version(name, version)


def signature_dtypes(pyfunc_model: mlflow.pyfunc.PyFuncModel) -> dict[str, str]:
    """Column -> numpy dtype, taken from the logged signature rather than guessed.

    The signature mixes widths (integer/int32 for the derived fields, long/int64 for
    the location ids). JSON numbers become float64 in pandas by default, which pyfunc
    schema enforcement rejects, so serving must restore the exact recorded types.
    """
    schema = pyfunc_model.metadata.get_input_schema()
    if schema is None:
        raise RuntimeError("logged model has no input schema; cannot coerce safely")
    return {col.name: np.dtype(col.type.to_numpy()).name for col in schema.inputs}


def coerce(records: list[dict], dtypes: dict[str, str]) -> pd.DataFrame:
    """Build the prediction frame in signature order with signature dtypes."""
    frame = pd.DataFrame(records, columns=list(dtypes))
    return frame.astype(dtypes)


@contextmanager
def _socket_timeout(seconds: float):
    """Bound every blocking socket read for the duration of the load.

    MLflow's presigned artifact download calls cloud_storage_http_request with
    timeout=None, so without this a stalled MinIO connection never returns.
    """
    previous = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(previous)


def _load(settings: Settings) -> LoadedModel:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    version = resolve_version(settings.model_uri, registry_client())
    pinned_uri = f"models:/{version.name}/{version.version}"
    logger.info("resolved %s -> %s", settings.model_uri, pinned_uri)

    with _socket_timeout(settings.artifact_socket_timeout_seconds):
        pyfunc_model = mlflow.pyfunc.load_model(pinned_uri)

    dtypes = signature_dtypes(pyfunc_model)
    logger.info("loaded v%s (run %s) with dtypes %s", version.version, version.run_id, dtypes)
    return LoadedModel(
        pyfunc=pyfunc_model,
        name=version.name,
        version=version.version,
        run_id=version.run_id,
        dtypes=dtypes,
    )


def load_with_timeout(settings: Settings) -> LoadedModel:
    """Load under a hard wall-clock ceiling.

    The socket timeout bounds each individual read, but MLflow retries a failed chunk
    five times with exponential backoff, per file -- so per-read bounds alone still
    multiply out to many minutes. This ceiling bounds the whole operation.

    A daemon thread rather than a ThreadPoolExecutor: the executor's context manager
    calls shutdown(wait=True) on the way out, which blocks on the very worker the
    timeout was meant to escape, silently defeating the ceiling. A stuck daemon thread
    is abandoned instead -- the pod fails its startup probe and gets replaced.
    """
    outcome: dict[str, object] = {}

    def runner() -> None:
        try:
            outcome["model"] = _load(settings)
        except BaseException as exc:  # noqa: BLE001 -- re-raised on the caller's thread
            outcome["error"] = exc

    worker = threading.Thread(target=runner, name="model-load-worker", daemon=True)
    worker.start()
    worker.join(timeout=settings.model_load_timeout_seconds)

    if worker.is_alive():
        raise TimeoutError(
            f"model load exceeded {settings.model_load_timeout_seconds}s "
            f"for {settings.model_uri}"
        )
    if "error" in outcome:
        raise outcome["error"]
    return outcome["model"]
