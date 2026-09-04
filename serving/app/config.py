"""Serving configuration.

Every value arrives from the environment so the same image runs unchanged on the
compose network (M1) and in the cluster (M3). No credentials are baked in.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # `model_` is a protected namespace in pydantic v2; the fields below are model
    # configuration in the ML sense, not pydantic's, so the guard is switched off.
    model_config = SettingsConfigDict(protected_namespaces=(), extra="ignore")

    model_uri: str = "models:/taxi-trip-duration@champion"
    mlflow_tracking_uri: str = "http://mlflow:5000"

    # Bounds on the artifact path. MLflow's presigned download helper calls
    # cloud_storage_http_request with timeout=None, so a stalled MinIO connection
    # blocks forever by default -- observed as a 10-minute silent retry loop from
    # outside the compose network. These two caps make that impossible: the socket
    # timeout bounds each read, the load timeout bounds the whole operation.
    artifact_socket_timeout_seconds: float = 20.0
    model_load_timeout_seconds: float = 180.0


def get_settings() -> Settings:
    return Settings()
