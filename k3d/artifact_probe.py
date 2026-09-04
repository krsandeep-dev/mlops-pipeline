"""M2 gate: prove the network mechanism from inside the cluster, before any app exists.

Two things must hold for serving to be possible at all:
  1. the MLflow tracking API answers, and
  2. a real artifact download of the actual champion model completes.

(2) is the one that fails when the network is wrong, because MLflow hands back a
presigned URL pointing at http://minio:9000 and the client must reach that host itself.

Everything is bounded: without a socket timeout MLflow's presigned download blocks
indefinitely (cloud_storage_http_request passes timeout=None), which is exactly the
silent 10-minute stall this probe exists to make impossible.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path

SOCKET_TIMEOUT_SECONDS = float(os.environ.get("PROBE_SOCKET_TIMEOUT", "20"))
socket.setdefaulttimeout(SOCKET_TIMEOUT_SECONDS)

import mlflow  # placed after setdefaulttimeout so its sessions inherit it
from mlflow.tracking import MlflowClient

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI = os.environ.get("MODEL_URI", "models:/taxi-trip-duration@champion")


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print(f"socket timeout   : {SOCKET_TIMEOUT_SECONDS}s")
    print(f"tracking uri     : {TRACKING_URI}")
    print(f"model uri        : {MODEL_URI}")

    print("\n[1/3] DNS resolution")
    for host in ("mlflow", "minio"):
        try:
            print(f"  {host:8s} -> {socket.gethostbyname(host)}")
        except OSError as exc:
            fail(f"cannot resolve {host!r}: {exc}")

    print("\n[2/3] tracking API")
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(tracking_uri=TRACKING_URI)
    started = time.perf_counter()
    try:
        name, alias = MODEL_URI.removeprefix("models:/").split("@", 1)
        version = client.get_model_version_by_alias(name, alias)
    except Exception as exc:  # noqa: BLE001 -- a probe reports any failure verbatim
        fail(f"tracking API call failed: {type(exc).__name__}: {exc}")
    print(
        f"  resolved {MODEL_URI} -> v{version.version} (run {version.run_id}) "
        f"in {time.perf_counter() - started:.2f}s"
    )

    print("\n[3/3] real artifact download")
    started = time.perf_counter()
    try:
        local = mlflow.artifacts.download_artifacts(
            artifact_uri=f"models:/{name}/{version.version}", dst_path="/tmp/probe"
        )
    except Exception as exc:  # noqa: BLE001 -- a probe reports any failure verbatim
        fail(f"artifact download failed: {type(exc).__name__}: {exc}")
    elapsed = time.perf_counter() - started

    files = sorted(p for p in Path(local).rglob("*") if p.is_file())
    total = sum(p.stat().st_size for p in files)
    for path in files:
        print(f"  {path.stat().st_size:>10,d}  {path.relative_to(local)}")
    print(f"\n  {len(files)} files, {total:,d} bytes in {elapsed:.2f}s")

    if not any(p.name == "MLmodel" for p in files):
        fail("downloaded tree has no MLmodel file; this is not a real model artifact")

    print("\nM2 PROBE PASSED")


if __name__ == "__main__":
    main()
