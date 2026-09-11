"""Push batch drift metrics to Pushgateway.

Batch jobs cannot be scraped -- they are gone before Prometheus looks -- so drift metrics
are pushed instead. The route is the k3d load balancer by container name with an explicit
Host header: Airflow and the cluster nodes share the Compose network, and the k3d-internal
`host.k3d.internal` does not resolve from Compose.

Series are grouped by month so a re-run of the same month replaces its own metrics rather
than accumulating. Pushgateway keeps series forever until deleted, which makes a stale
month look like a live signal -- `delete_month` exists so cleanup is deliberate rather
than hoped for.
"""

from __future__ import annotations

import os
import urllib.request

DEFAULT_URL = "http://k3d-mlops-serverlb"
DEFAULT_HOST_HEADER = "pushgateway.localhost"
JOB = "taxi_drift"


def _endpoint(month: str) -> str:
    base = os.environ.get("PUSHGATEWAY_URL", DEFAULT_URL).rstrip("/")
    # Grouping key: one slot per month, so re-running a month overwrites in place.
    return f"{base}/metrics/job/{JOB}/month/{month}"


def _headers() -> dict[str, str]:
    return {
        "Host": os.environ.get("PUSHGATEWAY_HOST_HEADER", DEFAULT_HOST_HEADER),
        "Content-Type": "text/plain",
    }


def _request(method: str, month: str, body: bytes | None = None, timeout: float = 15.0) -> int:
    req = urllib.request.Request(_endpoint(month), data=body, method=method, headers=_headers())
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.status


def render(metrics: dict[str, float]) -> bytes:
    """Prometheus text exposition. The month is a grouping label, not a metric label."""
    lines = [f"{name} {float(value)}" for name, value in sorted(metrics.items())]
    return ("\n".join(lines) + "\n").encode()


def push(month: str, metrics: dict[str, float]) -> int:
    return _request("PUT", month, render(metrics))


def delete_month(month: str) -> int:
    """Remove a month's series. Stale batch metrics are a Pushgateway hazard, not data."""
    return _request("DELETE", month)
