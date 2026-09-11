"""Prometheus metrics beyond the instrumentator defaults.

The defaults cover request rate, latency and errors -- the shape of the traffic. These
cover the shape of the *model*: which one is loaded, when it was loaded, and what it is
predicting. A model change has to be a visible event on the dashboard, not something you
discover by reading pod logs.

Registered on prometheus_client's default REGISTRY, which is the same one
prometheus-fastapi-instrumentator exposes at /metrics, so nothing extra is wired up.
"""

from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, Histogram

# A labelled gauge pinned to 1: the labels carry the information, the value is a constant.
# This is the standard "info metric" shape, and it lets a dashboard show the running
# version as an annotation and alert on it changing.
MODEL_INFO = Gauge(
    "model_info",
    "Currently loaded model, as labels. Always 1.",
    ["name", "version", "run_id"],
)

MODEL_LOADED_TIMESTAMP = Gauge(
    "model_loaded_timestamp_seconds",
    "Unix time at which the currently loaded model finished loading.",
)

# Incremented by the Phase 5 alias watcher. Present from the start so the dashboard panel
# and any alert rule exist before the watcher ships, rather than appearing mid-phase.
MODEL_RELOAD_TOTAL = Counter(
    "model_reload_total",
    "Model reload attempts by outcome.",
    ["result"],
)

# Predicted trip duration, not request latency. Training clips the target to [1, 120]
# minutes, so the buckets span that range -- a distribution that shifts after a retrain is
# exactly the signal this phase exists to make visible.
PREDICTION_DURATION = Histogram(
    "prediction_duration_minutes",
    "Predicted trip duration in minutes.",
    buckets=(1, 2, 5, 10, 15, 20, 30, 45, 60, 90, 120, float("inf")),
)


def record_loaded(name: str, version: str, run_id: str) -> None:
    """Publish the newly loaded model, replacing whatever was published before.

    clear() first because MODEL_INFO is labelled: without it a swap would leave the old
    version's series behind at 1, and the dashboard would show two live models forever.
    """
    MODEL_INFO.clear()
    MODEL_INFO.labels(name=name, version=version, run_id=run_id).set(1)
    MODEL_LOADED_TIMESTAMP.set(time.time())


def observe_predictions(values: list[float]) -> None:
    for value in values:
        PREDICTION_DURATION.observe(value)
