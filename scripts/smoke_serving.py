"""HTTP contract checks against a running serving instance.

Used by the M1/M3/M4 make targets. Asserts the things a green pod does not:
predictions are plausible minute-scale durations, and /model reports the version the
registry alias actually points at.
"""

from __future__ import annotations

import argparse
import sys

import httpx

SAMPLE = {
    "pickup_hour": 8,
    "pickup_weekday": 1,
    "is_weekend": 0,
    "PULocationID": 138,
    "DOLocationID": 236,
    "passenger_count": 1,
}

# The model predicts trip duration in minutes and training clipped the target to
# [1, 120]; anything outside a slightly wider band means the frame reached the booster
# wrong (usually a dtype or column-order fault), not that traffic was unusual.
MIN_PLAUSIBLE_MIN = 0.5
MAX_PLAUSIBLE_MIN = 180.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="base URL of the serving instance")
    parser.add_argument("--host", default=None, help="Host header (ingress routing)")
    parser.add_argument("--expect-version", default=None, help="version /model must report")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    headers = {"Host": args.host} if args.host else {}
    client = httpx.Client(base_url=args.url, headers=headers, timeout=args.timeout)

    live = client.get("/health/live")
    assert live.status_code == 200, f"/health/live -> {live.status_code} {live.text}"
    print(f"/health/live      200 {live.json()}")

    ready = client.get("/health/ready")
    assert ready.status_code == 200, f"/health/ready -> {ready.status_code} {ready.text}"
    print(f"/health/ready     200 {ready.json()}")

    info = client.get("/model")
    assert info.status_code == 200, f"/model -> {info.status_code} {info.text}"
    model = info.json()
    print(f"/model            200 {model}")
    if args.expect_version and model["version"] != args.expect_version:
        print(
            f"FAIL: /model reports v{model['version']}, expected v{args.expect_version}",
            file=sys.stderr,
        )
        return 1

    batch = client.post("/predict", json={"records": [SAMPLE, SAMPLE]})
    assert batch.status_code == 200, f"/predict -> {batch.status_code} {batch.text}"
    payload = batch.json()
    predictions = payload["predictions"]
    print(f"/predict          200 {predictions} (model v{payload['model']['version']})")

    assert len(predictions) == 2, f"expected 2 predictions, got {len(predictions)}"
    for value in predictions:
        if not MIN_PLAUSIBLE_MIN <= value <= MAX_PLAUSIBLE_MIN:
            print(
                f"FAIL: {value:.3f} min is outside the plausible band "
                f"[{MIN_PLAUSIBLE_MIN}, {MAX_PLAUSIBLE_MIN}]",
                file=sys.stderr,
            )
            return 1

    rejected = client.post("/predict", json={"records": [{**SAMPLE, "trip_distance": 4.2}]})
    assert rejected.status_code == 422, (
        f"an unknown field must be rejected with 422, got {rejected.status_code}"
    )
    print("/predict (extra)  422 unknown field rejected")

    metrics = client.get("/metrics")
    assert metrics.status_code == 200, f"/metrics -> {metrics.status_code}"
    print("/metrics          200")

    print("\nSMOKE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
