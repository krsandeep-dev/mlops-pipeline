"""Pure tests for the drift contract -- no network, no Evidently, no cluster."""

import pytest

from mlops_pipeline.drift import DriftReport, _month_from_uri
from mlops_pipeline.pushgateway import render


def _report(share: float) -> DriftReport:
    return DriftReport(month="2023-07", reference_month="2023-01", drift_share=share,
                       drifted_features=int(share * 6), total_features=6)


def test_drifted_follows_evidently_dataset_threshold():
    """0.5 is Evidently's own dataset-level verdict, not a number invented here."""
    assert not _report(0.0).drifted
    assert not _report(0.4999).drifted
    assert _report(0.5).drifted


def test_month_is_parsed_from_a_reference_uri():
    assert _month_from_uri("s3://landing/reference/yellow_tripdata_2023-01_sample.parquet") == "2023-01"
    assert _month_from_uri("s3://landing/reference/yellow_tripdata_2023-12_sample.parquet") == "2023-12"


def test_unparseable_uri_raises_rather_than_guessing_a_month():
    """A wrong reference month silently compares against the wrong data."""
    with pytest.raises(ValueError):
        _month_from_uri("s3://landing/reference/sample.parquet")


def test_push_payload_is_sorted_prometheus_text():
    body = render({"drift_share": 0.167, "drifted_features": 1.0})
    assert body == b"drift_share 0.167\ndrifted_features 1.0\n"
