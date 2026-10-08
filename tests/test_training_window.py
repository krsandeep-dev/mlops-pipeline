"""The gate's holdout must come from the month under test, not the baseline month.

`decide` being correct is not enough and never was: it compares two numbers, and before
Phase 5 both numbers came from the last 20% of 2023-01 no matter which month had just
arrived. A model's error on January tells you nothing about whether it has gone stale on
December, so the bug these tests guard is "correct arithmetic on the wrong rows" -- which
no test of `decide` can catch.

Pure: string/plumbing assertions only, no network, no MinIO, no MLflow.
"""

import inspect

import pandas as pd

from mlops_pipeline import promote, train
from mlops_pipeline.features import (
    PICKUP_COLUMN,
    TARGET,
    VALID_FRACTION,
    temporal_split,
)


def test_reference_uri_and_data_path_follow_the_month():
    assert train.reference_uri("2023-12").endswith("yellow_tripdata_2023-12_sample.parquet")
    assert train.data_path("2023-12") == "data/raw/yellow_tripdata_2023-12.parquet"
    # The default is still the baseline month, so pre-Phase-5 call sites are unchanged.
    assert train.reference_uri() == train.reference_uri(train.DEFAULT_MONTH)
    assert "2023-01" in train.reference_uri()


def test_load_splits_reads_the_month_it_is_given():
    """The holdout's source file is month-derived -- the whole point of the parameter."""
    source = inspect.getsource(train.load_splits)
    assert "reference_uri(month)" in source, (
        "load_splits must read the month it is passed; a hardcoded URI would send the "
        "gate back to scoring every model on 2023-01 forever"
    )


def test_promote_scores_champion_on_the_candidates_month():
    """One month flows to both training and the holdout, or the comparison is meaningless."""
    source = inspect.getsource(promote.main)
    assert "train.main(month)" in source
    assert "train.load_splits(month)" in source, (
        "champion and candidate must be scored on the same month's rows"
    )


def test_training_dag_threads_the_month_into_both_tasks():
    """train_candidate and evaluate_champion must agree on the month, from dag_run.conf."""
    dag_source = (
        __import__("pathlib").Path(__file__).parent.parent / "dags" / "train_and_promote.py"
    ).read_text()
    # Both tasks read the month from the run config, not from a module constant.
    assert dag_source.count('conf or {}).get("month"') == 2, (
        "both train_candidate and evaluate_champion must take the month from "
        "dag_run.conf, or they can disagree and score on different data"
    )
    assert "train.load_splits(month)" in dag_source
    assert "train.main(month)" in dag_source


def test_holdout_is_the_latest_rows_not_a_random_sample():
    """Temporal, so the holdout is always forward in time from what the model saw."""
    frame = pd.DataFrame({
        PICKUP_COLUMN: pd.date_range("2023-12-01", periods=100, freq="h"),
        TARGET: range(100),
    })
    train_df, valid_df = temporal_split(frame)
    assert len(valid_df) == int(100 * VALID_FRACTION)
    # Every holdout timestamp is after every training timestamp.
    assert train_df[PICKUP_COLUMN].max() < valid_df[PICKUP_COLUMN].min()


def test_drift_dag_keeps_the_threshold_and_exposes_a_runtime_override():
    """The threshold stays Evidently's 0.5; forcing is a parameter, never a code edit."""
    dag_source = (
        __import__("pathlib").Path(__file__).parent.parent / "dags" / "drift_and_retrain.py"
    ).read_text()
    assert "DRIFT_SHARE_THRESHOLD = 0.5" in dag_source
    assert '"force_retrain": False' in dag_source, "forcing must default to off"
    assert 'conf.get("force_retrain"' in dag_source, "forcing must be a runtime parameter"
