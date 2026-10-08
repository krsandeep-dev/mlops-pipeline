"""Measure drift for a month, decide whether to retrain, and trigger it in-Airflow.

The decision is `drift_share >= DRIFT_SHARE_THRESHOLD`, which is Evidently's own
dataset-level verdict and is left exactly where it was. Phase 5's measurements found no
month in 2023 that crosses it on these six request-time features, and no material skill
degradation either, so the threshold stays intact, tested, and visibly unfired rather
than being lowered until something trips it.

`force_retrain` in dag_run.conf is the escape hatch: a runtime parameter, so demonstrating
the loop never requires editing the policy or the code it lives in. A forced run is
labelled as forced in the logs and in the pushed metrics, so it cannot be mistaken for
drift that was actually detected.
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

DEFAULT_MONTH = "2023-01"

# Evidently's dataset-level default: a dataset is "drifted" when at least half its columns
# are. Not tuned, not fitted to an observation -- deliberately the library's own number.
DRIFT_SHARE_THRESHOLD = 0.5


@dag(
    dag_id="drift_and_retrain",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["phase-5", "drift"],
    params={"month": DEFAULT_MONTH, "force_retrain": False},
)
def drift_and_retrain():
    @task
    def measure(**context) -> dict:
        """Evidently report for the month against the CHAMPION's own training data."""
        import gc
        import os
        import tempfile

        import mlflow

        from mlops_pipeline import drift, pushgateway
        from mlops_pipeline.data import build_reference_sample, clean_raw, read_raw
        from mlops_pipeline.features import build_features
        from mlops_pipeline.registry import CHAMPION_ALIAS, MODEL_NAME, client

        conf = context["dag_run"].conf or {}
        month = conf.get("month", DEFAULT_MONTH)
        forced = bool(conf.get("force_retrain", False))

        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        mlflow.set_experiment("drift-monitoring")
        mlflow_client = client()

        ref_uri, ref_month = drift.champion_reference_uri(
            mlflow_client, MODEL_NAME, CHAMPION_ALIAS
        )
        print(f"reference: {ref_uri} (month {ref_month}) vs current month {month}")

        import pandas as pd

        ref_feat, _, _ = build_features(
            pd.read_parquet(ref_uri, storage_options=drift.storage_options())
        )

        # read_raw projects to the 7 columns anything downstream uses. Unprojected this
        # path peaks at 2,238 MiB on a 3.4M-row month and OOM-killed a measurement loop;
        # projected it peaks at 923 MiB.
        cleaned, _ = clean_raw(
            read_raw(f"/opt/airflow/repo/data/raw/yellow_tripdata_{month}.parquet")
        )
        sample = build_reference_sample(cleaned)
        del cleaned
        gc.collect()
        cur_feat, _, _ = build_features(sample)
        del sample
        gc.collect()

        report = drift.compute(ref_feat, cur_feat, month, ref_month)
        drifted_by_threshold = report.drift_share >= DRIFT_SHARE_THRESHOLD
        retrain = drifted_by_threshold or forced

        with mlflow.start_run(run_name=f"drift-{month}"):
            mlflow.set_tags({
                "month": month, "reference_month": ref_month, "reference_uri": ref_uri,
                "evidently_version": "0.7.23", "forced": str(forced),
                "decision": "retrain" if retrain else "no-retrain",
            })
            mlflow.log_metrics({
                "drift_share": report.drift_share,
                "drifted_features": report.drifted_features,
                "n_reference": report.n_reference, "n_current": report.n_current,
                "retrain_triggered": float(retrain),
                "forced": float(forced),
                **{f"drift_{k}": v for k, v in report.per_feature.items()},
            })
            with tempfile.TemporaryDirectory() as d:
                html = os.path.join(d, f"drift_{month}.html")
                drift.save_html(ref_feat, cur_feat, html)
                mlflow.log_artifact(html, artifact_path="evidently")

        pushgateway.push(month, {
            "drift_share": report.drift_share,
            "drifted_features": float(report.drifted_features),
            "retrain_triggered": float(retrain),
            "forced": float(forced),
            **{f"drift_score_{k}": v for k, v in report.per_feature.items()},
        })

        print(
            f"drift_share={report.drift_share:.4f} "
            f"({report.drifted_features}/{report.total_features} features) "
            f"threshold={DRIFT_SHARE_THRESHOLD} -> crossed={drifted_by_threshold}; "
            f"forced={forced} -> retrain={retrain}"
        )
        return {"month": month, "retrain": retrain, "forced": forced,
                "drift_share": report.drift_share,
                "threshold_crossed": drifted_by_threshold}

    @task.branch
    def decide(measurement: dict) -> str:
        """Branch on the decision. A no-retrain run is a correct outcome, not a failure."""
        if measurement["retrain"]:
            how = "forced via dag_run.conf" if measurement["forced"] else "drift threshold crossed"
            print(f"retraining on {measurement['month']} ({how})")
            return "trigger_retrain"
        print(
            f"no retrain: drift_share {measurement['drift_share']:.4f} is below "
            f"{DRIFT_SHARE_THRESHOLD} and force_retrain was not set"
        )
        return "no_retrain"

    @task
    def no_retrain() -> str:
        return "NO_RETRAIN"

    measurement = measure()
    branch = decide(measurement)

    # In-Airflow trigger: no REST credential to stabilise, no network hop out and back.
    trigger = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="train_and_promote",
        conf={"month": "{{ dag_run.conf.get('month', params.month) }}"},
        wait_for_completion=True,
        poke_interval=20,
        deferrable=False,
    )

    branch >> [trigger, no_retrain()]


drift_and_retrain()
