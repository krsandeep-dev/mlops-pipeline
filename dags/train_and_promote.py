"""Train a candidate, evaluate it against the champion, promote if it earns it.

Phase 5 triggers this DAG over the REST API when drift is detected.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from airflow.sdk import dag, task


@dag(
    dag_id="train_and_promote",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["phase-2", "training"],
)
def train_and_promote():
    @task
    def train_candidate() -> dict:
        from mlops_pipeline import train

        return asdict(train.main())

    @task(retries=0)
    def register(result: dict) -> str:
        from mlops_pipeline import registry

        return registry.register_candidate(
            result["model_uri"],
            tags={"run_id": result["run_id"], "data_url": result["data_url"]},
        )

    @task
    def evaluate_champion() -> dict | None:
        from mlops_pipeline import registry, train
        from mlops_pipeline.features import build_features

        client = registry.client()
        version = registry.resolve_champion_version(client)
        if version is None:
            print("no champion registered; candidate will bootstrap")
            return None

        _, valid_df = train.load_splits()
        x_valid, y_valid, _ = build_features(valid_df)
        snapshot = registry.score_champion(version, x_valid, y_valid)
        print(f"champion v{snapshot.version} scored {snapshot.mae:.4f} on this split")
        return {"version": snapshot.version, "mae": snapshot.mae}

    @task(retries=0)
    def decide_and_apply(result: dict, version: str, champion: dict | None) -> str:
        from mlops_pipeline import registry

        snapshot = registry.ChampionSnapshot(**champion) if champion else None
        decision = registry.decide(
            candidate_mae=result["metrics"]["mae"],
            baseline_mae=result["baseline_metrics"]["mae"],
            champion=snapshot,
            candidate_version=version,
        )
        registry.apply(decision)
        verdict = "PROMOTED" if decision.promoted else "REJECTED"
        print(f"{verdict} v{version}: {decision.reason}")
        return verdict

    result = train_candidate()
    version = register(result)
    champion = evaluate_champion()
    decide_and_apply(result, version, champion)


train_and_promote()
