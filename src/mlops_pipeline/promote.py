"""Train a candidate, evaluate it against the champion, and promote if it earns it."""

from __future__ import annotations

from mlops_pipeline import registry, train
from mlops_pipeline.features import build_features


def main() -> None:
    result = train.main()

    _, valid_df = train.load_splits()
    x_valid, y_valid, _ = build_features(valid_df)

    mlflow_client = registry.client()
    champion_version = registry.resolve_champion_version(mlflow_client)
    champion = (
        registry.score_champion(champion_version, x_valid, y_valid)
        if champion_version
        else None
    )
    if champion:
        print(f"champion v{champion.version} scored {champion.mae:.4f} MAE on this split")

    version = registry.register_candidate(
        result.model_uri,
        tags={"run_id": result.run_id, "data_url": result.data_url},
    )

    decision = registry.decide(
        candidate_mae=result.metrics["mae"],
        baseline_mae=result.baseline_metrics["mae"],
        champion=champion,
        candidate_version=version,
    )
    registry.apply(decision)

    verdict = "PROMOTED" if decision.promoted else "REJECTED"
    print(f"{verdict} v{version}: {decision.reason}")


if __name__ == "__main__":
    main()
