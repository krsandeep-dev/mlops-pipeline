"""Registration and the champion promotion gate."""

from __future__ import annotations

import os
from dataclasses import dataclass

import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

MODEL_NAME = "taxi-trip-duration"
CHAMPION_ALIAS = "champion"
MIN_RELATIVE_IMPROVEMENT = 0.01


@dataclass(frozen=True)
class ChampionSnapshot:
    """The incumbent, pinned to a concrete version at read time."""

    version: str
    mae: float


@dataclass(frozen=True)
class PromotionDecision:
    promoted: bool
    reason: str
    candidate_version: str
    candidate_mae: float
    champion_version: str | None
    champion_mae: float | None


def client() -> MlflowClient:
    return MlflowClient(
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )


def resolve_champion_version(mlflow_client: MlflowClient) -> str | None:
    """Read the alias once and pin it. None on a fresh registry."""
    try:
        return mlflow_client.get_model_version_by_alias(
            MODEL_NAME, CHAMPION_ALIAS
        ).version
    except MlflowException:
        return None


def score_champion(
    version: str, x_valid: pd.DataFrame, y_valid: pd.Series
) -> ChampionSnapshot:
    """Score the incumbent on the candidate's validation rows, by pinned version."""
    from sklearn.metrics import mean_absolute_error

    model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{version}")
    prediction = model.predict(x_valid)
    return ChampionSnapshot(
        version=version, mae=float(mean_absolute_error(y_valid, prediction))
    )


def register_candidate(model_uri: str, tags: dict[str, str]) -> str:
    """Every candidate is registered, promoted or not — this is the audit trail."""
    version = mlflow.register_model(model_uri, MODEL_NAME, tags=tags)
    return version.version


def decide(
    candidate_mae: float,
    baseline_mae: float,
    champion: ChampionSnapshot | None,
    candidate_version: str,
    min_relative_improvement: float = MIN_RELATIVE_IMPROVEMENT,
) -> PromotionDecision:
    base = {
        "candidate_version": candidate_version,
        "candidate_mae": candidate_mae,
        "champion_version": champion.version if champion else None,
        "champion_mae": champion.mae if champion else None,
    }

    if candidate_mae >= baseline_mae:
        return PromotionDecision(
            promoted=False,
            reason=f"candidate MAE {candidate_mae:.4f} does not beat the mean "
            f"baseline {baseline_mae:.4f}",
            **base,
        )

    if champion is None:
        return PromotionDecision(
            promoted=True, reason="no champion exists; bootstrapping", **base
        )

    improvement = (champion.mae - candidate_mae) / champion.mae
    if improvement >= min_relative_improvement:
        return PromotionDecision(
            promoted=True,
            reason=f"MAE improved {improvement:.2%} over champion v{champion.version}, "
            f"at or above the {min_relative_improvement:.2%} threshold",
            **base,
        )

    return PromotionDecision(
        promoted=False,
        reason=f"MAE change {improvement:.2%} against champion v{champion.version} is "
        f"below the {min_relative_improvement:.2%} threshold",
        **base,
    )


def apply(decision: PromotionDecision) -> None:
    """Record the decision on the version, and move the alias if it was earned."""
    mlflow_client = client()
    mlflow_client.set_model_version_tag(
        MODEL_NAME, decision.candidate_version, "promoted", str(decision.promoted)
    )
    mlflow_client.set_model_version_tag(
        MODEL_NAME, decision.candidate_version, "decision_reason", decision.reason
    )
    if decision.champion_version:
        mlflow_client.set_model_version_tag(
            MODEL_NAME,
            decision.candidate_version,
            "evaluated_against_version",
            decision.champion_version,
        )
    if decision.promoted:
        mlflow_client.set_registered_model_alias(
            MODEL_NAME, CHAMPION_ALIAS, decision.candidate_version
        )
