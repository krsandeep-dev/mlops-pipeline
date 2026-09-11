"""Batch drift: a candidate month against the champion's own training data.

The reference is derived from the champion, never hardcoded. The champion's MLflow run
records the sample it trained on, so after a promotion the reference moves on its own --
which is the whole point, because drift is measured against what the live model actually
learned, not against a fixed month that becomes less relevant every promotion.

Current data is cleaned and sampled with the same functions and the same seed the reference
used (`clean_raw`, `build_reference_sample`), so a difference in the statistics is a
difference in the data rather than a difference in how it was prepared.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import pandas as pd

from mlops_pipeline.features import FEATURE_COLUMNS


@dataclass(frozen=True)
class DriftReport:
    month: str
    reference_month: str
    drift_share: float
    drifted_features: int
    total_features: int
    per_feature: dict[str, float] = field(default_factory=dict)
    n_reference: int = 0
    n_current: int = 0

    @property
    def drifted(self) -> bool:
        """Evidently's own dataset-level verdict threshold."""
        return self.drift_share >= 0.5


def storage_options() -> dict:
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["MLFLOW_S3_ENDPOINT_URL"]},
    }


def champion_reference_uri(mlflow_client, model_name: str, alias: str) -> tuple[str, str]:
    """The reference sample the live champion trained on, plus its month.

    Reads it off the champion's run tags rather than assuming a month, so a promotion
    silently moves the reference. Falls back to parsing the URI when the month tag is
    absent -- runs logged before Phase 5 do not carry one.
    """
    version = mlflow_client.get_model_version_by_alias(model_name, alias)
    run = mlflow_client.get_run(version.run_id)
    uri = run.data.tags["reference_uri"]
    month = run.data.tags.get("month") or _month_from_uri(uri)
    return uri, month


def _month_from_uri(uri: str) -> str:
    """`.../yellow_tripdata_2023-01_sample.parquet` -> `2023-01`."""
    stem = uri.rsplit("/", 1)[-1]
    for part in stem.replace(".parquet", "").split("_"):
        if len(part) == 7 and part[4] == "-" and part[:4].isdigit():
            return part
    raise ValueError(f"cannot infer month from {uri!r}; tag the run with `month`")


def compute(reference: pd.DataFrame, current: pd.DataFrame, month: str,
            reference_month: str) -> DriftReport:
    """Feature drift over the model's actual inputs, nothing else.

    Restricted to FEATURE_COLUMNS on purpose: drift in a column the model never sees is
    not a reason to retrain, and including it would dilute drift_share with noise.
    """
    from evidently import DataDefinition, Dataset, Report
    from evidently.presets import DataDriftPreset

    columns = list(FEATURE_COLUMNS)
    definition = DataDefinition(numerical_columns=columns)
    ref_ds = Dataset.from_pandas(reference[columns], data_definition=definition)
    cur_ds = Dataset.from_pandas(current[columns], data_definition=definition)

    snapshot = Report(metrics=[DataDriftPreset()]).run(cur_ds, ref_ds)
    result = json.loads(snapshot.json())

    # Parsed from each metric's structured `config`, not from the rendered metric_name.
    # `metric_id` is None in this version, and matching on the display string would break
    # the moment Evidently reformats it -- which is exactly how a silent zero gets
    # reported as "no drift".
    per_feature: dict[str, float] = {}
    share = 0.0
    drifted = 0
    for metric in result.get("metrics", []):
        config = metric.get("config") or {}
        kind = str(config.get("type", "")).rsplit(":", 1)[-1]
        value = metric.get("value")
        if kind == "ValueDrift" and isinstance(value, (int, float)):
            per_feature[str(config["column"])] = float(value)
        elif kind == "DriftedColumnsCount" and isinstance(value, dict):
            share = float(value.get("share", 0.0))
            drifted = int(value.get("count", 0))

    if not per_feature:
        raise RuntimeError(
            "Evidently returned no per-column drift scores -- the report structure has "
            "changed. Refusing to report zero drift from a parser that matched nothing."
        )

    return DriftReport(
        month=month,
        reference_month=reference_month,
        drift_share=share,
        drifted_features=drifted,
        total_features=len(columns),
        per_feature=per_feature,
        n_reference=len(reference),
        n_current=len(current),
    )
