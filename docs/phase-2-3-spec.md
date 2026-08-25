# Phase 2 spec — step 2.3: registry and promotion gate

Scope: register every trained candidate, compare it against the live champion on identical
validation data, and move the `champion` alias only when it earns it. Step 2.4 wraps
training and promotion into an Airflow DAG.

Before-blocks are **excerpts showing where to anchor an edit**, not full-file
reproductions.

---

## Part 0 — Design decisions

### Aliases, not stages

MLflow's `Staging`/`Production` stages are gone in 3.x. The replacement is **aliases** —
mutable named pointers to a specific version — plus version tags for metadata. So
`champion` is an alias that moves; version 7 is version 7 forever.

Knowing this is a currency signal: most tutorials still call
`transition_model_version_stage`, which no longer exists. If an interviewer asks how you
promote a model and you answer "set the Production stage", you're describing MLflow 2.

Convention here: `@champion` is what serving loads. `@challenger` is reserved for Phase 3,
when shadow traffic becomes possible.

### Alias or version in the gate — the answer is both, in order

The question is sharp and the answer isn't "pick one":

1. **Read by alias.** `@champion` means "whatever is live right now", which is exactly what
   a candidate must beat. Reading a hardcoded version number would compare against
   something that may have been superseded weeks ago.
2. **Immediately resolve it to a concrete version, and log that number.** From that point
   the run works with `models:/taxi-trip-duration/<version>`.

You don't avoid the moving target — you pin it at read time and record what you saw. The
resulting decision is then reproducible: "candidate v8 beat champion v5 by 4.2% on the
2023-01-26 split" is auditable, whereas "candidate beat champion" is not. If someone
promotes a different model mid-run, the log shows which incumbent was actually measured.

### Re-evaluate the champion; never compare against its stored metric

This is the most important decision in the step. It is tempting to read the champion's MAE
from the run that produced it and compare numbers. **Don't.** Those two numbers were
computed on different validation sets, and after Phase 5 retrains on a later month they
won't even be the same distribution. Comparing them measures the data as much as the model.

The gate loads the champion and scores it on the *candidate's* validation split, so both
numbers come from the same rows. Costs a couple of seconds at this size, and removes an
entire class of silently wrong promotion.

### Register always, promote conditionally

Every trained model becomes a registered version, including rejected ones. Registration is
the audit trail — what was tried, when, from which data — and rejection is a tagged fact
rather than a lost artifact. Promotion is the separate act of moving the alias.

### A margin, not a strict inequality

Promote only when the candidate improves MAE by at least 1% relative. Retraining on nearly
identical data produces noise-level differences in both directions; a bare `<` comparison
would swap the champion on every run, churning what production loads for no benefit. The
threshold is a parameter, logged with the decision.

Second guardrail: the candidate must also beat the mean baseline. A pipeline that
faithfully promotes a broken model is worse than one that fails.

---

## Part 0b — Where artifact-touching code runs (do this first)

MLflow 3's server answers download requests with a **presigned URL** minted from its own
`MLFLOW_S3_ENDPOINT_URL`, so the client is told to fetch from `http://minio:9000` — correct
inside the Compose network, unresolvable from the host. Uploads still stream through the
server; downloads do not.

### The flag is not the fix

`MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD` is worth ten minutes of testing because knowing
its real semantics is useful, but read the name carefully before adopting it: in MLflow,
"proxy multipart" *is* the presigned-URL flow — the server proxies the *negotiation* and
the client transfers directly. Enabling it is at least as likely to entrench the behaviour
as to reverse it. Test both settings and record what actually happens, rather than
reasoning from the name in either direction.

Even if it does force byte-proxying, it's the wrong fix here: it would degrade artifact
transfer for *every* client — including Phase 3's serving container, which reaches
`minio:9000` perfectly well — in order to solve a problem that only exists on the host.

### The actual fix: stop running artifact-touching code on the host

This is the same failure for the third time — `mlflow:5000` vs `localhost:5001` in 1.3,
`minio` vs `minio-docker` in 1.6, and now a presigned URL minted with an internal hostname.
The first two dodges were cheap. This one isn't, and the root cause is that half the code
runs somewhere with different DNS.

The host-side fast loop was a choice made in 2.2 for iteration speed, and it has now cost
more than it saved. Run training and promotion **inside the Compose network**, where every
hostname already resolves:

1. Pull 2.4's dependency work forward — add the resolved `lightgbm` and `scikit-learn`
   versions to `docker/airflow/requirements-airflow.txt` and rebuild. Check the 3.11
   constraints file first: if it pins either, adopt that pin rather than filtering.
2. Run through a container on the network:

```bash
docker compose run --rm \
  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
  -e DVC_REMOTE=minio-docker \
  -e DVC_REPO=/opt/airflow/repo \
  airflow-scheduler python -m mlops_pipeline.promote
```

`src/` is bind-mounted with `PYTHONPATH` already set, so there's no rebuild between code
edits — the loop stays fast.

Keep `pytest` and `ruff` on the host. They touch no artifacts, and that's where the fast
feedback actually matters.

If you later need host-side artifact access for debugging, `127.0.0.1 minio` in
`/etc/hosts` is a legitimate local escape hatch — but keep it out of the repo, since it
won't travel to CI or to anyone else's machine.

### Correct the README claim

Since 1.2 the README has said clients never hold storage credentials because the server
brokers all artifact traffic. That is true for uploads and false for downloads in MLflow 3.
Fix it in Part 6 rather than leaving a claim the code contradicts — a README that
overstates your security posture is worse than one that admits the split.

---

## Part 1 — Change: `src/mlops_pipeline/train.py`

Extract what the promotion gate needs to reuse. The training logic is unchanged — this is
a refactor so 2.3 and 2.4 can call the pieces.

### Before

```python
def main() -> None:
    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )
    mlflow.set_experiment(EXPERIMENT_NAME)

    reference = pd.read_parquet(REFERENCE_URI, storage_options=storage_options())
    train_df, valid_df = temporal_split(reference)
```

### After

```python
def load_splits() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reference sample, split temporally. Shared by training and the promotion gate."""
    reference = pd.read_parquet(REFERENCE_URI, storage_options=storage_options())
    return temporal_split(reference)


def configure_tracking() -> None:
    mlflow.set_tracking_uri(
        os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )
    mlflow.set_experiment(EXPERIMENT_NAME)


def main() -> None:
    configure_tracking()
    train_df, valid_df = load_splits()
```

Also capture the logged model's URI — the gate needs it. In the LightGBM run block:

### Before

```python
        mlflow.lightgbm.log_model(
            model,
            name="model",
            signature=infer_signature(x_valid, prediction),
            input_example=x_valid.head(5),
        )
```

### After

```python
        model_info = mlflow.lightgbm.log_model(
            model,
            name="model",
            signature=infer_signature(x_valid, prediction),
            input_example=x_valid.head(5),
        )
        print(f"logged model: {model_info.model_uri}")
```

Have `main()` return the pieces the DAG will need:

```python
@dataclass(frozen=True)
class TrainingResult:
    run_id: str
    model_uri: str
    metrics: dict[str, float]
    baseline_metrics: dict[str, float]
    data_url: str
```

`main()` returns a `TrainingResult`; the `__main__` block prints it and ignores the value.

Capture the run id from the context manager rather than reaching for
`mlflow.active_run()` after the block — the run is closed by then:

```python
    with mlflow.start_run(run_name="lightgbm") as run:
        ...
        run_id = run.info.run_id
```

---

## Part 2 — New file: `src/mlops_pipeline/registry.py`

```python
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


def get_client() -> MlflowClient:
    return MlflowClient(
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
    )


def resolve_champion_version(client: MlflowClient) -> str | None:
    """Read the alias once and pin it. None on a fresh registry."""
    try:
        return client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS).version
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
    client = get_client()
    client.set_model_version_tag(
        MODEL_NAME, decision.candidate_version, "promoted", str(decision.promoted)
    )
    client.set_model_version_tag(
        MODEL_NAME, decision.candidate_version, "decision_reason", decision.reason
    )
    if decision.champion_version:
        client.set_model_version_tag(
            MODEL_NAME,
            decision.candidate_version,
            "evaluated_against_version",
            decision.champion_version,
        )
    if decision.promoted:
        client.set_registered_model_alias(
            MODEL_NAME, CHAMPION_ALIAS, decision.candidate_version
        )
```

Notes:

- **`mlflow.pyfunc.load_model`, not the LightGBM flavor.** Phase 3's FastAPI service loads
  through pyfunc, so the gate exercises the same path serving will. If signature
  enforcement coerces the `category` dtypes and predictions shift, that is a real finding
  to fix now — not to route around by switching loaders, because serving can't switch.
- **`decide` is pure.** No client, no I/O, just numbers in and a decision out — which makes
  the promotion policy unit-testable without a running MLflow server. Policy logic that can
  only be tested by promoting something never gets tested.
- **The reason string is stored on the version.** Six months later, "why is v5 still
  champion?" has an answer in the registry rather than in a log file that rotated away.

---

## Part 3 — New file: `src/mlops_pipeline/promote.py`

```python
"""Train a candidate, evaluate it against the champion, and promote if it earns it."""

from __future__ import annotations

from mlops_pipeline import registry, train
from mlops_pipeline.features import build_features


def main() -> None:
    result = train.main()

    _, valid_df = train.load_splits()
    x_valid, y_valid, _ = build_features(valid_df)

    client = registry.get_client()
    champion_version = registry.resolve_champion_version(client)
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
```

---

## Part 4 — New file: `tests/test_registry.py`

```python
from mlops_pipeline.registry import ChampionSnapshot, decide


def _decide(candidate_mae, champion=None, baseline_mae=7.8):
    return decide(
        candidate_mae=candidate_mae,
        baseline_mae=baseline_mae,
        champion=champion,
        candidate_version="2",
    )


def test_bootstraps_when_no_champion_exists():
    decision = _decide(3.5)
    assert decision.promoted
    assert "bootstrapping" in decision.reason


def test_rejects_a_candidate_that_loses_to_the_baseline():
    decision = _decide(9.0)
    assert not decision.promoted
    assert "baseline" in decision.reason


def test_promotes_on_a_clear_improvement():
    decision = _decide(3.0, champion=ChampionSnapshot(version="1", mae=3.5))
    assert decision.promoted


def test_rejects_a_marginal_improvement():
    decision = _decide(3.48, champion=ChampionSnapshot(version="1", mae=3.5))
    assert not decision.promoted
    assert "below the" in decision.reason


def test_rejects_a_regression():
    decision = _decide(4.0, champion=ChampionSnapshot(version="1", mae=3.5))
    assert not decision.promoted
```

Five tests, no MLflow server required — that's the payoff of keeping `decide` pure.

---

## Part 5 — Run and verify

Tests and linting stay on the host — they touch no artifacts:

```bash
uv run pytest -q
uv run ruff check .
```

Training and promotion run inside the Compose network, per Part 0b:

```bash
PROMOTE="docker compose run --rm \
  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
  -e DVC_REMOTE=minio-docker \
  -e DVC_REPO=/opt/airflow/repo \
  airflow-scheduler python -m mlops_pipeline.promote"

$PROMOTE      # first run: bootstraps
$PROMOTE      # second run: must NOT promote
```

The second run is the actual test of the gate. Same data, same seed, so the candidate ties
the champion — a correct gate rejects it. If it promotes, the threshold logic is wrong, and
you'd have found out in Phase 5 instead, where retraining runs unattended.

### Definition of done

- [ ] `uv run pytest -q` — fourteen tests pass (nine existing, five new)
- [ ] `uv run ruff check .` clean
- [ ] First run: v1 registered, promoted, reason mentions bootstrapping
- [ ] `@champion` alias visible on v1 in the MLflow Models UI
- [ ] Second run: v2 registered, **rejected**, reason cites the threshold; `@champion`
      still points at v1
- [ ] v2 carries `promoted`, `decision_reason` and `evaluated_against_version` tags
- [ ] The champion was scored on this run's validation rows — the log shows a champion MAE
      computed now, not copied from v1's original run
- [ ] `mlflow.pyfunc.load_model("models:/taxi-trip-duration/1")` returns a working model
      from inside the network, and its predictions on `x_valid` match
      `mlflow.lightgbm.load_model` exactly — if signature enforcement coerces the four
      `category` columns and predictions shift, that is fixed here, not deferred
- [ ] `MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD` semantics recorded — what each setting
      actually did, not what the name implies
- [ ] README's server-brokered-artifacts claim corrected: uploads proxied, downloads
      presigned
- [ ] Rollback works: `client.set_registered_model_alias(MODEL_NAME, "champion", "1")`
      moves the alias back, and nothing else needs to change
- [ ] Phase 1 still healthy; `ingest_taxi_data` still green

---

## Part 6 — README and CLAUDE.md changes

### README — new bullet under "Design decisions"

```markdown
- **The promotion gate re-scores the incumbent.** A candidate is compared against the live
  champion on the candidate's own validation rows, not against the champion's stored
  metric — those were computed on different data and stop being comparable the moment
  retraining moves to a new month. The champion is read by alias and immediately pinned to
  a version so the decision is auditable, and promotion requires a 1% relative MAE
  improvement so noise doesn't churn what production serves.
```

### CLAUDE.md — Conventions, add

```markdown
- Registry: registered model `taxi-trip-duration`, alias `@champion` is what serving loads.
  MLflow stages do not exist in 3.x — use aliases. Load models by `models:/` URIs;
  `runs:/<id>/model` does not resolve for MLflow 3 logged-model entities.
- Register every candidate; promote conditionally. Rejection is a tagged version, not a
  discarded artifact.
```

### CLAUDE.md — Status

```markdown
## Status
Phase 2: 2.1 features, 2.2 tracked training, 2.2b interpreter parity, 2.3 registry and
promotion gate complete. Next: 2.4 the training DAG.
```

---

## Part 7 — Production notes

- **The gate is offline-only.** A better validation MAE does not prove a better production
  model. The full ladder is offline gate → shadow deployment (champion serves, challenger
  predicts in parallel and is measured) → canary on a traffic slice → full promotion. The
  `@challenger` alias exists for exactly that, and Phase 3 makes it reachable.
- **One metric hides failures.** Aggregate MAE can improve while long trips or a specific
  borough get worse. Production gates on segment metrics — by hour, by pickup zone, by
  duration band — and blocks promotion on any severe regression even when the average
  improves.
- **No human in the loop.** Regulated settings require an approval step and a signed record
  of who promoted what. MLflow has no built-in approval workflow; teams gate it in CI or a
  ticketing system instead.
- **Rollback is a first-class operation.** Reassigning the alias is instant — worth
  documenting in the README as the incident procedure, because knowing you can roll back is
  what makes automated promotion tolerable in the first place.
- **No model card.** Intended use, training data, limitations, known failure modes. Cheap
  to write and unusually visible in a portfolio.
