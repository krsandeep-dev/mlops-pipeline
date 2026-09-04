# Phase 2 spec — step 2.4: the training-and-promotion DAG

Scope: turn the 2.3 pipeline into an Airflow DAG — train, register, evaluate the champion,
decide — and prove it can be triggered over the REST API, which is exactly how Phase 5's
drift loop will invoke it. This closes Phase 2.

Before-blocks are **excerpts showing where to anchor an edit**, not full-file
reproductions.

---

## Part 0 — Design decisions

### Four tasks, not one

Calling `promote.main()` from a single task would work and would waste Airflow. Splitting
into stages buys per-stage retries, per-stage logs, and a graph that shows *where* a run
died:

| Task | Does | Depends on |
| --- | --- | --- |
| `train_candidate` | trains, logs to MLflow, round-trip guard | — |
| `register` | registers the logged model as a version | train |
| `evaluate_champion` | resolves `@champion`, re-scores it on the validation split | — |
| `decide_and_apply` | pure decision + alias move + tags | all three |

`evaluate_champion` deliberately does **not** depend on training — it needs only the
registry and the validation rows, so it runs in parallel with the 5-second training. Small
here; the habit of asking "what actually depends on what" is the thing that matters when a
stage takes an hour.

Both branches read the same validation rows because `load_splits()` is deterministic over
the same reference file. That determinism is load-bearing — it's what makes the two MAEs
comparable — and it has a concurrency caveat covered in Part 6.

### A rejection is a successful run

The DAG's job is to *make a correct decision*, and "candidate rejected, threshold not met"
is a correct decision. So rejection ends the run green, with the verdict in the logs and on
the version's tags. Failure is reserved for something actually breaking — training error,
unloadable model, unreachable registry. Conflating "the answer was no" with "the pipeline
broke" is how teams end up with alert fatigue and a red dashboard nobody reads.

### Retries: default 1, but 0 on anything that mutates the registry

`train_candidate` and `evaluate_champion` retry once — they're safe to re-run, and a
transient MinIO or MLflow blip shouldn't kill the pipeline. The cost is an occasional
orphaned MLflow run from a failed first attempt; acceptable, noted below.

`register` and `decide_and_apply` get `retries=0`. Registration is a mutating call: if the
request succeeded but the response was lost, a retry registers a *second* version of the
same model. Moving an alias has the same shape. When a mutating step fails, a human
re-triggers the whole run and looks first — that's the safer default until each mutation is
made properly idempotent.

### `max_active_runs=1`

Phase 5 adds an automatic trigger while the manual one still exists. Two promotion runs
racing each other would interleave alias reads and writes — candidate A evaluates against
champion v3 while candidate B promotes v4 mid-flight. One at a time, enforced by the
scheduler, not by hoping.

### Schedule stays `None`

The trigger loop is the point of this project, and it arrives in Phase 5 via the REST API.
A `@weekly` schedule would be one line — and it would blur whether a retrain happened
because drift demanded it or because it was Tuesday. One trigger path until the second one
is built deliberately.

`promote.py` stays as the manual in-network dev entrypoint; the DAG is the operational
path.

---

## Part 1 — Change: `docker-compose.yml`

The `docker compose run -e ...` variables from 2.3 become permanent task environment.

`x-airflow-common` environment — before (excerpt):

```yaml
    PYTHONPATH: /opt/airflow/src
    MLFLOW_TRACKING_URI: http://mlflow:5000
```

After:

```yaml
    PYTHONPATH: /opt/airflow/src
    MLFLOW_TRACKING_URI: http://mlflow:5000
    DVC_REMOTE: minio-docker
    DVC_REPO: /opt/airflow/repo
```

This is the 1.6 convention finishing the job: code never hardcodes an environment-specific
name; the environment supplies it.

---

## Part 2 — New file: `dags/train_and_promote.py`

```python
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

        client = registry.get_client()
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
```

Details worth understanding:

- **XCom carries dictionaries, never models.** `TrainingResult` crosses as `asdict(...)`
  — run id, URI, metric numbers. The model itself moves through MLflow, the data through
  object storage; XCom is for coordinates, same rule as 1.6.
- **`ChampionSnapshot(**champion)`** rebuilds the dataclass from its XCom dict. The
  serialization boundary between tasks is real: each task is a separate process, and
  everything crossing it must survive JSON.
- **The round-trip guard travels for free.** `train.main()` already refuses to return an
  unservable model, so the DAG inherits the 2.3 guarantee without a dedicated task — the
  signature/dtype assertion lives where the model is made, which is where it belongs.
- **`decide_and_apply` takes all three inputs**, which is also its dependency
  declaration — TaskFlow reads the edges out of the data flow, as in 1.6.

---

## Part 3 — Run and verify

```bash
docker compose up -d          # picks up the env additions; no rebuild needed
uv run ruff check .
uv run pytest -q
```

Trigger `train_and_promote` from the UI. Expected on the current registry (champion v1,
rejected v2): a new version registered and **REJECTED** at the threshold, all four tasks
green, `register` and `evaluate_champion` overlapping in the Gantt view.

### Prove the promote path through the DAG

The gate's promote branch can't fire while the candidate ties the champion, so exercise it
via bootstrap:

```bash
docker compose run --rm airflow-scheduler python -c "
from mlops_pipeline import registry
registry.get_client().delete_registered_model_alias(registry.MODEL_NAME, registry.CHAMPION_ALIAS)
print('alias cleared')"
```

Trigger again → `PROMOTED ... bootstrapping`, and `@champion` lands on the new version.
Same bits as before, so nothing of value was demoted — but the DAG has now demonstrably
taken both branches.

### Trigger it the way Phase 5 will — over the REST API

```bash
PASS=$(python -c "import json; print(json.load(open('config/simple_auth_manager_passwords.json'))['admin'])")

TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"username\": \"admin\", \"password\": \"$PASS\"}" \
  | python -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

curl -s -X POST http://localhost:8080/api/v2/dags/train_and_promote/dagRuns \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"logical_date": null}'
```

Watch the run appear in the UI. This is the entire Phase 5 trigger mechanism, de-risked
now: JWT from the auth endpoint, one authenticated POST. If the dagRuns call returns 422,
open the api-server's interactive API docs (linked from the UI) and check the expected
body — the v2 API is strict about it.

---

## Part 4 — README and CLAUDE.md changes

### README — Roadmap

Phase 2 → complete; Phase 3 (serving on k3d) becomes current.

### README — new bullet under "Design decisions"

```markdown
- **Retraining is a DAG with a threshold, and rejection is success.** The pipeline
  registers every candidate, re-scores the live champion on the candidate's validation
  rows, and moves the `@champion` alias only on a ≥1% MAE improvement. A run that
  correctly declines to promote ends green — failure is reserved for the pipeline actually
  breaking. Registry-mutating tasks never retry automatically.
```

### CLAUDE.md — Status

```markdown
## Status
Phase 2 complete: features, tracked training, interpreter parity, registry + promotion
gate, training DAG with REST-API trigger verified.
Next: Phase 3 — FastAPI serving on k3d (multi-stage image, manifests, Helm).
```

### CLAUDE.md — Conventions, add

```markdown
- DAG runs that make a correct negative decision (rejection) succeed; task failure means
  the pipeline broke. Registry-mutating tasks (register, alias moves) run with retries=0.
```

---

## Part 5 — Definition of done

- [ ] `uv run pytest -q` — fourteen tests pass; `ruff check .` clean
- [ ] `train_and_promote` appears with no import errors; existing DAGs untouched
- [ ] UI-triggered run: four tasks green, REJECTED at the threshold, `register` and
      `evaluate_champion` overlap in the Gantt view
- [ ] The rejected version carries `promoted`, `decision_reason` and
      `evaluated_against_version` tags
- [ ] Bootstrap run after clearing the alias: PROMOTED, `@champion` on the new version
- [ ] REST-API trigger: token obtained from `/auth/token`, POST creates a run, run
      completes green — record the two curl commands' responses
- [ ] Round-trip guard visibly ran inside `train_candidate`'s log
- [ ] A deliberate failure: point `REFERENCE_URI` at a nonexistent object, trigger,
      confirm `train_candidate` **fails red** (broken pipeline ≠ rejection) — then revert
- [ ] `ingest_taxi_data` and `hello_stack` still green; compose stack healthy

---

## Part 6 — Production notes

- **Asset-driven scheduling, deliberately deferred.** Airflow 3's assets would let
  ingestion declare the reference sample as an outlet and this DAG schedule on it —
  re-ingest, auto-retrain. Not wired yet on purpose: Phase 5 adds the drift trigger, and
  two implicit trigger paths before either is understood is how pipelines become haunted.
  Name it in an interview as the pattern you'd reach for.
- **The concurrency caveat.** `max_active_runs=1` serialises promotion runs against each
  other, but not against `ingest_taxi_data` rewriting the reference sample mid-run — the
  1.6 idempotency note coming due. The production fix is run-scoped, immutable reference
  paths (`reference/{data_version}/...`) so a training run reads a snapshot no one can
  overwrite.
- **Orphaned MLflow runs.** A retried `train_candidate` leaves a half-finished run behind.
  Harmless clutter at this scale; production tags runs with the Airflow run id and sweeps
  incomplete ones.
- **LocalExecutor ceiling.** All four tasks execute on the scheduler host. The
  Kubernetes-native version is task-per-pod (KubernetesExecutor), which is also the honest
  answer to "how does this scale" — and Phase 3's k3d cluster is where that story becomes
  demonstrable.
- **No alerting.** A failed run turns red in a UI nobody watches. Production wires
  `on_failure_callback` to Slack/PagerDuty and defines SLAs per task. Worth one README
  line as a known gap.
