# Phase 5 Spec — Monitoring, Drift Detection, and the Automated Retraining Loop

**Input state:** Phase 4 closed (`c7c5659`). Serving runs the GHCR image via Helm on k3d, champion v4 trained on the 2023-01 DVC-pinned reference sample; `/metrics` is exposed by `prometheus-fastapi-instrumentator` but nothing scrapes it. Airflow DAGs: `ingest_taxi_data`, `train_and_promote` (MAE gate), `hello_stack`. Registry: v1 ≡ v4; v2/v3/v5 rejected with "MAE change 0.00%" because they were trained on identical data. From this phase on, every serving change ships through the Phase 4 pipeline.

## 1. Objective

Close the loop: a new month of data arrives → drift is measured against the champion's training data → retraining is triggered automatically → the promotion gate compares champion and candidate on the **same current data** → the new champion is served with no manual step → the dashboard shows the model change. Definition of done: one trigger, no hands, and the fixed smoke payload's predictions differ from v4's — the demo Phase 3 could not do.

## 2. Decisions already made — plan within these

1. **Batch drift, not request-level drift.** Evidently compares the newly ingested month against the champion's training data. Serving-side request logging and live-feature drift are out of scope.
2. **The loop triggers in-Airflow** (`TriggerDagRunOperator`), not through the REST API. No credential to stabilize, no network hop. The REST trigger proven in Phase 2 stays as the external/manual interface; its per-container password remains a recorded gap for external callers, not fixed here.
3. **Reference = the champion's training data**, derived from what the champion run logged (data pointer, DVC rev, month, sample size) — never hardcoded to 2023-01. After a promotion the reference moves with the champion.
4. **The gate evaluates champion and candidate on the same held-out slice of the new month.** Comparing stored MAEs from different months is meaningless under drift: the whole point is that the champion's error on *current* data has degraded. §4 requires inspecting what the gate does today.
5. **Serving pulls its model the way the cluster pulls its image.** An alias watcher in the serving pod polls the registry at a fixed interval and hot-swaps atomically when the champion's `run_id` changes. Nothing pushes into the cluster; no kubeconfig leaves it — the same pull-based principle Phase 4 chose over a CI deploy. The alternative (Airflow restarting the Deployment through the k8s API) is rejected because it hands Airflow cluster credentials and network reach. The Phase 3 rollout-restart path remains the manual fallback.
6. **Prometheus + Grafana run in-cluster via Helm**; dashboards and alert rules are provisioned as code from the repo, never click-ops. Batch drift metrics reach Prometheus through Pushgateway; the Evidently report and its metrics are also logged to MLflow as the system of record.
7. **Serving changes ship through Phase 4:** PR → CI → GHCR → bump → `helm-sync`. No local-only image is part of the loop.

## 3. Constraints carried from Phases 1–4

- **Evidently must resolve against the constraints file** (pandas/cryptography pins forced by MLflow). If it cannot, the drift task runs in an isolated environment (separate image or venv) rather than loosening a pin that guarantees model parity.
- **The signature contract is unchanged.** Retraining on a new month keeps features and dtypes; the pure contract test and the M1 gate must stay green. A signature change would be a spec violation for this phase.
- **Reach.** Airflow (Compose) → Pushgateway (k3d) goes through the k3d load balancer with an explicit `Host` header, which `requests`/`curl` can set and Prometheus scrape config cannot — which is why Prometheus scrapes serving from *inside* the cluster. Both directions must be proven before anything depends on them (§6 P1/P2).
- **Sampling parity.** New months are sampled with the same size and seed logic as the reference sample, so drift statistics compare like with like.
- **Deterministic training stays** (`random_state=42`). Different data now yields a different model — that is the point.
- **Hot-swap safety.** Load the new model in the background; run a canary prediction on the fixed smoke payload; swap only on success; on any failure keep serving the old model and expose the failure as a metric and a log line. Readiness never flaps; `/model` reflects the swap.
- **Cluster resources.** The monitoring stack adds roughly 1–2 GB. Fine on this machine, but trim unused components and set limits; state the memory estimate in the plan.
- **Zero paid cloud.** Everything in this phase runs locally.

## 4. Must-inspect before the plan — the repo determines these

1. `train_and_promote`: how the gate computes and compares MAE today — same evaluation set, or each run's own stored metric? What parameters define the training data window? What does a run log about its data (path, DVC rev, month, sample size)?
2. `ingest_taxi_data`: is the month a parameter? Where does the sampled parquet land and how is it DVC-tracked? Are later 2023 months reachable with the existing loader?
3. Serving: how the Phase 3 loader holds the model, whether a swap under concurrent requests is safe, and what `/model` exposes today.
4. Evidently: which line (0.4/0.6 legacy API vs 0.7+ new API) resolves against the pins, with the resolver output as evidence.

## 5. Metrics contract

- **Serving (scraped):** instrumentator defaults (request rate, latency histogram, error rate) plus custom `prediction_duration_minutes` histogram, `model_info{name,version,run_id}` gauge = 1, `model_reload_total{result}` counter, `model_loaded_timestamp_seconds`. A model-version change must be a visible event on the dashboard.
- **Drift (pushed per month, per DAG run):** `drift_share`, `drifted_features`, per-feature drift score, `champion_mae_current`, `candidate_mae_current` (when a retrain ran), `retrain_triggered`. Pushgateway series are labelled by month and cleaned up deliberately; stale batch metrics are a known Pushgateway hazard.
- Exact names are the plan's to finalize; the contract is what must be observable.

## 6. Verification milestones — ordered, each proven before the next

- **P1 — Monitoring:** Prometheus + Grafana up in k3d via Helm; serving scraped by in-cluster discovery; provisioned dashboard shows traffic from a load loop of the smoke records; `model_info` shows v4. Prove Airflow → Pushgateway reach with a throwaway push before P2 depends on it.
- **P2 — Drift measured on real data:** ingest 2023-02 and at least one later month (a summer month is a stronger candidate); Evidently report versus the champion's reference; metrics in Prometheus and MLflow; the threshold decision recorded. **Report what the data actually shows** — do not presuppose drift. Pick the demo month from evidence, and if no month crosses the threshold, that is a finding to bring back, not a reason to lower the threshold silently.
- **P3 — Retrain with a comparable gate:** drift → `TriggerDagRun` → train on the new month → gate evaluates champion and candidate on the same current holdout → promote or reject **with both numbers**. Under real drift the champion's MAE on current data should be worse than the candidate's; if it isn't, say so.
- **P4 — End-to-end, no hands (definition of done):** from a single ingest trigger to the new champion served by the watcher without a restart. `/model` shows the new version and `run_id`; the fixed payload's predictions differ from v4's; Grafana shows the version change; the M1 gate and pure contract test still pass; `make helm-sync` state is unchanged (the loop changed the model, not the deployment). Then re-run Phase 3's manual promotion demo once to confirm the restart path still works alongside the watcher.

## 7. Deliverables

- Drift DAG (ingest → drift → decision → trigger); `train_and_promote` updated with a parameterized data window and the comparable gate; Evidently task in a resolvable or isolated environment
- Serving: alias watcher with atomic swap and canary; custom metrics; shipped through a real PR → CI → bump → `helm-sync` cycle
- Helm: monitoring stack values, Pushgateway, ingress hosts for Grafana (and Pushgateway); dashboard JSON and alert rules committed
- Make targets for the loop demo; this spec committed under `docs/`; the README loop diagram deferred to Phase 6

## 8. Out of scope

Alertmanager → Airflow REST webhook; request-level drift and request logging; approval gates or shadow deployment before promotion; ArgoCD (Phase 7); the Fargate demo (Phase 6); Phase 4's carried gaps except where the loop touches them.

## 9. Enterprise notes — gaps accepted locally, flagged for production

- Thresholds are policy, not science; retraining and promoting without human approval is a real hazard. Production adds an approval step or a shadow evaluation before the alias moves.
- Hot-swap skips the probe validation a rollout gives. The canary prediction mitigates but does not replace a rollout; production uses a rollout, or a model server with revision-based traffic shifting.
- Registry polling from every replica scales linearly; production uses a single controller or an event.
- Pushgateway is for batch jobs only and keeps series forever unless deleted.
- Drift on a public dataset is a rehearsal; real deployments need ground-truth latency and label delay handled explicitly.

## 10. Plan requirements

Inspect §4 first, with file:line evidence. Then produce — **before writing any code** — (1) the file tree, (2) the gate redesign with the exact comparison it will make and the data it will hold out, (3) how the reference is derived from the champion run and how the data window is parameterized, (4) the watcher design: interval, atomicity, canary, failure metric, thread safety, (5) the monitoring stack choice (kube-prometheus-stack vs lighter charts) with a memory estimate, (6) the Evidently version and resolver evidence, (7) the Airflow → Pushgateway reach test, (8) ordered steps mapped to P1–P4 with exact commands, (9) open questions. Stop for review after the plan.
