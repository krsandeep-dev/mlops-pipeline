# Phase 3 Spec — Model Serving: FastAPI + `@champion` on k3d

**Input state:** Phases 1–2 complete. Training-and-promotion DAG live, REST trigger verified end-to-end. Registry alias `@champion` → v4; rejected candidates tagged with reasons.

## 1. Objective

Serve the champion model over HTTP from a local k3d cluster: a FastAPI app that loads `models:/<registered_model>@champion` at startup, packaged as a multi-stage Docker image, deployed first with raw Kubernetes manifests, then converted to a Helm chart. Definition of done is the promotion-to-rollout demo (M4, §9).

## 2. Constraints carried from Phases 1–2 (repo-verified)

- **Signature dtypes.** The model signature records integer/long columns. JSON numbers land in pandas as float64 by default, so serving must coerce every input column to the exact signature dtype before `predict`, or pyfunc schema enforcement rejects the frame.
- **Artifact access is network-bound.** The MLflow server issues presigned MinIO URLs embedding the compose-internal hostname (`minio:<port>`). Anything outside the compose network can reach the tracking API but fails at artifact download. k3d pods are outside. This is the phase's main risk — see §5.
- **Dependency parity.** Serving deps derive from the model's logged requirements, reconciled against the repo constraints file (the parity reference; pandas/cryptography pins are deliberate, forced by MLflow ceilings). Runtime image on Python 3.11 to preserve the one-interpreter setup.
- **Registry helper is `registry.client()`.** Reuse it if the packaging layout allows imports from the serving module; do not reintroduce `get_client`.

## 3. Decisions already made — plan within these

1. **Load at startup by alias, not a baked-in model.** Keeps deployment decoupled from training: promote in the registry, `kubectl rollout restart`, new model serves with no rebuild. This is the story Phase 2's promotion gate sets up and Phase 5's drift loop will reuse. (Baked-image alternative acknowledged in §12.)
2. **One uvicorn worker per pod; scale via replicas.** Default `replicas: 1`; the chart exposes the knob.
3. **Access path:** ClusterIP Service + Ingress on k3s's bundled Traefik, with a k3d host-port mapping to the load balancer. `kubectl port-forward` is acceptable only for M1-level smoke checks.
4. **Image into cluster via `k3d image import`.** A local registry is deferred; Phase 4 introduces a remote one.
5. **Cluster definition as a committed k3d config file**, not ad-hoc CLI flags.
6. **Manifests first, Helm second.** Get raw YAML green (M3), then chart it; the chart must reproduce the same deployment.

## 4. API contract

- `POST /predict` — body `{"records": [{...feature fields...}]}`; response `{"predictions": [...], "model": {"name": ..., "version": ..., "run_id": ...}}`. Single record = batch of one.
- **Request schema:** an explicit pydantic model whose fields mirror the signature inputs exactly (names and types), plus a unit test asserting pydantic-schema ↔ model-signature parity so a training change cannot silently desync serving.
- **Coercion step** builds the DataFrame with signature dtypes (integers stay the recorded int type).
- `GET /health/live` — unconditional 200. `GET /health/ready` — 200 only once the model is loaded. `GET /model` — name/version/run_id resolved from the alias at load time.
- Errors: 422 on validation (pydantic default); 503 if `/predict` is hit before load completes.
- Include `prometheus-fastapi-instrumentator` and `/metrics` now (single cheap dependency); scraping and dashboards remain Phase 5.

## 5. The network decision — plan must resolve; the repo determines the answer

Pods need (a) the MLflow tracking API and (b) name resolution + reachability of `minio` at the presigned port. Candidate mechanisms — choose after inspecting the compose file and how artifacts were logged (`mlflow-artifacts:/` proxy vs `s3://` direct):

- **A. Attach the cluster to the compose network** (`network:` in the k3d config). Then *prove* from inside a pod that compose service names resolve — k3d CoreDNS forwards to node DNS, but pod-level resolution of Docker container names must be verified, not assumed. If it fails, fall back to B.
- **B. Separate networks + host-published ports.** Tracking URI via `http://host.k3d.internal:<published-port>`; map the literal hostname `minio` to the host IP via CoreDNS custom config / NodeHosts so presigned URLs resolve. Works only if MinIO's host-published port equals its internal port — presigned signatures cover the Host header, so the name must stay `minio:<port>`; the IP behind it is free.
- **C. Direct object access** (`MLFLOW_S3_ENDPOINT_URL` + credentials), bypassing the proxy — only viable if the artifact URIs permit it, and it still needs name resolution, so it composes with A/B rather than replacing them.

**Hard requirement:** before deploying the app, prove the chosen mechanism from a throwaway in-cluster pod — one tracking-API call succeeds and one real artifact download succeeds. That is milestone M2. The app and the network do not get debugged at the same time.

### Resolution (measured 2026-09-04)

**C is eliminated.** Artifacts are addressed with the proxy scheme, not `s3://` — the
champion's run reports `artifact_uri=mlflow-artifacts:/2/<run>/artifacts` and the logged
model `artifact_location=mlflow-artifacts:/2/models/m-cde5ffab…/artifacts`, consistent with
`--artifacts-destination s3://mlflow-artifacts` (docker-compose.yml:100). But the scheme is
not server-brokered bytes: MLflow 3.15 answers
`GET /api/2.0/mlflow-artifacts/presigned/<path>` with

```json
{"url": "http://minio:9000/mlflow-artifacts/…&X-Amz-SignedHeaders=host&X-Amz-Signature=…"}
```

`X-Amz-SignedHeaders=host` puts the Host header inside the SigV4 signature, so the hostname
and port cannot be rewritten by an ingress, a port remap, or a proxy. Every client must
reach a host literally named `minio` on port 9000. Setting `MLFLOW_S3_ENDPOINT_URL` changes
nothing about that requirement, so C has no independent existence.

**B is blocked.** Its tracking URI would be `http://host.k3d.internal:5001`, and the MLflow
server's `--allowed-hosts` (docker-compose.yml:103) admits only `localhost*`, `127.0.0.1*`
and `mlflow*`. Measured against the running server:

| Host header | Response |
| --- | --- |
| `localhost:5001` | 200 |
| `mlflow:5000` | 200 |
| `host.k3d.internal:5001` | **403** |

B would therefore require widening `--allowed-hosts` on the live MLflow service. There is a
second skew in the same direction: MinIO publishes `9000:9000` (host port equals internal
port, so B's stated precondition does hold there), but MLflow publishes `5001:5000`, so the
tracking URI cannot keep one form across both networks. B is not adopted; Phase 3 does not
edit running Phase 1–2 infrastructure.

**A is chosen.** The k3d cluster attaches to `mlops-pipeline_default` (the Compose default
network — no `networks:` key is declared, and `docker network ls` confirms the name), where
Docker's embedded DNS resolves both `mlflow` and `minio` natively. Pods then reach tracking
at `http://mlflow:5000`, whose Host header is already allowed, and presigned URLs resolve
unmodified. Subnets are disjoint: Compose is `172.18.0.0/16`, k3s defaults are
`10.42.0.0/16` (pods) and `10.43.0.0/16` (services) — verified before attaching.

**Fallback A′ if pods cannot resolve Docker container names.** k3d nodes join the network,
but pods resolve through CoreDNS, which forwards to the node's `resolv.conf`; Docker's
embedded resolver at `127.0.0.11` is node-local and means the pod's own loopback inside a
pod. If M2's DNS step fails, add a CoreDNS `NodeHosts` entry mapping `minio` (and `mlflow`)
to their container IPs on that network, which the node can route. Tracking stays
`http://mlflow:5000`, so no Compose edit is needed in that case either.

**Timeouts are part of the resolution, not a detail.** MLflow's presigned download calls
`cloud_storage_http_request` with `timeout=None` (`mlflow/utils/request_utils.py`) and
retries five times with exponential backoff per file, so a wrong network does not raise —
it stalls. Observed from outside the Compose network: ten minutes, zero bytes, no error.
Both the M2 probe and the serving app therefore bound the artifact path explicitly — a
socket timeout per read and a hard wall-clock ceiling on the whole load.

## 6. App structure

Own serving directory. Config via pydantic-settings: `MODEL_URI` (default `models:/<registered_model>@champion`), `MLFLOW_TRACKING_URI`, and only whatever S3 endpoint/credential vars the §5 outcome requires. No credentials in the image — anything secret arrives via a Kubernetes Secret. The loader resolves alias → concrete version/run_id once at startup and caches the metadata for `/model`.

## 7. Image

Multi-stage: builder installs pinned deps (logged model requirements reconciled with the constraints file) into a venv; runtime is `python:3.11-slim`, copies venv + app, runs as non-root, ships no build toolchain, no secrets, no model weights. Tag with the short git SHA.

## 8. Kubernetes resources

- Dedicated namespace (e.g. `serving`).
- **Deployment:** env from ConfigMap (URIs) + Secret (credentials if the mechanism needs them); `startupProbe` on `/health/ready` with a generous `failureThreshold` to cover the model download, then liveness on `/health/live` and readiness on `/health/ready`; resource requests/limits set (starting point ~250m/512Mi requests, 1 CPU/1Gi limits — plan may adjust); non-root `securityContext`.
- Service (ClusterIP) and Ingress (Traefik).
- Secret committed only as `*.example`; real values applied locally and gitignored.

## 9. Verification milestones — ordered, each proven before the next

- **M1 — app correct, network trivial:** run the serving container attached to the compose network; `/predict` returns plausible durations for a sample payload; `/model` shows v4. Isolates app bugs from cluster networking.
- **M2 — cluster + egress:** k3d up from the config file; throwaway in-cluster pod proves tracking access and a real artifact download per §5.
- **M3 — deployed:** manifests applied; probes green; prediction served via ingress from the host.
- **M4 — promotion demo (definition of done):** move `@champion` to a different version (or re-run promotion), `kubectl rollout restart`, `/model` reflects the new version, predictions still valid. Then Helm: remove the raw-manifest deployment, `helm install` reproduces M3 + M4.

  **Pass criterion (settled during M4): `/model`'s `run_id` follows the alias. Changing
  prediction values is NOT the criterion.** v1 and v4 are the same model — identical MAE
  to 13 decimal places (3.4599869925067), identical `data_url`, identical artifact byte
  count — because training is deterministic (`random_state=42`) over the same DVC-pinned
  reference sample. Their `sha256` differs only through skops serialization metadata.
  Identical predictions after a flip are therefore the **expected, correct** result, not a
  failed demo; this is the same fact the gate reports when it rejects candidates at
  "MAE change 0.00%". A promotion visible in the prediction values needs a genuinely
  different model, which arrives with Phase 5's drift data (later months of taxi data).

## 10. Deliverables

- Serving app + tests (schema-parity test, dtype-coercion test)
- Multi-stage Dockerfile and serving requirements/constraints wiring
- Committed k3d config; raw manifests; Helm chart (values: image repo/tag, replicas, resources, env, ingress host)
- Smoke script or Make targets covering the M1–M4 commands
- This spec committed under `docs/` following the existing naming convention

## 11. Out of scope

CI/CD (Phase 4), Prometheus/Grafana wiring and drift (Phase 5), ECS Fargate demo (Phase 6 window), ArgoCD (Phase 7), autoscaling/HPA, API auth/TLS, canary or shadow deployments.

## 12. Enterprise notes — gaps accepted locally, flagged for production

- Kubernetes Secrets are base64, not encryption. Production: External Secrets / Sealed Secrets, or IRSA instead of static keys on AWS.
- Startup-pull adds cold-start latency and a registry dependency at boot. Production mitigations: immutable baked images per model version, an init-container artifact cache on a PVC, or a dedicated model server (KServe/Seldon). Simplicity here is deliberate.
- Rollout-restart promotion is manual; GitOps closes that loop in Phase 7.
- Pin images by digest in production; SHA tags suffice here.
- Single replica means no HA; the ingress is unauthenticated. Fine on k3d, unacceptable in production.

## 13. Plan requirements — what to hand back for review

Inspect the repo first: compose network name, artifact URI scheme, registered model name, packaging layout, existing Make conventions. Then produce — **before writing any code** — (1) the proposed file tree of additions/changes, (2) the §5 mechanism chosen, with the repo evidence supporting it, (3) ordered implementation steps mapped to M1–M4 with exact commands, (4) open questions. Stop for review after the plan.
