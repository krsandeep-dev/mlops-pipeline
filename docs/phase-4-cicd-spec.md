# Phase 4 Spec — CI/CD: GitHub Actions, multi-arch images, tag-bump handoff

**Input state:** Phase 3 closed (commit `8b44866`). Serving runs as a Helm release on k3d; the chart requires an explicit `image.tag` supplied by `make helm-install` from the `.image-tag` stamp. 23 tests pass, 1 skips (the in-network signature check). Image is ~1.4 GB, built locally and loaded via `k3d image import`.

## 1. Objective

A GitHub Actions pipeline that lints, tests, builds a multi-arch image, pushes it to a registry, and then **declares intent** by bumping the image tag in the Helm values file and committing it. CI never touches the cluster. `helm upgrade` runs locally against the committed value until Phase 7, when ArgoCD reads the same value with no change to the pipeline.

## 2. Decisions already made — plan within these

1. **Helm is the single source of truth.** Delete `k8s/` (Deployment, Service, Ingress, ConfigMap, Namespace, kustomization) and the `set image` scaffolding in `m3-deploy`. Git history is the archive; one README line points at `8b44866` for the pre-chart manifests. Two definitions of one deployment guarantees drift once ArgoCD syncs the chart.
2. **Both registries, different consumers and different triggers.** GHCR is the pull source for k3d and for anyone reading the repo — public packages need no pull secret. ECR exercises the OIDC role built in Phase 1.4 and feeds the Phase 6 Fargate demo. See §5 for why their triggers differ.
3. **GitHub-hosted runners.** Self-hosted CI runs only when the laptop is awake and shows stale or red to a reviewer; fork-PR execution on self-hosted runners is a real security footgun. Docker Hub pull limits do not apply to hosted runners pulling public images, so the `python:3.11-slim` base needs no login or mirror.
4. **CI stops at push + tag bump. No deploy from CI.** A hosted runner cannot reach a cluster on the laptop, and the workarounds (tunnel, exposed API server, self-hosted runner holding cluster credentials) are all worse than the gap. Push-based CD requires CI to hold cluster credentials and network reach; pull-based CD inverts that. This gap is the argument for Phase 7, not an obstacle to it.
5. **Multi-arch, native, no QEMU.** Hosted runners are amd64; k3d nodes run under Docker on Apple Silicon and are arm64. Build on a matrix of `ubuntu-latest` and `ubuntu-24.04-arm` (arm64 hosted runners are GA and free on public repos, 4 vCPU), push each by digest, then join with `docker buildx imagetools create`. QEMU emulation of a scipy/scikit-learn/mlflow build is slow enough to distort the whole pipeline.
6. **Repo goes public** — free unlimited Actions minutes on standard runners, anonymous GHCR pulls, and a reviewer can pull the image with one command. §8 is the pre-flight checklist that gates flipping visibility.

## 3. Constraints carried from Phases 1–3

- **CI cannot reach MLflow or MinIO.** Every artifact-touching path needs the Compose network (presigned URLs with the hostname inside the SigV4 signature). Hosted runners have neither the network nor the services. This is fine for the test suite — the 23 passing tests are pure functions, the payoff of keeping `decide` free of I/O in Phase 2.3 — but it has one sharp consequence, §4.
- **uv is authoritative and the interpreter is 3.11.** CI installs with `astral-sh/setup-uv` and `uv sync` against the lockfile; no `pip install`. Python 3.11 everywhere, matching the one-interpreter rule from Phase 1. The constraints file remains the parity reference for the serving image.
- **Chart refuses to guess its tag.** `image.tag: ""` plus `required` in the template. That guard stays; Phase 4 changes who satisfies it.
- **`imagePullPolicy` must be exactly `IfNotPresent`.** Two paths must both work after this phase: the local loop (`make image` → `k3d image import` → `make helm-install`) and the sync path (`make helm-sync` pulling from GHCR). `Always` breaks the local loop by re-pulling an image that was imported, not pushed; `Never` breaks the sync path. `IfNotPresent` serves both — and it only works if the local build is tagged with the **same repository string** the chart uses (`ghcr.io/<owner>/mlops-serving:<sha>`), so `make image` must tag with the full GHCR name even for local-only builds.
- **Linux/A′ is still untested**, but it does **not** block Phase 4: CI neither loads models nor deploys, so it never resolves `minio`. Leave it as a Phase 7 prerequisite.

## 4. The signature-parity gap — must be solved, not skipped

`m1-verify` gates on the in-network signature check, which loads the model and therefore needs the artifact path. On a hosted runner it will **skip**, which is the same failure the gate was built to prevent: a contract test that never runs.

**Required design:** a committed signature contract file, checked three ways.

- A local, in-network `make export-signature` target reads the champion's signature from the registry and writes a committed JSON contract (`serving/signature.json` or similar). Training itself runs in Airflow containers that never touch git, so this is deliberately a developer step, not a DAG step — the contract diff in a PR is the review signal that the signature changed.
- **CI (pure, every PR):** the pydantic request schema matches the committed contract. No network.
- **Local M1 gate (in-network):** the committed contract matches the live champion's signature. This is the existing check, re-pointed, so the file cannot silently drift from the registry.

The plan should state the JSON shape, where the export target runs (compose network, like M1), and what a signature change looks like in review.

## 5. Registry triggers — cost is a real constraint here

The image is ~1.4 GB, and multi-arch doubles stored bytes to roughly 2.8 GB per version. Under the existing 10-image ECR lifecycle policy that is ~28 GB, which is a few dollars a month at ECR's per-GB rate once the 500 MB free tier is exhausted (12-month, not permanent). That conflicts with the project's near-zero-cloud-cost rule.

- **GHCR: every push to `main` that touches build-relevant paths** (see §6 path filters). Free for public packages, and it is what k3d pulls.
- **ECR: release tags (`v*`) or `workflow_dispatch` only**, never every commit. Still proves the OIDC role keylessly and still feeds Phase 6. Tighten the lifecycle policy to ~3 images.
- **The OIDC trust policy must match the trigger.** A tag push presents `sub` = `repo:<owner>/<repo>:ref:refs/tags/<tag>`, not `refs/heads/main`. If the Phase 1.4 trust policy only allows the main branch, N3 fails at `AssumeRoleWithWebIdentity`. Read the trust policy before choosing between a `refs/tags/*` condition and an environment-scoped `sub`; changing it is a Terraform change and must be stated in the plan.

Plan should confirm the current lifecycle policy, the trust policy's `sub` condition, and the free-tier position rather than assuming this section is right.

## 6. Tag flow — do not recreate the duplication just removed

After this phase two tag sources exist: `values.yaml image.tag` (CI-bumped, committed) and `.image-tag` (local build stamp). State the precedence rule explicitly and implement it:

- `values.yaml` is authoritative for **what is deployed** — it is what Phase 7's ArgoCD will read.
- `.image-tag` supplies `TAG=` for **local builds only**.
- `required` stays, so a fresh clone before the first CI run fails loudly rather than guessing.
- `make helm-sync` deploys the committed value from GHCR; `make helm-install` keeps deploying the local stamp. They are distinct targets with distinct names.

**Retrigger guard, layered:** commits pushed with the default `GITHUB_TOKEN` do not start new workflow runs — that is the primary mechanism. Add `[skip ci]` as belt and braces. Set a `concurrency` group on `main` so two merges cannot race on the same values file. **Path filters:** the build-and-push job runs only when `serving/`, the Dockerfile, the chart, or the requirements/lockfiles change — a README edit must not rebuild a 1.4 GB multi-arch image or produce a bump commit. And the values file itself must **not** be a build-trigger path, or the bump commit re-enters the build.

**Branch protection is a decision, not a default.** The bump commit needs `permissions: contents: write` and a direct push to `main`, which a branch-protection rule rejects unless the workflow is exempted. Choose deliberately: unprotected `main` on a solo repo (simplest, honest for a portfolio) or a bot-opened PR (the pattern Argo Image Updater uses, but it breaks full automation until merged). State the choice in the plan.

## 7. Pipeline shape

- **On PR:** ruff, pytest via `uv`, the §4 contract check, the Q2 requirements-drift check (install `serving/requirements.txt` and the dev group, diff resolved versions — uv-native, not `pip freeze`), `helm lint` + `helm template`, `terraform fmt -check` + `terraform validate` (pure; no credentials), and a **non-blocking** Trivy image scan that reports but does not fail the build. No registry credentials, no OIDC, no push. Use `pull_request`, never `pull_request_target`. `concurrency` with cancel-in-progress per PR.
- **On push to `main` (path-filtered):** the PR jobs, then the multi-arch build, GHCR push, then the values bump commit.
- **On release tag / dispatch:** ECR push via OIDC.
- Cache uv and buildx layers; report build and push time per architecture.
- DAG integrity tests (do the DAG files parse) need an Airflow runtime and are **out of scope** — a deliberate omission, noted in the README.

## 8. Public-repo pre-flight — gates flipping visibility

- **OIDC trust policy** pinned to the exact repo and the ref the ECR trigger actually presents (§5), never a wildcard. Verify before the repo is public.
- **Fork PRs must not receive credentials or OIDC.** Gate every push/auth job on event and branch.
- **GHCR package visibility.** A package first published to a personal account defaults to **private even from a public repo** — packages inherit repository permissions, not visibility. N2's "k3d pulls with no secret" fails with `ImagePullBackOff` until the package is flipped to public in its settings, which is a one-time manual step and **irreversible**. Add the `org.opencontainers.image.source` label to the image so the package links to the repo before first publish. Record the flip as an explicit N2 step.
- **Scan history**, not just the working tree, for MinIO credentials, the AWS account ID, and any Airflow password. Compose almost certainly carries `minioadmin` defaults — move them to `.env` with a committed `.env.example`.
- Confirm Terraform state and `.image-tag` are gitignored and were never committed.

## 9. Verification milestones

- **N1 — PR gate:** open a PR; lint, tests, contract check, drift check, helm lint, terraform checks, Trivy report all green; no push job runs; no credentials exposed. A docs-only PR runs the checks but is proven **not** to trigger a build.
- **N2 — GHCR + k3d:** merge to `main`; multi-arch manifest list pushed; `docker buildx imagetools inspect` shows both platforms; package flipped to public; k3d pulls the GHCR image (no local import, no pull secret) and serves a prediction through the ingress with `/model` reporting v4. Then prove the local loop still works: `make image` + `make helm-install` on a local build with the same repository string.
- **N3 — ECR via OIDC:** push a `v*` tag; keyless push succeeds under the verified trust policy; report the exact stored size per architecture and the resulting lifecycle position.
- **N4 — tag-bump handoff (definition of done):** the bump commit lands on `main`, does **not** retrigger the workflow (prove it in the Actions tab), and `make helm-sync` on a clean checkout reproduces the running release from the committed value alone.

## 10. Deliverables

- Workflow files, `.dockerignore` if missing, updated Makefile targets (`export-signature`, `helm-sync`, retagged `image`), updated chart defaults (repository, `pullPolicy`)
- Signature contract file + export target + CI check + re-pointed M1 gate
- Deleted `k8s/` and retired `set image` scaffolding, with the README pointer
- Trust-policy and lifecycle-policy changes as Terraform, if §5 requires them
- Public-repo checklist recorded as done in the README, including the GHCR visibility flip
- This spec committed under `docs/`

## 11. Out of scope

Deploying from CI, ArgoCD (Phase 7), drift and monitoring (Phase 5), the Fargate demo (Phase 6), `replicas: 2` + preStop (carried, still deferred), TLS and auth, DAG integrity tests, blocking vulnerability gates, image signing and SBOM (name them as production gaps; Trivy is in scope only as a report).

## 12. Carried gaps to record, not fix

`readOnlyRootFilesystem: false`; Linux/A′ untested; cutover 502s at `replicas: 1`; image size — but §5 gives the size a cost consequence now, so **measure** `mlflow` vs `mlflow-skinny` and report real numbers (build time, pushed size per architecture, whether the model still loads) rather than deferring again. Deviating from the logged requirements remains a judgment call to be made with data.

## 13. Plan requirements

Inspect the repo first: current Makefile targets, chart values, `k8s/` contents, `pyproject.toml` dependency groups and lockfile, the Phase 1.4 OIDC role and its trust-policy `sub` condition, the ECR lifecycle policy, the test suite's network dependencies, and how the existing signature check reads the registry. Then produce — **before writing any code** — (1) the proposed file tree of additions, changes, and deletions, (2) the §4 contract-file design, (3) the §6 tag precedence, path-filter, and branch-protection choices, (4) the §5 trust-policy finding and whether Terraform changes, (5) ordered steps mapped to N1–N4 with exact commands, (6) the §8 checklist findings, (7) open questions. Stop for review after the plan.
