# Phase 3 serving targets. M1-M4 are the verification milestones from
# docs/phase-3-serving-spec.md; each is proven before the next.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GIT_SHA        := $(shell git rev-parse --short HEAD)
# The full GHCR repository string, even for local-only builds. imagePullPolicy is
# IfNotPresent, which only serves both the local loop (build + k3d import) and the sync
# loop (pull from GHCR) if a locally built image and a pulled image share one repository
# string. A bare `mlops-serving` would make the two paths different images.
REGISTRY       := ghcr.io/krsandeep-dev
IMAGE          := $(REGISTRY)/mlops-serving
# Docker container names cannot contain slashes, so the M1 container needs its own short
# name now that IMAGE carries the registry path.
M1_CONTAINER   := mlops-serving-m1
IMAGE_TAG_FILE := .image-tag
COMPOSE_NET    := mlops-pipeline_default
CLUSTER        := mlops
NAMESPACE      := serving
MODEL_URI      ?= models:/taxi-trip-duration@champion
M1_PORT        ?= 8000
INGRESS_HOST   ?= mlops-serving.localhost
INGRESS_PORT   ?= 8081
EXPECT_VERSION ?= 4

# `make image` stamps the tag it produced here; every consuming target defaults to it.
# Without this the tag tracked HEAD, so the first commit after a build left run/import
# targets pointing at an image that was never built. An explicit TAG= still wins, and
# `image` itself always builds from current HEAD.
STAMPED_TAG := $(shell cat $(IMAGE_TAG_FILE) 2>/dev/null)
TAG         ?= $(if $(STAMPED_TAG),$(STAMPED_TAG),$(GIT_SHA))

.PHONY: help lint test image require-image m1-up m1-verify m1-down \
        cluster-up cluster-start cluster-stop cluster-down cluster-info \
        coredns-refresh m2-dns m2-verify image-import signature-check \
        m3-verify export-signature helm-sync \
        helm-lint helm-template helm-install helm-uninstall

help:  ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

lint:  ## ruff
	uv run ruff check .

test:  ## Host test suite
	uv run pytest -q

# ---------------------------------------------------------------- image

image:  ## Build the serving image from HEAD and stamp .image-tag
	docker build -f serving/Dockerfile -t $(IMAGE):$(GIT_SHA) .
	@echo $(GIT_SHA) > $(IMAGE_TAG_FILE)
	@echo "built $(IMAGE):$(GIT_SHA) and stamped $(IMAGE_TAG_FILE)"

# Guard for every target that consumes an image. Fails with the missing tag and the two
# ways out, instead of a bare "image not found" from docker or k3d three layers down.
require-image:
	@docker image inspect $(IMAGE):$(TAG) >/dev/null 2>&1 || { \
	  echo "ERROR: image $(IMAGE):$(TAG) is not present locally."; \
	  echo "  fix:  make image             # build from HEAD ($(GIT_SHA)) and stamp $(IMAGE_TAG_FILE)"; \
	  echo "  or:   make <target> TAG=xxx  # use an image you already have"; \
	  echo "  have: $$(docker images $(IMAGE) --format '{{.Tag}}' | tr '\n' ' ')"; \
	  exit 1; \
	}

# ---------------------------------------------------------------- M1

m1-up: require-image  ## M1: run the image on the compose network (networking is a non-issue here)
	docker run -d --rm --name $(M1_CONTAINER) \
	  --network $(COMPOSE_NET) \
	  -p $(M1_PORT):8000 \
	  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  -e MODEL_URI='$(MODEL_URI)' \
	  $(IMAGE):$(TAG)
	@echo "waiting for readiness..."
	@for i in $$(seq 1 60); do \
	  if curl -sf http://localhost:$(M1_PORT)/health/ready >/dev/null 2>&1; then \
	    echo "ready after $${i}s"; exit 0; fi; sleep 1; done; \
	  echo "NOT READY -- logs:"; docker logs $(M1_CONTAINER); exit 1

# signature-check runs first: the int32/int64 contract is the thing most likely to break
# silently on a retrain, so the gate asserts it rather than leaving it opt-in.
m1-verify: signature-check  ## M1: live signature parity, then the HTTP contract
	uv run python scripts/smoke_serving.py --url http://localhost:$(M1_PORT)

m1-down:  ## M1: stop the container
	-docker stop $(M1_CONTAINER)

# Runs in-network on purpose: the assertion loads the model, and MLflow's presigned
# artifact URLs only resolve inside the compose network. Same check() the pytest suite
# calls under SERVING_SIGNATURE_CHECK=1, so there is one implementation.
# Writes the committed contract. Runs in-network for the same reason signature-check
# does: reading a signature means loading the model. JSON on stdout, logs on stderr.
export-signature: require-image  ## Refresh serving/signature.json from the live champion
	docker run --rm -i --network $(COMPOSE_NET) \
	  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  -e MODEL_URI='$(MODEL_URI)' \
	  --entrypoint python $(IMAGE):$(TAG) /dev/stdin \
	  < scripts/export_signature.py > serving/signature.json
	@echo "wrote serving/signature.json"

signature-check: require-image  ## Live signature-parity assertion, inside the compose network
	docker run --rm --network $(COMPOSE_NET) \
	  -e SERVING_SIGNATURE_CHECK=1 \
	  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  -e MODEL_URI='$(MODEL_URI)' \
	  -v "$$PWD/scripts":/w/scripts:ro \
	  -v "$$PWD/serving/signature.json":/w/serving/signature.json:ro \
	  -w /w --entrypoint python $(IMAGE):$(TAG) \
	  scripts/check_signature_parity.py

# ---------------------------------------------------------------- M2

cluster-up: ## M2: create the k3d cluster from the committed config
	k3d cluster create --config k3d/cluster.yaml
	kubectl config use-context k3d-$(CLUSTER)
	kubectl wait --for=condition=Ready nodes --all --timeout=120s
	$(MAKE) coredns-refresh

cluster-start:  ## Start a stopped cluster
	k3d cluster start $(CLUSTER)
	kubectl config use-context k3d-$(CLUSTER)
	kubectl wait --for=condition=Ready nodes --all --timeout=120s
	$(MAKE) coredns-refresh

cluster-stop:  ## Stop the cluster without deleting it
	k3d cluster stop $(CLUSTER)

cluster-down:  ## Delete the cluster
	-k3d cluster delete $(CLUSTER)

# A restarted cluster leaves CoreDNS holding stale state -- measured: NXDOMAIN for
# kubernetes.default and for the compose names until the deployment is restarted. Cheap
# and idempotent, so it runs after every create and start rather than living in a
# troubleshooting note nobody reads at the moment it is needed.
coredns-refresh:  ## Restart CoreDNS and wait for it to come back
	kubectl -n kube-system rollout restart deploy/coredns
	kubectl -n kube-system rollout status deploy/coredns --timeout=60s

cluster-info:  ## Show cluster + network wiring
	kubectl get nodes -o wide
	@echo "--- cluster containers on $(COMPOSE_NET) ---"
	@docker network inspect $(COMPOSE_NET) \
	  --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}' | grep k3d || true

# Trailing dots make these absolute queries. busybox nslookup walks the ndots:5 search
# list and never retries the bare name, so `nslookup minio` reports NXDOMAIN even when
# resolution works -- a false negative. glibc/musl getaddrinfo, which the app uses, does
# fall back, which is why the M2 probe passes either way.
m2-dns:  ## M2 step 1: can a pod resolve the compose service names?
	kubectl run dnscheck-$$RANDOM --rm -i --restart=Never --image=busybox:1.36 -- \
	  sh -c 'nslookup mlflow. && nslookup minio.'

image-import: require-image  ## Load a locally built image into the cluster (helm-sync pulls from GHCR instead)
	k3d image import $(IMAGE):$(TAG) -c $(CLUSTER)

# The probe runs on the serving image rather than a bare python one: it already carries
# the exact MLflow client that will do the real load, and a `pip install mlflow` inside a
# throwaway pod is both slow and a different stack from the one under test. The script
# arrives on stdin, so nothing probe-specific is baked into the image.
m2-verify: image-import  ## M2 step 2: tracking API + a real artifact download, from a pod
	kubectl delete pod artifact-probe --ignore-not-found >/dev/null 2>&1 || true
	kubectl run artifact-probe --rm -i --restart=Never \
	  --image=$(IMAGE):$(TAG) --image-pull-policy=IfNotPresent \
	  --env=MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  --env=MODEL_URI='$(MODEL_URI)' \
	  --command -- python /dev/stdin < k3d/artifact_probe.py

# ---------------------------------------------------------------- M3

m3-verify:  ## M3: assert the contract through the Traefik ingress from the host
	uv run python scripts/smoke_serving.py \
	  --url http://$(INGRESS_HOST):$(INGRESS_PORT) --expect-version $(EXPECT_VERSION)

# ---------------------------------------------------------------- M4 / Helm

RELEASE ?= model-serving
CHART   := charts/model-serving

helm-lint:  ## Lint the chart
	helm lint $(CHART) --set image.tag=$(TAG)

helm-template:  ## Render the chart as it would be installed
	helm template $(RELEASE) $(CHART) --namespace $(NAMESPACE) --set image.tag=$(TAG)

# The tag is passed, never defaulted. .image-tag is written by `make image`, so the
# release always carries the image that was actually built and verified here; the chart
# refuses to render without it rather than falling back to something plausible.
helm-install: require-image image-import  ## Install/upgrade the release from the stamped tag
	helm upgrade --install $(RELEASE) $(CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --set image.tag=$(TAG) \
	  --wait --timeout 5m
	kubectl -n $(NAMESPACE) get pods -o wide

# The other half of the tag-precedence rule: helm-install deploys the LOCAL build from
# .image-tag, helm-sync deploys whatever CI committed to values.yaml and lets the cluster
# pull it from GHCR. Distinct names because they answer different questions -- "does my
# build work" versus "does the declared state deploy". Phase 7's ArgoCD reads the same
# committed value this target does.
# --reset-values is load-bearing, not decoration. helm upgrade carries forward the
# user-supplied values of the previous release, so after a `helm-install --set
# image.tag=<local>` this target would keep deploying that local tag and silently ignore
# the committed one -- invisible whenever the two happen to match. Resetting makes the
# chart's values.yaml the only input, which is what "deploy the declared state" means and
# what Phase 7's ArgoCD will do by construction.
helm-sync:  ## Deploy the tag committed in values.yaml (pulls from GHCR)
	helm upgrade --install $(RELEASE) $(CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --reset-values \
	  --wait --timeout 5m
	kubectl -n $(NAMESPACE) get pods -o wide

helm-uninstall:  ## Remove the release
	-helm uninstall $(RELEASE) --namespace $(NAMESPACE)
