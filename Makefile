# Phase 3 serving targets. M1-M4 are the verification milestones from
# docs/phase-3-serving-spec.md; each is proven before the next.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GIT_SHA      := $(shell git rev-parse --short HEAD)
IMAGE        := mlops-serving
TAG          ?= $(GIT_SHA)
COMPOSE_NET  := mlops-pipeline_default
CLUSTER      := mlops
NAMESPACE    := serving
MODEL_URI    ?= models:/taxi-trip-duration@champion
M1_PORT      ?= 8000

.PHONY: help lint test image m1-up m1-verify m1-down cluster-up cluster-down \
        cluster-info m2-dns m2-verify image-import signature-check

help:  ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

lint:  ## ruff
	uv run ruff check .

test:  ## Host test suite
	uv run pytest -q

# ---------------------------------------------------------------- image

image:  ## Build the serving image (context is the repo root)
	docker build -f serving/Dockerfile -t $(IMAGE):$(TAG) .
	@echo "built $(IMAGE):$(TAG)"

# ---------------------------------------------------------------- M1

m1-up:  ## M1: run the image on the compose network (networking is a non-issue here)
	docker run -d --rm --name $(IMAGE)-m1 \
	  --network $(COMPOSE_NET) \
	  -p $(M1_PORT):8000 \
	  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  -e MODEL_URI='$(MODEL_URI)' \
	  $(IMAGE):$(TAG)
	@echo "waiting for readiness..."
	@for i in $$(seq 1 60); do \
	  if curl -sf http://localhost:$(M1_PORT)/health/ready >/dev/null 2>&1; then \
	    echo "ready after $${i}s"; exit 0; fi; sleep 1; done; \
	  echo "NOT READY -- logs:"; docker logs $(IMAGE)-m1; exit 1

m1-verify:  ## M1: assert the HTTP contract
	uv run python scripts/smoke_serving.py --url http://localhost:$(M1_PORT)

m1-down:  ## M1: stop the container
	-docker stop $(IMAGE)-m1

signature-check:  ## Run the live signature-parity test inside the compose network
	docker run --rm --network $(COMPOSE_NET) \
	  -e SERVING_SIGNATURE_CHECK=1 \
	  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
	  -e MODEL_URI='$(MODEL_URI)' \
	  -v "$$PWD/scripts":/w/scripts:ro -w /w --entrypoint python $(IMAGE):$(TAG) \
	  scripts/check_signature_parity.py

# ---------------------------------------------------------------- M2

cluster-up:  ## M2: create the k3d cluster from the committed config
	k3d cluster create --config k3d/cluster.yaml
	kubectl config use-context k3d-$(CLUSTER)
	kubectl wait --for=condition=Ready nodes --all --timeout=120s

cluster-down:  ## Delete the cluster
	-k3d cluster delete $(CLUSTER)

cluster-info:  ## Show cluster + network wiring
	kubectl get nodes -o wide
	@echo "--- cluster containers on $(COMPOSE_NET) ---"
	@docker network inspect $(COMPOSE_NET) \
	  --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}' | grep k3d || true

m2-dns:  ## M2 step 1: can a pod resolve the compose service names?
	kubectl run dnscheck-$$RANDOM --rm -i --restart=Never --image=busybox:1.36 -- \
	  sh -c 'nslookup mlflow && nslookup minio'

image-import:  ## Load the serving image into the cluster (no registry until Phase 4)
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
