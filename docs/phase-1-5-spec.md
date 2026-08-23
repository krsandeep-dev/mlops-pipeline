# Phase 1 spec — step 1.5: DVC and the dataset

Scope: initialize DVC, choose and ingest the dataset, configure a local MinIO remote plus a
cloud S3 remote, and prove the round-trip both ways. The ingestion DAG is 1.6.

Before-blocks in this spec are **excerpts showing where to anchor an edit**, not
full-file reproductions. Match on the anchor lines and preserve anything else already
present.

---

## Part 0 — What DVC does here and why

### The problem it solves

Git stores code well and binary data badly. A 50 MB parquet file committed to git is
50 MB in every clone, forever, and a one-line change to it duplicates the whole thing.

DVC splits the two: the **bytes** go to object storage, and a small text pointer file
(`something.parquet.dvc`, containing an MD5 hash and size) goes into git. Checking out an
old commit gives you the pointer for that commit's data; `dvc pull` fetches exactly those
bytes back. That is what makes "this model came from this data" a verifiable claim rather
than a hopeful comment.

### The boundary with MLflow

This overlap confuses people, and being able to draw the line cleanly is worth interview
points:

| | Versions | Answers |
| --- | --- | --- |
| **DVC** | inputs — datasets, feature files | "what data produced this?" |
| **MLflow** | outputs — runs, params, metrics, model artifacts | "what did training produce, and how good was it?" |
| **Git** | code and both sets of pointers | "what logic was used?" |

A reproducible run is the intersection: a git commit pins the code, the `.dvc` pointer it
contains pins the data, and the MLflow run records what came out.

### Decision: DVC for versioning only, not for orchestration

DVC also has a pipeline feature (`dvc.yaml` + `dvc repro`) that builds a stage DAG and
re-runs only what changed. **We are not using it.** Airflow is already the orchestrator;
running two DAG engines over the same steps means two places to define dependencies and two
things to debug. DVC's job here stops at versioning data and artifacts.

Say this out loud in an interview and it reads as judgement. Adopting both without noticing
the overlap reads as tool-collecting.

### Decision: Airflow consumes pinned data, it does not produce DVC versions

Running `git commit` from inside an Airflow task is a mess — DAG runs would rewrite the
repository they were launched from. So:

- **Humans and CI** add and push dataset versions (`dvc add`, `dvc push`).
- **Airflow tasks** fetch the pinned version and read it.

Step 1.6 wires up the mechanics of the fetch. This step just makes the data versioned and
available.

---

## Part 1 — Dataset choice: NYC Yellow Taxi

The brief allowed NYC Taxi, Census, or Credit Default. Take **NYC Taxi**, for one reason
that outweighs everything else: it has a real time axis.

| | Rows | Time axis | Drift story |
| --- | --- | --- | --- |
| **NYC Taxi** | ~3M/month, monthly files | yes | feed a later month, drift is genuine |
| Credit Default (UCI) | 30k | no | drift must be faked by perturbing columns |
| Census / Adult | 48k | no | same |

Phases 5 and 7 are the entire point of this project, and both are drift-driven. On a
dataset with no time axis, "drift detected → retrain" is a demo where you corrupt your own
inputs to trigger your own alarm — and an interviewer will notice. With monthly taxi files
you train on January, replay February and March through the API, and Evidently detects
distribution shift that actually happened in the world. Fare changes, weather, seasonality
and route mix all move.

Also in its favour: multi-file structure exercises DVC properly, and it's a well-known
public dataset so a reviewer can sanity-check your numbers.

**Target:** trip duration in minutes, derived from the pickup and dropoff timestamps —
regression, interpretable, and it drifts seasonally.

**Baseline file:** `yellow_tripdata_2023-01.parquet` (~45 MB). Get the URL from the
official page — <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> — which
currently serves files as
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet`.
Verify the link on the page rather than trusting a hardcoded URL, and don't download more
than one month yet; later months arrive in Phase 5 as the drift source.

---

## Part 2 — Install DVC

**Environment: uv is authoritative.** The conda env is retired — deactivate it, and don't
install project packages there again. Anything the repo doesn't declare in
`pyproject.toml` will not exist in CI.

```bash
conda deactivate
uv sync                       # rebuilds .venv from pyproject.toml + uv.lock
uv add "dvc[s3]"
uv run dvc version
```

If you installed anything into conda during 1.2–1.4 that isn't in `pyproject.toml`
(`mlflow` and `boto3` should already be there from 1.1), add it with `uv add` now. Run
project Python through `uv run` from here on — that's what guarantees local and CI resolve
to the same packages. Once you're confident nothing is left behind, `conda env remove -n
mlops-pipeline` removes the temptation entirely.

Also add DVC to the Airflow image so tasks can fetch data in 1.6.

`docker/airflow/requirements-airflow.txt` — before:

```
mlflow==3.15.1
boto3==1.40.11
```

After:

```
mlflow==3.15.1
boto3==1.40.11
dvc[s3]==3.65.0
```

Pin whatever version `dvc version` reports on the host — host and container must match, for
the same reason MLflow's client and server do. Rebuild with
`docker compose build airflow-apiserver && docker compose up -d`.

Two things to get right here:

- **Set `requires-python = ">=3.11,<3.12"` in the restored `pyproject.toml`.** A
  `.python-version` file governs which interpreter uv uses locally; `requires-python` is
  what constrains the resolution recorded in `uv.lock`. With a loose floor, CI can
  legitimately resolve a different dependency set on a different interpreter and still call
  the lockfile satisfied.
- **`dvc[s3]` can disturb Airflow's own dependency tree.** It pulls `s3fs` and
  `aiobotocore`, which pin narrow `botocore` ranges, and the base image's Airflow install
  has constraints of its own. Install against Airflow's published constraints file for the
  pinned version, or at minimum treat the rebuild as unverified until all four Airflow
  components report healthy *and* `hello_stack` runs green again. A container that builds
  is not a container that works.

---

## Part 3 — Initialize DVC and configure remotes

```bash
dvc init
git status          # .dvc/config, .dvc/.gitignore, .dvcignore now staged
```

### Local remote (MinIO) — the default

```bash
dvc remote add -d minio s3://dvc-data
dvc remote modify minio endpointurl http://localhost:9000
dvc remote modify --local minio access_key_id "$MINIO_ROOT_USER"
dvc remote modify --local minio secret_access_key "$MINIO_ROOT_PASSWORD"
```

### Cloud remote (S3) — the switch

```bash
cd infra/terraform
BUCKET=$(terraform output -raw data_bucket_name)
cd ../..
dvc remote add aws "s3://$BUCKET/dvc"
dvc remote modify aws region eu-north-1
dvc remote modify --local aws profile mlops
```

### The `--local` flag is the whole security story here

DVC writes two config files:

| File | Contents | Git |
| --- | --- | --- |
| `.dvc/config` | remote names, URLs, region, endpoint | **committed** |
| `.dvc/config.local` | credentials, profile names, machine-specific overrides | **gitignored by DVC automatically** |

So a teammate clones the repo, gets the remote definitions for free, and supplies their own
credentials. Everything written **without** `--local` ends up in git — which is how access
keys get committed. Before your first push, open `.dvc/config` and confirm it contains no
secrets.

The `dvc-data` bucket already exists in MinIO from step 1.2's `minio-init`. The S3 bucket
came from Terraform in 1.4, and the `/dvc` prefix keeps DVC's content-addressed objects
from colliding with anything else you put in that bucket later.

---

## Part 4 — Ingest and push

```bash
mkdir -p data/raw
curl -L -o data/raw/yellow_tripdata_2023-01.parquet <verified-url>
ls -lh data/raw/

dvc add data/raw/yellow_tripdata_2023-01.parquet
git add data/raw/yellow_tripdata_2023-01.parquet.dvc data/raw/.gitignore .dvc/config .dvcignore
git commit -m "Phase 1.5: track NYC taxi 2023-01 with DVC"

dvc push
```

`dvc add` did three things worth understanding:

1. Computed the file's MD5 and moved the bytes into `.dvc/cache/`, content-addressed.
2. Wrote the `.dvc` pointer file — open it, it's about five lines.
3. Generated `data/raw/.gitignore` excluding the parquet itself, so git can never
   accidentally swallow it.

### Verify the bytes landed

MinIO console at <http://localhost:9001> → `dvc-data` bucket. You'll see nested
directories of hex-named objects rather than a file called `yellow_tripdata_2023-01.parquet`
— that's content addressing: objects are named by their hash, so identical content is
stored once no matter how many datasets reference it.

### Prove the round-trip

```bash
rm data/raw/yellow_tripdata_2023-01.parquet
rm -rf .dvc/cache
dvc pull
ls -lh data/raw/
```

Deleting the cache as well as the file matters — otherwise `dvc pull` restores from the
local cache and you've proven nothing about the remote.

### Prove the cloud switch

```bash
dvc push -r aws
aws s3 ls "s3://$BUCKET/dvc/" --recursive --profile mlops | head
```

One flag, same data, different backend — because MinIO speaks the S3 API. This is the
concrete payoff of the local-first architecture decision from the very first spec, and it's
worth about 50 MB of storage (fractions of a cent) to have demonstrated it.

---

## Part 5 — README and CLAUDE.md changes

### README — Repository layout, before

```
├── dags/                # Airflow DAGs
```

### After

```
├── dags/                # Airflow DAGs
├── data/                # DVC-tracked datasets (pointers in git, bytes in the remote)
```

### README — new subsection under "Design decisions"

```markdown
- **DVC for versioning, not orchestration.** DVC's pipeline feature (`dvc repro`) overlaps
  with Airflow; running both would mean two dependency graphs over the same steps. DVC
  versions data and artifacts, Airflow orchestrates, MLflow records outcomes.
- **NYC Yellow Taxi data.** Chosen over Census or Credit Default because it has a real time
  axis: drift in Phase 5 comes from replaying later months, not from synthetically
  corrupting inputs.
```

### README — Quickstart, add after the existing commands

```bash
dvc pull                # fetch the dataset (MinIO by default, `-r aws` for S3)
```

### README — Roadmap table row for Phase 1

Update the status note to mention DVC and the dataset.

### CLAUDE.md — Status

```markdown
## Status
1.1–1.5 complete (Compose stack, Airflow, AWS foundation, DVC + dataset).
Next: 1.6 ingestion DAG.
Specs: docs/phase-1-spec.md, docs/phase-1-3-spec.md, docs/phase-1-4-spec.md,
docs/phase-1-5-spec.md.
```

### CLAUDE.md — Conventions, add

```markdown
- DVC remotes: `minio` (default, local) and `aws` (S3, `-r aws`). Credentials live in
  `.dvc/config.local`, never `.dvc/config`.
- Dataset: NYC Yellow Taxi monthly parquet. 2023-01 is the training baseline; later months
  are reserved as the Phase 5 drift source.
```

---

## Part 6 — Definition of done

- [ ] `uv.lock` is committed and includes DVC; conda is deactivated and no longer used
- [ ] `uv run python scripts/smoke_mlflow.py` passes — the earlier pass was from conda and
      proves nothing about the uv environment
- [ ] `dvc doctor` runs clean and lists both remotes
- [ ] `.dvc/config` is committed and contains **no credentials**; `.dvc/config.local` is
      untracked
- [ ] `data/raw/yellow_tripdata_2023-01.parquet.dvc` is committed; the parquet itself is not
- [ ] `dvc push` succeeded and hex-named objects are visible in the MinIO `dvc-data` bucket
- [ ] After deleting both the file and `.dvc/cache`, `dvc pull` restores it at the same size
- [ ] `dvc push -r aws` succeeded and `aws s3 ls` shows objects under the bucket's `/dvc`
      prefix
- [ ] `dvc version` on the host matches the pin in `requirements-airflow.txt`, and the
      rebuilt Airflow image has DVC (`docker compose exec airflow-scheduler dvc version`)
- [ ] After the rebuild, all four Airflow components are healthy and `hello_stack` still
      runs green — installing DVC must not have disturbed Airflow's dependencies
- [ ] Everything from 1.2–1.3 still healthy (`docker compose ps`)
- [ ] README and CLAUDE.md updated per Part 5

---

## Part 7 — Production notes

- **Credentials in CI.** Phase 4's GitHub Actions job needs no DVC secrets: the OIDC role
  from 1.4 already grants `s3:GetObject`/`PutObject` on this bucket, so `dvc pull` works
  from temporary credentials with nothing stored in GitHub.
- **Cache growth.** `.dvc/cache` accumulates every version ever added. `dvc gc` prunes it;
  the S3 lifecycle rules from 1.4 cap the remote side.
- **Sensitive data.** DVC pointers in a public repo reveal file names and hashes, not
  contents — but a public *remote* would expose the data itself. This bucket blocks all
  public access, which is why that was configured before any data existed.
- **Scale limits.** DVC over S3 is right up to roughly hundreds of GB and file-level
  versioning. Beyond that — row-level time travel, concurrent writers, schema evolution —
  the answer is a table format such as Delta Lake or Iceberg, or lakeFS. Knowing where the
  tool stops is more convincing than claiming it scales forever.
- **Data quality gates.** Nothing currently validates schema or ranges on ingest. Great
  Expectations or Pandera in the 1.6 DAG is the standard answer, and Evidently in Phase 5
  covers the distribution side.
