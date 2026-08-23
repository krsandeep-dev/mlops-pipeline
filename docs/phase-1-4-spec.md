# Phase 1 spec — step 1.4: Terraform (S3, ECR, GitHub OIDC)

Scope: provision the small, cheap AWS foundation the later phases need, entirely in code.
Nothing from 1.2/1.3 changes. DVC wiring is 1.5; the ingestion DAG is 1.6.

⚠️ This is the first step that touches a real AWS account and can cost real money.
**You** run `terraform apply` and `aws configure`, not Claude Code. Read Part 1 before
anything else.

---

## Part 0 — What you are building and why

### What gets provisioned

| Resource | Used by | Why now |
| --- | --- | --- |
| S3 bucket | DVC remote (1.5), MLflow artifacts in cloud mode | DVC needs a remote to push to |
| ECR repository | FastAPI image from GitHub Actions (Phase 4) | CI needs somewhere to push |
| GitHub OIDC provider + IAM role | GitHub Actions auth (Phase 4) | so CI never holds long-lived AWS keys |

### What deliberately does *not* get provisioned

No VPC, no NAT gateway, no RDS, no ECS service, no load balancer. Everything above is
storage and identity: it costs cents at rest and nothing when idle. The classic portfolio
mistake is a NAT gateway (~$32/month, running whether you use it or not) left behind after
a tutorial. The ECS Fargate demo in Phase 6 gets its own Terraform workspace, applied and
destroyed in one sitting.

### Terraform concepts this step teaches

- **Provider** — the plugin translating HCL into AWS API calls; pinned so a future release
  can't silently change behaviour.
- **Resource** — a thing Terraform creates and owns.
- **Variable / output** — inputs, and values other tools read afterwards.
- **State** — Terraform's record of what exists and which real resource each block maps
  to. Losing it means Terraform forgets it owns your infrastructure.

**State stays local in this step.** A remote S3 backend is the production answer, but the
bucket holding the state has to exist before Terraform can use it — the bootstrap
chicken-and-egg. Part 8 shows the migration, which is a good thing to have done once.

---

## Part 1 — Manual prerequisites (you, not Claude Code)

### 1. Know which free tier your account is on

The account already exists, so this is a check rather than a choice — but it changes how
long this project's infrastructure survives. Open Billing and Cost Management → the
account plan is shown there.

**Free Plan (accounts created on or after 15 July 2025).** Credit-based: $100 at sign-up
plus up to $100 more for onboarding activities. The plan expires six months after sign-up
or when the credits run out, whichever comes first. After expiry there is a 90-day window
to upgrade to a Paid Plan before AWS closes the account and deletes its contents. Some
higher-cost services are also restricted on this plan.

**Legacy free tier (accounts created before that date).** The older 12-month trial model
plus always-free allowances, no expiry, no service restrictions.

What this means for the project:

- Either way, the resources in this step are trivially cheap: S3 always-free covers 5 GB,
  and ECR plus IAM add cents. Credits are not the constraint.
- If you are on the Free Plan, put the expiry date in your calendar now. A portfolio you
  show recruiters for a year outlives a six-month account, and everything in it is deleted
  on closure. Upgrading to a Paid Plan before expiry keeps the same account and account ID;
  with the budget alarm below, the monthly bill for this project stays under a dollar.
- Before relying on the Phase 6 ECS Fargate demo, confirm Fargate is available on your
  plan — a restricted service will fail at apply time, not at plan time.
- Everything through Phase 5 runs locally on MinIO and k3d, so an expired account never
  blocks the project itself. Only the cloud demo depends on it.

### 2. Budget alarm — before creating anything

AWS Console → Billing → Budgets → create a monthly cost budget of $5 with an alert at 50%
and 100% to your email. Do this first, every time, in every account you own.

### 3. Credentials

Install the CLI (Terraform comes from the HashiCorp tap):

```bash
brew install awscli
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
terraform version && aws --version
```

**Creating the access key.** In IAM → your user → Security credentials → Create access
key, choose **Command Line Interface (CLI)**, acknowledge the recommendation banner, and
create. The use-case selection is purely advisory — it records intent and shows a warning,
and does not affect the key's permissions in any way. All five options produce an
identical key pair; CLI is simply the truthful one here, since Terraform and the AWS CLI
are what will use it.

Then configure a **named profile** so this project never touches a default profile:

```bash
aws configure --profile mlops        # paste key ID and secret; region eu-north-1; output json
export AWS_PROFILE=mlops             # add to ~/.zshrc
aws sts get-caller-identity          # should return your IAM user's ARN, not root
```

Terraform picks up `AWS_PROFILE` automatically — no credentials go in any `.tf` file.

**Key hygiene, because this key has AdministratorAccess:**

- Enable MFA on the IAM user. An admin key plus a laptop is the whole account.
- The secret is shown exactly once. Store it in a password manager; delete the downloaded
  `.csv` afterwards. `aws configure` writes it in plaintext to `~/.aws/credentials`, which
  is why the file lives outside the repo and the repo must never reference it.
- Delete the key in the IAM console when the project is done, and rotate it if it is ever
  pasted anywhere.
- Never use root account credentials, and never paste AWS keys into a chat or into any
  file Claude Code can read.
- Claude Code must never run `aws configure`, never handle credentials, and never run
  `terraform apply` without your explicit go-ahead in that session.

**The better pattern, worth knowing for interviews:** long-lived access keys are exactly
what the OIDC role in `iam_github_oidc.tf` exists to avoid for CI. The human equivalent is
IAM Identity Center with `aws configure sso`, which issues short-lived session credentials
instead of a permanent key pair. It is more setup than a single-user learning account
warrants, but "we removed static keys from both CI and developer laptops" is the answer
you want to be able to give, so know why you chose the simpler option here.

### 4. Region

Use `eu-north-1` (Stockholm): nearest region, among the cheapest, and the natural answer
when a Gothenburg interviewer asks why you chose it.

---

## Part 2 — New files

```
infra/terraform/
├── versions.tf
├── providers.tf
├── variables.tf
├── s3.tf
├── ecr.tf
├── iam_github_oidc.tf
├── outputs.tf
└── terraform.tfvars.example
```

One resource type per file. Terraform loads every `.tf` in the directory regardless of
name, so file layout is purely for humans — which is exactly why it should be readable.

### `.gitignore` — before

```
logs/
plugins/
config/simple_auth_manager_passwords.json
!config/.gitkeep
```

### `.gitignore` — after

```
logs/
plugins/
config/simple_auth_manager_passwords.json
!config/.gitkeep
*.tfvars
!*.tfvars.example
crash.log
```

`.terraform/` and `*.tfstate*` are already ignored from step 1.1. **State files can
contain secrets in plaintext** — never commit them, even for a demo repo.

---

## Part 3 — File contents

### `versions.tf`

```hcl
terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
```

`>= 1.11` because native S3 state locking (Part 8) is GA from 1.11 — earlier versions
need a DynamoDB table, which is now deprecated. `~> 6.0` allows 6.1, 6.2… but never 7.0;
major bumps should be a deliberate commit, not a surprise.

### `providers.tf`

```hcl
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Owner     = "sandeep"
    }
  }
}
```

`default_tags` stamps every resource automatically. Untagged resources are how cloud bills
become unexplainable — with these you can filter Cost Explorer by `Project` and see
exactly what this repo costs.

### `variables.tf`

```hcl
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "mlops-pipeline"
}

variable "github_repo" {
  description = "GitHub repository allowed to assume the CI role, as owner/repo"
  type        = string
}
```

`github_repo` has no default on purpose: Terraform will refuse to run until you supply it,
which is better than defaulting to something wrong and granting a stranger's repo access
to your account.

### `terraform.tfvars.example`

```hcl
github_repo = "your-github-username/mlops-pipeline"
```

Copy to `terraform.tfvars` and fill in. The real file is gitignored.

### `s3.tf`

```hcl
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket = "${var.project_name}-data-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
```

Why each block:

- **Random suffix** — S3 bucket names are globally unique across all AWS customers.
  `mlops-pipeline-data` is long gone.
- **Separate resources** for versioning/encryption/access-block — AWS split these out of
  the bucket resource in provider v4. Old tutorials showing inline `versioning { }` blocks
  are pre-v4 and won't work.
- **Versioning** — DVC is content-addressed so it rarely overwrites, but versioning is
  also what makes S3-native state locking safe later.
- **`AES256`, not `aws:kms`** — SSE-S3 is free; customer-managed KMS keys cost ~$1/month
  each plus per-request charges. Name this trade-off in an interview: KMS buys key
  rotation, per-key access policies, and CloudTrail on key usage, which regulated
  workloads need and a portfolio doesn't.
- **Public access block** — publicly readable buckets are the single most common cause of
  cloud data leaks. Blocking at the bucket level is defence in depth.
- **Abort incomplete uploads** — failed multipart uploads leave invisible fragments that
  you are billed for indefinitely. This rule is nearly free money.
- **Expire old versions** — versioning without expiry means storage grows forever.

### `ecr.tf`

```hcl
resource "aws_ecr_repository" "api" {
  name                 = "${var.project_name}/inference-api"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "api" {
  repository = aws_ecr_repository.api.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only the 10 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}
```

- **`IMMUTABLE`** — a tag can never be repointed to a different image. This is the fix for
  "it worked in staging": if `v1.2.3` is immutable, staging and production provably ran
  identical bytes. It also forces CI to tag by commit SHA instead of overwriting `latest`.
- **`scan_on_push`** — free CVE scanning of image layers. Cheap credibility in a security
  conversation.
- **`force_delete = true`** — lets `terraform destroy` remove the repo even with images
  in it. Right for a demo, wrong for production; leave a comment saying so.
- **Lifecycle policy** — without it, every CI run's image accumulates at $0.10/GB-month
  forever.

### `iam_github_oidc.tf`

```hcl
data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
}

data "aws_iam_policy_document" "github_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.project_name}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_assume_role.json
}

data "aws_iam_policy_document" "ci_permissions" {
  statement {
    sid       = "ECRAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "ECRPush"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [aws_ecr_repository.api.arn]
  }

  statement {
    sid       = "DataBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid       = "DataBucketObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }
}

resource "aws_iam_role_policy" "ci_permissions" {
  name   = "${var.project_name}-ci"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.ci_permissions.json
}
```

This is the most interview-relevant file in the step:

- **OIDC instead of access keys.** GitHub Actions presents a short-lived signed token;
  AWS exchanges it for temporary credentials. No `AWS_SECRET_ACCESS_KEY` in GitHub
  secrets means nothing to leak, nothing to rotate. This is the current best practice, and
  "we removed long-lived cloud keys from CI" is a strong thing to have actually done.
- **The `sub` condition is the security boundary.** Without it, *any* GitHub repository in
  the world could assume your role. `repo:owner/name:*` scopes it to yours. Tightening
  further to `repo:owner/name:ref:refs/heads/main` blocks pull requests from forks —
  worth adding once Phase 4 works.
- **Why `ecr:GetAuthorizationToken` is on `"*"`.** It's an account-level call with no
  resource to scope to; AWS simply doesn't support narrowing it. Knowing *which*
  permissions genuinely can't be scoped, and being able to say why, is what separates
  least-privilege from cargo-culting.
- **Bucket ARN vs object ARN.** `ListBucket` acts on the bucket; `GetObject` acts on
  objects inside it (`arn:.../*`). Mixing these up is the most common IAM bug there is.
- **Fetching the thumbprint** via the `tls` data source instead of hardcoding it: GitHub's
  certificate rotates, and hardcoded fingerprints in blog posts go stale.

### `outputs.tf`

```hcl
output "data_bucket_name" {
  description = "S3 bucket for DVC remote storage"
  value       = aws_s3_bucket.data.id
}

output "ecr_repository_url" {
  description = "ECR repository URL for the inference API image"
  value       = aws_ecr_repository.api.repository_url
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions to assume via OIDC"
  value       = aws_iam_role.github_actions.arn
}
```

Steps 1.5 and Phase 4 consume these — outputs are the seam between infrastructure and
application config.

---

## Part 4 — Run and verify

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your repo
terraform init
terraform fmt -check
terraform validate
terraform plan
```

Read the plan. It should show **exactly 12 resources to add and none to change or
destroy**. Reading a plan properly is the habit that prevents production incidents;
"Claude Code said it was fine" is not reading a plan.

Then, yourself:

```bash
terraform apply
terraform output
```

Verify out-of-band, because Terraform reporting success is not the same as AWS being right:

```bash
aws s3 ls | grep mlops-pipeline
aws ecr describe-repositories --region eu-north-1
aws iam get-role --role-name mlops-pipeline-github-actions
```

### Definition of done

- [ ] `aws sts get-caller-identity` returns your IAM user ARN under the `mlops` profile
- [ ] `terraform validate` and `terraform fmt -check` both clean
- [ ] `terraform plan` shows 12 to add, 0 to change, 0 to destroy
- [ ] `terraform apply` succeeds; three outputs printed
- [ ] The three `aws` verification commands confirm the resources exist
- [ ] Budget alarm is configured and you received the confirmation email
- [ ] `git status` shows no `terraform.tfvars`, no `*.tfstate`, no `.terraform/`
- [ ] Everything from 1.2 and 1.3 still runs (`docker compose ps`)

### Tearing down

```bash
terraform destroy
```

Idle cost here is a few cents a month, so leaving it up is fine — but run `destroy` once
now and re-apply, so you know the teardown works and there's no surprise months later.

---

## Part 5 — README changes

### 5a. Prerequisites — before

```markdown
Prerequisites: Docker Desktop, [uv](https://github.com/astral-sh/uv).
```

### 5a. Prerequisites — after

```markdown
Prerequisites: Docker Desktop, [uv](https://github.com/astral-sh/uv).
For the optional AWS layer: Terraform ≥ 1.11 and the AWS CLI.
```

### 5b. Roadmap table — before

```markdown
| 1 | Local infra: Compose stack (MinIO, Postgres, MLflow, Airflow), Terraform, DVC, ingestion DAG | 🔨 in progress |
```

### 5b. Roadmap table — after

```markdown
| 1 | Local infra: Compose stack (MinIO, Postgres, MLflow, Airflow), Terraform, DVC, ingestion DAG | 🔨 in progress — Compose stack and AWS foundation done |
```

### 5c. New section — insert after "Design decisions", before "Roadmap"

```markdown
## Cloud footprint and cost

The AWS layer is deliberately minimal and fully described in `infra/terraform/`:

| Resource | Purpose | Cost when idle |
| --- | --- | --- |
| S3 bucket | DVC remote | ~$0.02/GB-month, lifecycle rules cap growth |
| ECR repository | Inference API images | ~$0.10/GB-month, last 10 images retained |
| GitHub OIDC provider + IAM role | Keyless CI authentication | free |

No always-on compute is provisioned. The ECS Fargate demo in Phase 6 is applied and
destroyed in a single session.

Everything runs locally without an AWS account: MinIO stands in for S3, and k3d for
managed Kubernetes.

```bash
cd infra/terraform && terraform init && terraform plan
```

**Security posture:** CI authenticates via GitHub OIDC — no long-lived AWS keys exist
anywhere in the repo or in GitHub secrets. The IAM role is scoped to one repository, one
ECR repository, and one S3 bucket.
```

### 5d. Production gaps — add three bullets

```markdown
- Terraform state is local; production uses an S3 backend with native locking
  (`use_lockfile = true`), one state file per environment.
- Bucket encryption is SSE-S3; regulated workloads use customer-managed KMS keys for
  rotation, per-key policies, and CloudTrail on key usage.
- The CI role trusts any ref in the repository; tightening the `sub` condition to
  `refs/heads/main` blocks fork-PR access.
```

---

## Part 6 — `CLAUDE.md` change

Add to the **Working rules** section:

```markdown
- Every step ends with a README update: roadmap status, plus any new prerequisite,
  command, or production gap the step introduced.
- Never run `terraform apply`, `terraform destroy`, `aws configure`, or any command that
  creates, modifies, or deletes cloud resources. Prepare the change, show the plan, and
  stop — Sandeep runs it.
```

And update **Status**:

```markdown
## Status
1.1–1.4 complete (Compose stack + Airflow + AWS foundation). Next: 1.5 DVC.
Specs: docs/phase-1-spec.md, docs/phase-1-3-spec.md, docs/phase-1-4-spec.md.
```

---

## Part 7 — Production notes (for the README's gap list, and for interviews)

- **Remote state.** Once a bucket exists, state moves into it. The migration is a
  `backend "s3"` block plus `terraform init -migrate-state`, using native S3 locking
  (`use_lockfile = true`) rather than the deprecated DynamoDB table. Do this once as an
  exercise — the bootstrap problem is a standard interview question.
- **Environments.** One state file per environment (dev/staging/prod), via separate
  backend keys or workspaces — never one state for everything.
- **Policy as code.** `tfsec` or `checkov` in CI catches public buckets and over-broad IAM
  before merge. A natural Phase 4 addition.
- **Plan in CI, apply gated.** Production teams run `terraform plan` on every PR and post
  the diff as a comment; `apply` requires a human approval.
- **Drift detection.** A scheduled `terraform plan` that alerts when reality diverges from
  code — the thing that catches console changes made during an incident.
