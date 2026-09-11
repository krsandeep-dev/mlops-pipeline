data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
}

locals {
  github_owner = split("/", var.github_repo)[0]
  github_name  = split("/", var.github_repo)[1]

  # The owner/repo portion of the OIDC subject claim, in GitHub's immutable ID form.
  oidc_subject_repo = "${local.github_owner}@${var.github_owner_id}/${local.github_name}@${var.github_repo_id}"
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

    # GitHub issues IMMUTABLE subject claims here: the sub embeds the numeric owner and
    # repository IDs, so trust cannot be inherited by a renamed repo, or by someone who
    # re-registers the name after a delete. Measured from a real token on 2026-09-11:
    #
    #   repo:krsandeep-dev@190084059/mlops-pipeline@1338539854:ref:refs/heads/main
    #
    # The name-only form never matched that, which is why this role had never actually
    # been assumable -- neither the original "repo:<owner>/<repo>:*" wildcard nor the
    # name-only refs that replaced it. Nothing exercised the role until Phase 4's N3, so
    # the breakage sat undetected from Phase 1.4 onward. StringLike is kept for the tag
    # glob; everything left of ":ref:" is now exact.
    #
    #   refs/tags/v*     release-ecr on a version tag
    #   refs/heads/main  the same workflow via workflow_dispatch
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${local.oidc_subject_repo}:ref:refs/tags/v*",
        "repo:${local.oidc_subject_repo}:ref:refs/heads/main",
      ]
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
      # The workflow reports stored size and lifecycle position after a copy. Without
      # this it got AccessDenied -- and the run still went green, because the reporting
      # command was piped and the pipeline returned tee's exit code. The pipe is fixed
      # in the workflow; this is the permission it needed all along.
      "ecr:DescribeImages",
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
