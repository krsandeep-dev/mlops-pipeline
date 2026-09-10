resource "aws_ecr_repository" "api" {
  name                 = "${var.project_name}/inference-api"
  image_tag_mutability = "IMMUTABLE"

  # Demo convenience: lets `terraform destroy` remove the repository even when it
  # still holds images. Production leaves this false so images cannot be lost.
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "api" {
  repository = aws_ecr_repository.api.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only the 3 most recent images"
      selection = {
        tagStatus = "any"
        countType = "imageCountMoreThan"
        # 3, not 10: the serving image is ~1.4 GB, and ECR bills per GB beyond a 500 MB
        # free tier that expires after 12 months. Ten versions would be ~14 GB standing
        # against a project rule of near-zero cloud cost, for images whose only consumer
        # is a Phase 6 demo that is torn down the same session.
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}
