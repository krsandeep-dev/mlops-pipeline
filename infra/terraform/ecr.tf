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
