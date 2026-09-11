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
      description  = "Keep only the 6 most recent images (3 releases)"
      selection = {
        tagStatus = "any"
        countType = "imageCountMoreThan"
        # Counted in IMAGES, not releases, and each release lands two entries: the tagged
        # OCI index and the untagged amd64 manifest it points at. At 3 this retained one
        # and a half releases, so the second release would have evicted half of the
        # first, leaving a tag whose child manifest was gone. 6 keeps three whole
        # releases. Still bounded, because the image is ~293 MB pushed and ECR bills per
        # GB past a 500 MB free tier that expires after 12 months.
        countNumber = 6
      }
      action = { type = "expire" }
    }]
  })
}
