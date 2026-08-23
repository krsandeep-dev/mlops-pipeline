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
