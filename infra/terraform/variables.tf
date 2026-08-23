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
