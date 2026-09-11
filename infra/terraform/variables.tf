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

# Numeric GitHub IDs, which appear in the OIDC subject claim alongside the names. Public
# information, not secrets: `gh api repos/<owner>/<repo> --jq '{id, owner: .owner.id}'`.
# They are defaulted so no tfvars change is needed, and pinning them is what makes the
# trust survive a rename and refuse a re-registered name.
variable "github_owner_id" {
  description = "Numeric GitHub owner ID embedded in the OIDC subject claim"
  type        = string
  default     = "190084059"
}

variable "github_repo_id" {
  description = "Numeric GitHub repository ID embedded in the OIDC subject claim"
  type        = string
  default     = "1338539854"
}
