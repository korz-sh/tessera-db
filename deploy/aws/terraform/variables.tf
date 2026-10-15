variable "name" {
  description = "Unique deployment prefix in this AWS account and region."
  type        = string
  default     = "tessera"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,25}$", var.name))
    error_message = "name must be 3..26 lowercase letters, digits or hyphens."
  }
}

variable "region" {
  description = "Single AWS region for the data plane."
  type        = string
}

variable "lambda_zip_path" {
  description = "Absolute path to the Linux x86_64 Python 3.12 Lambda ZIP built from this source."
  type        = string
}

variable "admin_secret_arn" {
  description = "ARN of an existing Secrets Manager secret containing one random admin Bearer token (>=32 characters)."
  type        = string
}

variable "force_destroy_bucket" {
  description = "Allow terraform destroy to delete all database objects; use only for isolated disposable stacks."
  type        = bool
  default     = false
}

variable "enable_aws_call_metrics" {
  description = "Log request-scoped logical AWS SDK call counts for a bounded cost study; excludes SQL, parameters and tokens."
  type        = bool
  default     = false
}

variable "allow_legacy_root_create" {
  description = "Compatibility escape hatch for old deployments: permits root-created databases outside tenant quotas. Keep false for new BYOC installations."
  type        = bool
  default     = false
}

variable "ephemeral_storage_mb" {
  description = "Lambda /tmp size; must exceed the largest restored SQLite database plus temporary files."
  type        = number
  default     = 512

  validation {
    condition     = var.ephemeral_storage_mb >= 512 && var.ephemeral_storage_mb <= 10240
    error_message = "ephemeral_storage_mb must be between 512 and 10240."
  }
}

variable "max_concurrency" {
  description = "Regional Lambda concurrency cap for this deployment."
  type        = number
  default     = 10

  validation {
    condition     = var.max_concurrency >= 1 && var.max_concurrency <= 1000
    error_message = "max_concurrency must be between 1 and 1000."
  }
}

variable "retention_keep_last" {
  description = "Minimum number of historical versions retained per active database."
  type        = number
  default     = 8

  validation {
    condition     = var.retention_keep_last >= 1 && var.retention_keep_last <= 100 && floor(var.retention_keep_last) == var.retention_keep_last
    error_message = "retention_keep_last must be an integer from 1 to 100."
  }
}

variable "retention_seconds" {
  description = "Minimum age of historical versions before collection; zero is for isolated tests only."
  type        = number
  default     = 604800

  validation {
    condition     = var.retention_seconds >= 0 && var.retention_seconds <= 31536000 && floor(var.retention_seconds) == var.retention_seconds
    error_message = "retention_seconds must be an integer from 0 to 31536000."
  }
}

variable "gc_grace_seconds" {
  description = "Minimum age of unreferenced artifacts before deletion; keep above the maximum reader and publication window."
  type        = number
  default     = 3600

  validation {
    condition     = var.gc_grace_seconds >= 60 && var.gc_grace_seconds <= 31536000 && floor(var.gc_grace_seconds) == var.gc_grace_seconds
    error_message = "gc_grace_seconds must be an integer from 60 to 31536000."
  }
}

variable "enable_native_gc" {
  description = "Opt in to the experimental bounded native image GC scheduler for histories above the complete-listing limit."
  type        = bool
  default     = false
}

variable "retention_metadata_limit" {
  description = "Complete-listing metadata cap before opt-in native GC; 999 by default. Lower values require enable_native_gc=true."
  type        = number
  default     = 999

  validation {
    condition     = var.retention_metadata_limit >= 1 && var.retention_metadata_limit <= 999 && floor(var.retention_metadata_limit) == var.retention_metadata_limit
    error_message = "retention_metadata_limit must be an integer from 1 to 999."
  }
}
