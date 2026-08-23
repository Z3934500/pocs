variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "environment" {
  type        = string
  description = "Runtime environment: dev, staging or production."

  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "environment must be one of: dev, staging, production."
  }
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "msk_security_group_id" {
  type = string
}

variable "redis_security_group_id" {
  type = string
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "msk_broker_count" {
  type = number
}

variable "msk_instance_type" {
  type = string
}

variable "msk_ebs_volume_gb" {
  type = number
}

variable "redis_engine_version" {
  type    = string
  default = "7.1"
}

variable "redis_node_type" {
  type = string
}

variable "redis_replica_count" {
  type = number
}

variable "apply_immediately" {
  type        = bool
  description = "Whether Redis changes should apply immediately. Keep false for production unless this is an emergency change."
  default     = false
}

variable "tags" {
  type        = map(string)
  description = "Extra resource tags."
  default     = {}
}
