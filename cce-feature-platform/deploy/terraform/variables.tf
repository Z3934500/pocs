variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
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

variable "msk_broker_count" {
  type    = number
  default = 3
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "msk_ebs_volume_gb" {
  type    = number
  default = 500
}

variable "redis_node_type" {
  type    = string
  default = "cache.m6g.large"
}

variable "redis_replica_count" {
  type    = number
  default = 1
}
