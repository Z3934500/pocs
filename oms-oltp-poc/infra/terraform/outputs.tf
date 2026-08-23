output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs (ALB + NAT GW)"
  value       = aws_subnet.public[*].id
}

output "private_app_subnet_ids" {
  description = "Private app subnet IDs (EKS nodes)"
  value       = aws_subnet.private_app[*].id
}

output "private_data_subnet_ids" {
  description = "Private data subnet IDs (Aurora / Redis / MSK)"
  value       = aws_subnet.private_data[*].id
}

output "nat_gateway_ids" {
  description = "NAT Gateway IDs (one per AZ)"
  value       = aws_nat_gateway.nat[*].id
}

output "sg_alb_id" {
  description = "Security Group ID for the ALB"
  value       = aws_security_group.alb.id
}

output "sg_app_id" {
  description = "Security Group ID for EKS app tier"
  value       = aws_security_group.app.id
}

output "sg_aurora_id" {
  description = "Security Group ID for Aurora"
  value       = aws_security_group.aurora.id
}

output "sg_redis_id" {
  description = "Security Group ID for Redis"
  value       = aws_security_group.redis.id
}

output "sg_msk_id" {
  description = "Security Group ID for MSK"
  value       = aws_security_group.msk.id
}
