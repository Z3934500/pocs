# ── ALB Security Group ─────────────────────────────────────────────────────────
# Receives HTTPS from the internet; sends plain HTTP into the app tier
resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-sg-alb"
  description = "ALB: inbound 443 from internet, outbound to app tier"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS from internet"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "HTTP to EKS app nodes"
    from_port   = 8081
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = var.private_app_subnet_cidrs
  }

  tags = { Name = "${local.name_prefix}-sg-alb" }
}

# ── App Tier Security Group (EKS nodes / pods) ────────────────────────────────
resource "aws_security_group" "app" {
  name        = "${local.name_prefix}-sg-app"
  description = "EKS app tier: inbound from ALB only; outbound to data tier and AWS endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Order / Inventory / Payment service ports from ALB"
    from_port       = 8081
    to_port         = 8083
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # EKS nodes communicate with each other (same SG)
  ingress {
    description = "Intra-cluster pod traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "Allow all outbound (NAT GW restricts to NAT; VPC endpoints intercept AWS calls)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-sg-app" }
}

# ── Aurora Security Group ─────────────────────────────────────────────────────
resource "aws_security_group" "aurora" {
  name        = "${local.name_prefix}-sg-aurora"
  description = "Aurora PG: inbound 5432 from app tier only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "PostgreSQL from EKS app tier"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }

  tags = { Name = "${local.name_prefix}-sg-aurora" }
}

# ── Redis Security Group ──────────────────────────────────────────────────────
resource "aws_security_group" "redis" {
  name        = "${local.name_prefix}-sg-redis"
  description = "Redis Cluster: inbound 6379 from app tier only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Redis from EKS app tier"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }

  tags = { Name = "${local.name_prefix}-sg-redis" }
}

# ── MSK Security Group ────────────────────────────────────────────────────────
resource "aws_security_group" "msk" {
  name        = "${local.name_prefix}-sg-msk"
  description = "MSK Kafka: inbound 9094 (TLS) from app tier only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Kafka TLS client from EKS app tier"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }

  # MSK broker-to-broker replication
  ingress {
    description = "Kafka inter-broker replication"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    self        = true
  }

  tags = { Name = "${local.name_prefix}-sg-msk" }
}

# ── VPC Endpoint Security Group ───────────────────────────────────────────────
# Interface endpoints (ECR, CW Logs, Secrets Manager) share one SG
resource "aws_security_group" "vpce" {
  name        = "${local.name_prefix}-sg-vpce"
  description = "VPC Interface Endpoints: HTTPS from private app subnets only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS from private app subnets"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.private_app_subnet_cidrs
  }

  tags = { Name = "${local.name_prefix}-sg-vpce" }
}
