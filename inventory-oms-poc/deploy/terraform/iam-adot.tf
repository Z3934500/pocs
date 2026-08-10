# Terraform — IRSA for ADOT Collector
# Grants the ADOT Collector K8s ServiceAccount IAM permissions to write to:
#   X-Ray (traces) + AMP (metrics remote write) + CloudWatch Logs
#
# Pattern: IRSA (IAM Roles for Service Accounts) — Pod-level least-privilege.
# The Collector SA gets its own role, separate from business service roles.
# EKS 1.24+: use Pod Identity Association instead of IRSA annotation if preferred.
#
# Apply after:
#   1. OIDC provider is registered for the EKS cluster
#   2. ADOT Collector is deployed to the 'monitoring' namespace
#   3. K8s ServiceAccount 'adot-collector' exists in 'monitoring'

variable "eks_oidc_issuer_url" {
  type        = string
  description = "OIDC issuer URL from: aws eks describe-cluster --query cluster.identity.oidc.issuer"
}

variable "amp_workspace_arn" {
  type        = string
  description = "Amazon Managed Prometheus workspace ARN"
}

locals {
  oidc_provider = replace(var.eks_oidc_issuer_url, "https://", "")
}

# ── IAM Role — ADOT Collector ─────────────────────────────────────────────────
resource "aws_iam_role" "adot_collector" {
  name        = "oms-adot-collector-${var.environment}"
  description = "IRSA role for ADOT Collector — X-Ray + AMP + CloudWatch write"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_provider}" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          # Scoped to the exact ServiceAccount in the monitoring namespace
          "${local.oidc_provider}:sub" = "system:serviceaccount:monitoring:adot-collector"
          "${local.oidc_provider}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = {
    Environment = var.environment
    Component   = "observability"
    ManagedBy   = "terraform"
  }
}

# ── Policy: X-Ray write + centralized sampling ────────────────────────────────
# Includes GetSamplingRules/Targets so OTEL_TRACES_SAMPLER=xray works.
# Without these, the xray extension in adot-collector.yaml cannot fetch sampling
# rules and falls back to 100% sampling.
resource "aws_iam_role_policy" "adot_xray" {
  name = "adot-xray-write"
  role = aws_iam_role.adot_collector.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "XRayWrite"
      Effect = "Allow"
      Action = [
        "xray:PutTraceSegments",
        "xray:PutTelemetryRecords",
        "xray:GetSamplingRules",       # Required for centralized sampling
        "xray:GetSamplingTargets",     # Required for centralized sampling
        "xray:GetSamplingStatisticSummaries",
      ]
      Resource = "*"   # X-Ray does not support resource-level restrictions
    }]
  })
}

# ── Policy: AMP remote write ──────────────────────────────────────────────────
resource "aws_iam_role_policy" "adot_amp" {
  name = "adot-amp-remote-write"
  role = aws_iam_role.adot_collector.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "AMPRemoteWrite"
      Effect   = "Allow"
      Action   = ["aps:RemoteWrite", "aps:GetSeries", "aps:GetLabels", "aps:GetMetricMetadata"]
      Resource = var.amp_workspace_arn
    }]
  })
}

# ── Policy: CloudWatch Logs write ─────────────────────────────────────────────
resource "aws_iam_role_policy" "adot_cloudwatch_logs" {
  name = "adot-cloudwatch-logs"
  role = aws_iam_role.adot_collector.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "CloudWatchLogsWrite"
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
      ]
      # Scoped to OMS log groups only — not account-wide
      Resource = [
        "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/oms/*",
        "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/oms/*:*",
      ]
    }]
  })
}

# ── K8s ServiceAccount annotation (output for kubectl / Helm) ─────────────────
# After applying Terraform, annotate the K8s SA:
#   kubectl annotate serviceaccount adot-collector \
#     -n monitoring \
#     eks.amazonaws.com/role-arn=<role_arn>
# Or set in ADOT Collector Helm values / manifest serviceAccount.annotations.
output "adot_collector_role_arn" {
  value       = aws_iam_role.adot_collector.arn
  description = "Set as eks.amazonaws.com/role-arn on K8s ServiceAccount adot-collector in monitoring namespace"
}
