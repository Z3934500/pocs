# Terraform — AWS Security Services
# GuardDuty + Macie + CloudTrail + Security Hub
# Part of: infrastructure-live/modules/security/ (per deploy/terraform/README.md)
# Apply separately from application Terraform — security resources are account-wide.

variable "environment"     { type = string }
variable "aws_region"      { type = string }
variable "alert_email"     { type = string; description = "Security alert destination" }
variable "log_retention_days" {
  type    = number
  default = 365   # Compliance: 1 year minimum for payment audit logs
}

# ── CloudTrail (all API calls + data events) ──────────────────────────────────
resource "aws_s3_bucket" "cloudtrail" {
  bucket        = "oms-cloudtrail-${var.environment}-${data.aws_caller_identity.current.account_id}"
  force_destroy = false  # Never destroy audit logs automatically
}

resource "aws_s3_bucket_policy" "cloudtrail" {
  bucket = aws_s3_bucket.cloudtrail.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSCloudTrailAclCheck"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:GetBucketAcl"
        Resource  = aws_s3_bucket.cloudtrail.arn
      },
      {
        Sid       = "AWSCloudTrailWrite"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.cloudtrail.arn}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
        Condition = {
          StringEquals = { "s3:x-amz-acl" = "bucket-owner-full-control" }
        }
      }
    ]
  })
}

# CloudWatch Logs destination for real-time CloudTrail streaming
# Enables: CloudWatch Alarms on API calls, Insights queries, 15-min latency vs S3 Athena
resource "aws_cloudwatch_log_group" "cloudtrail" {
  name              = "/oms/cloudtrail/${var.environment}"
  retention_in_days = var.log_retention_days
  tags = {
    Environment = var.environment
    Purpose     = "cloudtrail-stream"
    DataClass   = "restricted"
  }
}

# IAM role that allows CloudTrail to write to the above log group
resource "aws_iam_role" "cloudtrail_cw" {
  name = "oms-cloudtrail-cw-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudtrail.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Environment = var.environment }
}

resource "aws_iam_role_policy" "cloudtrail_cw" {
  name = "cloudtrail-cw-logs"
  role = aws_iam_role.cloudtrail_cw.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      # Scope strictly to the OMS CloudTrail log group
      Resource = "${aws_cloudwatch_log_group.cloudtrail.arn}:*"
    }]
  })
}

resource "aws_cloudtrail" "oms" {
  name                          = "oms-audit-${var.environment}"
  s3_bucket_name                = aws_s3_bucket.cloudtrail.id
  include_global_service_events = true
  is_multi_region_trail         = true
  enable_log_file_validation    = true  # Detect log tampering

  # P1 fix: stream to CloudWatch Logs for real-time alerting (≤15 min latency)
  # S3 → Athena remains the forensic path; CW Logs enables operational alerting
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail.arn}:*"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail_cw.arn

  # Capture S3 data events — detect if PII data is read/written unexpectedly
  event_selector {
    read_write_type           = "All"
    include_management_events = true

    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::oms-*"]  # All OMS-related S3 buckets
    }
  }

  tags = {
    Environment = var.environment
    Purpose     = "payment-audit"
    DataClass   = "restricted"
  }
}

# ── GuardDuty (threat detection) ─────────────────────────────────────────────
resource "aws_guardduty_detector" "oms" {
  enable = true

  datasources {
    s3_logs { enable = true }   # Detect anomalous S3 access (PII exfiltration)
    kubernetes {
      audit_logs { enable = true }  # Detect K8s API anomalies (unusual exec/port-forward)
    }
    malware_protection {
      scan_ec2_instance_with_findings {
        ebs_volumes { enable = true }
      }
    }
  }

  tags = { Environment = var.environment }
}

# ── Macie (PII discovery in S3) ───────────────────────────────────────────────
resource "aws_macie2_account" "oms" {
  finding_publishing_frequency = "SIX_HOURS"
  status                       = "ENABLED"
}

# Scan the application log bucket for accidentally logged PII
resource "aws_macie2_classification_job" "log_pii_scan" {
  depends_on = [aws_macie2_account.oms]
  name       = "oms-log-pii-scan-${var.environment}"
  job_type   = "SCHEDULED"

  schedule_frequency { weekly_schedule = "MONDAY" }

  s3_job_definition {
    bucket_definitions {
      account_id = data.aws_caller_identity.current.account_id
      buckets    = ["oms-logs-${var.environment}-*"]
    }
  }

  tags = {
    Environment = var.environment
    Purpose     = "pii-detection"
  }
}

# ── Security Hub (aggregate findings) ────────────────────────────────────────
resource "aws_securityhub_account" "oms" {}

resource "aws_securityhub_standards_subscription" "aws_foundational" {
  depends_on    = [aws_securityhub_account.oms]
  standards_arn = "arn:aws:securityhub:${var.aws_region}::standards/aws-foundational-security-best-practices/v/1.0.0"
}

resource "aws_securityhub_standards_subscription" "pci_dss" {
  depends_on    = [aws_securityhub_account.oms]
  standards_arn = "arn:aws:securityhub:${var.aws_region}::standards/pci-dss/v/3.2.1"
}

# Enable GuardDuty → Security Hub integration
resource "aws_securityhub_product_subscription" "guardduty" {
  depends_on  = [aws_securityhub_account.oms]
  product_arn = "arn:aws:securityhub:${var.aws_region}::product/aws/guardduty"
}

# ── SNS + CloudWatch Alarms for critical findings ────────────────────────────
resource "aws_sns_topic" "security_alerts" {
  name              = "oms-security-alerts-${var.environment}"
  kms_master_key_id = aws_kms_key.security_sns.key_id
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.security_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Alert on HIGH/CRITICAL GuardDuty findings
resource "aws_cloudwatch_metric_alarm" "guardduty_high_severity" {
  alarm_name          = "oms-guardduty-high-severity-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FindingCount"
  namespace           = "AWS/GuardDuty"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  dimensions          = { Severity = "High" }
  alarm_actions       = [aws_sns_topic.security_alerts.arn]
  treat_missing_data  = "notBreaching"
  alarm_description   = "GuardDuty HIGH severity finding detected in OMS ${var.environment}"
}

# Alert on data export denied spike (cross-border PII control trigger)
resource "aws_cloudwatch_metric_alarm" "pii_export_denied_spike" {
  alarm_name          = "oms-pii-export-denied-spike-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "data_export_denied_total"
  namespace           = "OMS/Security"
  period              = 300
  statistic           = "Sum"
  threshold           = 10   # > 10 denied exports in 5min is anomalous
  alarm_actions       = [aws_sns_topic.security_alerts.arn]
  treat_missing_data  = "notBreaching"
  alarm_description   = "Unusual volume of cross-border PII export denials — possible policy bypass attempt"
}

# ── KMS key for security SNS topic ───────────────────────────────────────────
resource "aws_kms_key" "security_sns" {
  description             = "OMS security SNS encryption — ${var.environment}"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  tags = { Environment = var.environment, Purpose = "security-alerts" }
}

data "aws_caller_identity" "current" {}
