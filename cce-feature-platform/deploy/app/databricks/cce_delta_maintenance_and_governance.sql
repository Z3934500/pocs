-- CCE Databricks Delta maintenance and governance template.
--
-- Replace cce_dev with cce_staging or cce_prod before running.
-- This file is intentionally scoped to:
-- cce-feature-platform/01_foundation/03_mvp2_480k_emr_delta/dev_stage_prod/deploy/databricks
--
-- Goals:
-- 1. Register CCE Bronze/Silver/Gold/MLOps schemas.
-- 2. Show Delta table creation with clustering-friendly layout.
-- 3. Apply Unity Catalog row filters and column masks for row-block governance.
-- 4. Define OPTIMIZE / ZORDER / Liquid / VACUUM maintenance examples.

CREATE CATALOG IF NOT EXISTS cce_dev;
USE CATALOG cce_dev;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS mlops;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE SCHEMA IF NOT EXISTS reference;

-- ---------------------------------------------------------------------------
-- Gold table shapes
-- ---------------------------------------------------------------------------
-- For new Unity Catalog managed tables, prefer liquid clustering when the
-- selected Databricks Runtime supports it. If the team keeps legacy partitioned
-- tables, keep the OPTIMIZE ZORDER examples later in this file instead.

CREATE TABLE IF NOT EXISTS gold.customer_features (
  unified_customer_key STRING,
  business_date DATE,
  recency_days INT,
  tx_count_30d INT,
  monetary_30d DOUBLE,
  product_diversity INT,
  velocity_7d INT,
  cluster_id INT,
  segment_name STRING,
  risk_score DOUBLE,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (business_date, unified_customer_key)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = '30 days',
  'delta.logRetentionDuration' = '90 days'
);

CREATE TABLE IF NOT EXISTS gold.policy_features (
  policy_id STRING,
  unified_customer_key STRING,
  business_date DATE,
  policy_type STRING,
  policy_status STRING,
  policy_tenure_days INT,
  premium_amount DOUBLE,
  claim_count_12m INT,
  renewal_due_days INT,
  lapse_risk_score DOUBLE,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (business_date, policy_id, unified_customer_key)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = '30 days',
  'delta.logRetentionDuration' = '90 days'
);

CREATE TABLE IF NOT EXISTS mlops.feature_drift (
  feature_name STRING,
  business_date DATE,
  baseline_mean DOUBLE,
  current_mean DOUBLE,
  drift_ratio DOUBLE,
  severity STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (business_date, feature_name);

-- ---------------------------------------------------------------------------
-- Row-block governance with Unity Catalog row filters
-- ---------------------------------------------------------------------------
-- These examples implement "row-block" semantics: users who do not satisfy the
-- function do not receive those rows at all. Adapt group names to the real
-- identity provider and workspace groups.

CREATE OR REPLACE FUNCTION governance.can_read_customer_segment(segment_name STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('cce-feature-admin')
  OR is_account_group_member('cce-compliance')
  OR (
    is_account_group_member('cce-marketing-priority')
    AND segment_name = 'Priority'
  )
  OR (
    is_account_group_member('cce-marketing-growth')
    AND segment_name IN ('Priority', 'Growth')
  );

ALTER TABLE gold.customer_features
SET ROW FILTER governance.can_read_customer_segment ON (segment_name);

CREATE OR REPLACE FUNCTION governance.can_read_policy_status(policy_status STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('cce-feature-admin')
  OR is_account_group_member('cce-compliance')
  OR (
    is_account_group_member('cce-retention-ops')
    AND policy_status IN ('ACTIVE', 'PENDING_RENEWAL')
  );

ALTER TABLE gold.policy_features
SET ROW FILTER governance.can_read_policy_status ON (policy_status);

-- ---------------------------------------------------------------------------
-- Column masks for sensitive keys
-- ---------------------------------------------------------------------------
-- Broad BI users can still aggregate by segment, campaign and risk band without
-- seeing stable customer or policy identifiers. The Feature API service
-- principal should use a narrower serving view or a privileged group.

CREATE OR REPLACE FUNCTION governance.mask_customer_key(unified_customer_key STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('cce-feature-admin')
      OR is_account_group_member('cce-compliance')
      OR is_account_group_member('cce-feature-api-runtime')
    THEN unified_customer_key
    ELSE concat('MASKED_', sha2(unified_customer_key, 256))
  END;

ALTER TABLE gold.customer_features
ALTER COLUMN unified_customer_key
SET MASK governance.mask_customer_key;

ALTER TABLE gold.policy_features
ALTER COLUMN unified_customer_key
SET MASK governance.mask_customer_key;

-- ---------------------------------------------------------------------------
-- Serving views
-- ---------------------------------------------------------------------------
-- Prefer views for downstream users and APIs. This keeps table-level policies
-- and API-specific field selection separate from internal ETL tables.

CREATE OR REPLACE VIEW gold.v_campaign_customer_features AS
SELECT
  unified_customer_key,
  business_date,
  segment_name,
  recency_days,
  tx_count_30d,
  monetary_30d,
  product_diversity,
  velocity_7d,
  risk_score,
  updated_at
FROM gold.customer_features;

CREATE OR REPLACE VIEW gold.v_retention_policy_features AS
SELECT
  policy_id,
  unified_customer_key,
  business_date,
  policy_type,
  policy_status,
  policy_tenure_days,
  premium_amount,
  claim_count_12m,
  renewal_due_days,
  lapse_risk_score,
  updated_at
FROM gold.policy_features;

-- ---------------------------------------------------------------------------
-- Data governance checks inspired by data-governance-poc
-- ---------------------------------------------------------------------------
-- These SQL checks are examples for workflow validation tasks. Production
-- pipelines should fail before publishing Gold if these queries return bad rows.

-- Freshness check.
SELECT
  'freshness.gold_customer_features' AS check_name,
  max(updated_at) AS latest_update,
  CASE
    WHEN max(updated_at) >= current_timestamp() - INTERVAL 36 HOURS THEN 'OK'
    ELSE 'FAIL'
  END AS status
FROM gold.customer_features;

-- Volume check by business date.
SELECT
  business_date,
  count(*) AS customer_feature_rows
FROM gold.customer_features
GROUP BY business_date
ORDER BY business_date DESC;

-- Duplicate semantic feature rows.
SELECT
  business_date,
  unified_customer_key,
  count(*) AS duplicate_count
FROM gold.customer_features
GROUP BY business_date, unified_customer_key
HAVING count(*) > 1;

-- Drift severity check.
SELECT
  feature_name,
  drift_ratio,
  severity,
  created_at
FROM mlops.feature_drift
WHERE severity IN ('medium', 'high')
ORDER BY created_at DESC, drift_ratio DESC;

-- ---------------------------------------------------------------------------
-- Maintenance: liquid clustering
-- ---------------------------------------------------------------------------
-- For liquid-clustered tables, run OPTIMIZE without ZORDER. Use this pattern
-- for new CCE Gold tables when the selected runtime supports liquid clustering.

OPTIMIZE gold.customer_features
WHERE business_date >= current_date() - INTERVAL 7 DAYS;

OPTIMIZE gold.policy_features
WHERE business_date >= current_date() - INTERVAL 7 DAYS;

OPTIMIZE mlops.feature_drift
WHERE business_date >= current_date() - INTERVAL 30 DAYS;

-- ---------------------------------------------------------------------------
-- Maintenance: legacy Z-order layout
-- ---------------------------------------------------------------------------
-- Use these only for legacy tables that are not liquid clustered.
-- Do not casually combine ZORDER with liquid clustering on the same table.

-- OPTIMIZE gold.customer_features
-- WHERE business_date >= current_date() - INTERVAL 7 DAYS
-- ZORDER BY (unified_customer_key);

-- OPTIMIZE gold.policy_features
-- WHERE business_date >= current_date() - INTERVAL 7 DAYS
-- ZORDER BY (policy_id, unified_customer_key);

-- ---------------------------------------------------------------------------
-- VACUUM retention
-- ---------------------------------------------------------------------------
-- Run DRY RUN first. Align retention with replay, time-travel, audit and
-- rollback requirements. The 720-hour example equals 30 days.

VACUUM gold.customer_features RETAIN 720 HOURS DRY RUN;
VACUUM gold.policy_features RETAIN 720 HOURS DRY RUN;
VACUUM mlops.feature_drift RETAIN 720 HOURS DRY RUN;

-- Production execution only after DRY RUN review and owner approval.
-- VACUUM gold.customer_features RETAIN 720 HOURS;
-- VACUUM gold.policy_features RETAIN 720 HOURS;
-- VACUUM mlops.feature_drift RETAIN 720 HOURS;

-- ---------------------------------------------------------------------------
-- Policy rollback examples
-- ---------------------------------------------------------------------------
-- Keep these commented unless a release requires policy rollback.

-- ALTER TABLE gold.customer_features DROP ROW FILTER;
-- ALTER TABLE gold.customer_features ALTER COLUMN unified_customer_key DROP MASK;
-- ALTER TABLE gold.policy_features DROP ROW FILTER;
-- ALTER TABLE gold.policy_features ALTER COLUMN unified_customer_key DROP MASK;
