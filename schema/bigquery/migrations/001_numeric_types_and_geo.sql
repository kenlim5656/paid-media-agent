-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- MIGRATION 001: NUMERIC Financial Types + Geo Dimensions
-- =============================================================================
-- Applies to: existing deployments of paid-media-schema v1
-- Required before: Task 27 (Meridian MMM) and Task 30 (IP intelligence)
--
-- CHANGE 1 — Geo dimensions on spend tables (ALTER TABLE ADD COLUMN)
--   BigQuery supports ADD COLUMN without data loss. Run section A first.
--   Required for Meridian MMM which needs geo × media × spend data.
--
-- CHANGE 2 — NUMERIC financial types (CTAS migration — see section B)
--   BigQuery does NOT support ALTER COLUMN type. To migrate FLOAT64 → NUMERIC
--   on financial columns, use the CREATE TABLE AS SELECT pattern in section B.
--   Affects: spend, cpa, roas, budget, credit_weight, credit_value, deal_value.
--   New deployments (no existing data): ignore section B — the updated DDL
--   source files (03_platform.sql, 04_attribution.sql, etc.) already use NUMERIC.
--
-- EXECUTION ORDER: Run A before B. B is optional for existing deployments
--   with no data — just re-run the DDL source files from scratch instead.
--
-- Usage (replace placeholders):
--   sed 's/{project}/my-project/g; s/{dataset}/paid_media/g' \
--     bigquery/migrations/001_numeric_types_and_geo.sql \
--     | bq query --use_legacy_sql=false
-- =============================================================================


-- ============================================================================
-- SECTION A: Add geo dimension columns to spend tables
-- Safe to run on tables with existing data — no data loss.
-- ============================================================================

-- platform_daily_spend (campaign-level)
ALTER TABLE `{project}.{dataset}.platform_daily_spend`
    ADD COLUMN IF NOT EXISTS geo_country_code  STRING
        OPTIONS (description = "ISO 3166-1 alpha-2 country code (e.g. 'US')"),
    ADD COLUMN IF NOT EXISTS geo_state_code    STRING
        OPTIONS (description = "State / province code (e.g. 'CA'). Required for Meridian MMM geo-level modeling."),
    ADD COLUMN IF NOT EXISTS geo_dma_code      STRING
        OPTIONS (description = "Nielsen DMA code (e.g. '807' = San Francisco). US only."),
    ADD COLUMN IF NOT EXISTS geo_region_name   STRING
        OPTIONS (description = "Human-readable region name for reporting (e.g. 'California')");

-- platform_daily_spend_ad_group (ad group-level)
ALTER TABLE `{project}.{dataset}.platform_daily_spend_ad_group`
    ADD COLUMN IF NOT EXISTS geo_country_code  STRING,
    ADD COLUMN IF NOT EXISTS geo_state_code    STRING,
    ADD COLUMN IF NOT EXISTS geo_dma_code      STRING,
    ADD COLUMN IF NOT EXISTS geo_region_name   STRING;

-- platform_daily_spend_ad (ad-level, from 06_reporting.sql)
ALTER TABLE `{project}.{dataset}.platform_daily_spend_ad`
    ADD COLUMN IF NOT EXISTS geo_country_code  STRING,
    ADD COLUMN IF NOT EXISTS geo_state_code    STRING,
    ADD COLUMN IF NOT EXISTS geo_dma_code      STRING,
    ADD COLUMN IF NOT EXISTS geo_region_name   STRING;

-- platform_daily_spend_keyword (keyword-level, from 06_reporting.sql)
ALTER TABLE `{project}.{dataset}.platform_daily_spend_keyword`
    ADD COLUMN IF NOT EXISTS geo_country_code  STRING,
    ADD COLUMN IF NOT EXISTS geo_state_code    STRING,
    ADD COLUMN IF NOT EXISTS geo_dma_code      STRING,
    ADD COLUMN IF NOT EXISTS geo_region_name   STRING;


-- ============================================================================
-- SECTION B: NUMERIC financial type migration (CTAS pattern)
-- Run ONLY on existing deployments with data. New deployments: skip this —
-- use the updated DDL source files which already define NUMERIC types.
--
-- Pattern for each table:
--   1. Rename original table to _float64_backup
--   2. CREATE TABLE AS SELECT with explicit CAST to NUMERIC
--   3. Verify row counts match
--   4. Drop backup after verification (not included — do manually)
-- ============================================================================

-- ── platform_daily_spend ─────────────────────────────────────────────────────

-- Step 1: Back up
ALTER TABLE `{project}.{dataset}.platform_daily_spend`
    RENAME TO platform_daily_spend_float64_backup;

-- Step 2: Recreate with NUMERIC financial columns
CREATE TABLE `{project}.{dataset}.platform_daily_spend`
PARTITION BY date
CLUSTER BY platform, campaign_id
OPTIONS (description = "Daily spend and performance metrics by campaign across all platforms.")
AS SELECT
    spend_id,
    date,
    platform,
    campaign_id,
    platform_campaign_id,
    -- Financial: FLOAT64 → NUMERIC
    CAST(spend AS NUMERIC)                          AS spend,
    currency,
    CAST(impressions AS INT64)                      AS impressions,
    CAST(clicks AS INT64)                           AS clicks,
    CAST(reach AS INT64)                            AS reach,
    CAST(video_views AS INT64)                      AS video_views,
    CAST(video_views_25pct AS INT64)                AS video_views_25pct,
    CAST(video_views_50pct AS INT64)                AS video_views_50pct,
    CAST(video_views_75pct AS INT64)                AS video_views_75pct,
    CAST(video_views_100pct AS INT64)               AS video_views_100pct,
    CAST(engagements AS INT64)                      AS engagements,
    CAST(platform_conversions AS NUMERIC)           AS platform_conversions,
    CAST(platform_conversion_value AS NUMERIC)      AS platform_conversion_value,
    platform_attribution_model,
    CAST(ctr AS FLOAT64)                            AS ctr,   -- rate: keep FLOAT64
    CAST(cpc AS NUMERIC)                            AS cpc,
    CAST(cpm AS NUMERIC)                            AS cpm,
    CAST(platform_cpa AS NUMERIC)                   AS platform_cpa,
    CAST(platform_roas AS NUMERIC)                  AS platform_roas,
    CAST(daily_budget AS NUMERIC)                   AS daily_budget,
    CAST(budget_utilization_pct AS FLOAT64)         AS budget_utilization_pct, -- rate
    CAST(projected_monthly_spend AS NUMERIC)        AS projected_monthly_spend,
    pacing_status,
    CAST(pacing_variance_pct AS FLOAT64)            AS pacing_variance_pct,    -- rate
    platform_metrics,
    -- New geo columns (NULL for historical rows)
    CAST(NULL AS STRING)                            AS geo_country_code,
    CAST(NULL AS STRING)                            AS geo_state_code,
    CAST(NULL AS STRING)                            AS geo_dma_code,
    CAST(NULL AS STRING)                            AS geo_region_name,
    ingested_at,
    data_source
FROM `{project}.{dataset}.platform_daily_spend_float64_backup`;

-- Step 3: Verify (run this separately before dropping backup)
-- SELECT COUNT(*) AS new_count FROM `{project}.{dataset}.platform_daily_spend`;
-- SELECT COUNT(*) AS backup_count FROM `{project}.{dataset}.platform_daily_spend_float64_backup`;
-- Step 4 (manual, after verification): DROP TABLE `{project}.{dataset}.platform_daily_spend_float64_backup`;


-- ── attribution_results ───────────────────────────────────────────────────────

ALTER TABLE `{project}.{dataset}.attribution_results`
    RENAME TO attribution_results_float64_backup;

CREATE TABLE `{project}.{dataset}.attribution_results`
PARTITION BY conversion_date
CLUSTER BY model_name, platform, channel, campaign_id
OPTIONS (description = "Weighted attribution credit per touchpoint per model run.")
AS SELECT
    result_id, run_id, path_id, touchpoint_id, conversion_id, entity_id,
    conversion_date, touchpoint_date, platform, channel, campaign_id,
    ad_group_id, ad_id, touchpoint_type, path_position, path_total_touches,
    conversion_type,
    CAST(conversion_value AS NUMERIC)   AS conversion_value,
    CAST(deal_value AS NUMERIC)         AS deal_value,
    CAST(credit_weight AS NUMERIC)      AS credit_weight,
    CAST(credit_conversions AS NUMERIC) AS credit_conversions,
    CAST(credit_value AS NUMERIC)       AS credit_value,
    CAST(credit_deal_value AS NUMERIC)  AS credit_deal_value,
    model_name, period_start, period_end, created_at
FROM `{project}.{dataset}.attribution_results_float64_backup`;


-- ── attribution_channel_summary ───────────────────────────────────────────────

ALTER TABLE `{project}.{dataset}.attribution_channel_summary`
    RENAME TO attribution_channel_summary_float64_backup;

CREATE TABLE `{project}.{dataset}.attribution_channel_summary`
PARTITION BY period_start
CLUSTER BY model_name, platform, channel
OPTIONS (description = "Pre-aggregated attribution results by channel.")
AS SELECT
    summary_id, run_id, model_name, period_start, period_end,
    platform, channel, conversion_type, funnel_stage,
    CAST(total_touches AS INT64)                AS total_touches,
    CAST(unique_entities AS INT64)              AS unique_entities,
    CAST(first_touch_count AS INT64)            AS first_touch_count,
    CAST(last_touch_count AS INT64)             AS last_touch_count,
    CAST(attributed_conversions AS NUMERIC)     AS attributed_conversions,
    CAST(attributed_value AS NUMERIC)           AS attributed_value,
    CAST(attributed_deal_value AS NUMERIC)      AS attributed_deal_value,
    CAST(credit_share_pct AS FLOAT64)           AS credit_share_pct,
    CAST(total_spend AS NUMERIC)                AS total_spend,
    currency,
    CAST(attributed_cpa AS NUMERIC)             AS attributed_cpa,
    CAST(attributed_roas AS NUMERIC)            AS attributed_roas,
    CAST(attributed_roi AS NUMERIC)             AS attributed_roi,
    CAST(platform_conversions AS NUMERIC)       AS platform_conversions,
    CAST(platform_cpa AS NUMERIC)               AS platform_cpa,
    CAST(attribution_vs_platform_delta_pct AS FLOAT64) AS attribution_vs_platform_delta_pct,
    generated_at
FROM `{project}.{dataset}.attribution_channel_summary_float64_backup`;


-- ============================================================================
-- SECTION C: Update DDL source file reference
-- ============================================================================
-- After running this migration, the following source DDL files have been updated
-- to use NUMERIC for financial columns in new deployments:
--
--   bigquery/03_platform.sql         — spend, budget, cpa, roas, cpm, cpc
--   bigquery/04_attribution.sql      — credit_weight, credit_value, deal_value
--   bigquery/06_reporting.sql        — spend_ad, spend_keyword tables
--   bigquery/07_account_analytics.sql — annual_revenue, deal_value, icp_score
--
-- Source files also include geo dimension columns (geo_country_code, geo_state_code,
-- geo_dma_code, geo_region_name) on all spend fact tables.
-- ============================================================================
