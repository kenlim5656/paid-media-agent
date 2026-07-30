-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — REDDIT ADS LAYER (Task 33)
-- =============================================================================
-- Output tables for the Reddit Ads API v3 client in tools/reddit_ads_client.py.
--
-- Tables in this file:
--   reddit_ads_runs          — run registry: one row per extraction run
--   reddit_daily_spend       — daily campaign + ad-group spend/performance
--   reddit_spatial_performance — geo-segmented performance by country + DMA region
--
-- Relationship to other schema layers:
--   Complements:
--     platform_daily_spend     (03_platform.sql) — normalized cross-platform spend
--     platform_campaigns       (03_platform.sql) — campaign metadata
--
--   Feeds Meridian MMM (Task 27):
--     reddit_spatial_performance.country_code → geo dimension
--     reddit_spatial_performance.dma_region   → sub-national DMA dimension
--     Both map to the geo_allowlist parameter of run_mmm_pipeline().
--
-- Account ID format:
--   Reddit Ads account IDs use the Reddit entity prefix scheme.
--   Ad account IDs begin with 't2_' (user accounts) or 'a2_' (ad accounts).
--   Every row that stores an account_id has been validated against these prefixes
--   by _validate_account_id() in tools/reddit_ads_client.py.
--
-- Financial fields:
--   All monetary columns (spend, cpc, cpm, daily_budget, total_budget) use
--   NUMERIC(38,9) to match the NUMERIC convention across paid-media-schema.
--   The client converts Reddit API micro-USD values to decimal USD before write.
--
-- Usage:
--   bq query --use_legacy_sql=false < 13_reddit_ads.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: reddit_ads_runs
-- =============================================================================
-- One row per extraction run. Consistent with tiktok_ads_runs / attribution_runs
-- / mmm_runs / causal_impact_runs run-registry pattern.
--
-- status values:
--   "completed" — all requested account IDs returned data without error
--   "partial"   — at least one account succeeded, at least one failed
--   "failed"    — all accounts failed (auth error, rate limit, API outage)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.reddit_ads_runs`
(
    run_id                  STRING    NOT NULL,  -- UUID

    -- ── Scope ─────────────────────────────────────────────────────────────────
    account_ids_processed   JSON,
    -- Array of ad account ID strings processed in this run.
    -- Example: ["t2_abc123", "t2_def456"]
    -- Every ID was validated against the t2_ / a2_ prefix requirement.

    start_date              DATE      NOT NULL,  -- reporting window start (inclusive)
    end_date                DATE      NOT NULL,  -- reporting window end (inclusive)
    time_zone_id            STRING,              -- IANA timezone applied to the reporting window
                                                 -- e.g. "America/New_York", "UTC"

    -- ── Results ───────────────────────────────────────────────────────────────
    campaigns_fetched       INT64,               -- distinct campaign IDs seen in this run
    spend_rows_written      INT64,               -- rows written to reddit_daily_spend
    spatial_rows_written    INT64,               -- rows written to reddit_spatial_performance

    -- ── Status ────────────────────────────────────────────────────────────────
    status                  STRING    NOT NULL,
    error_message           STRING,              -- populated on partial / failed

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_by              STRING,              -- "operator_agent" or caller identifier
    created_at              TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY status
OPTIONS (
    description = "One row per Reddit Ads API extraction run. Tracks account scope, reporting window, row counts written to reddit_daily_spend and reddit_spatial_performance, and run status."
);


-- =============================================================================
-- TABLE: reddit_daily_spend
-- =============================================================================
-- Daily performance metrics at campaign + ad-group grain.
-- Populated from GET /api/v3/ad_accounts/{account_id}/reports with
-- breakdown=DATE,CAMPAIGN,AD_GROUP.
--
-- Financial values (spend, cpc, cpm) are NUMERIC — converted from Reddit's
-- micro-USD response format by the client before insertion.
--
-- CTR is stored as FLOAT64 (rate; rounding drift is acceptable for ratios).
-- All impression / click / conversion counts are INT64.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.reddit_daily_spend`
(
    row_id                  STRING    NOT NULL,  -- UUID (generated at ingest time)
    run_id                  STRING    NOT NULL,  -- → reddit_ads_runs.run_id

    -- ── Account & campaign identifiers ────────────────────────────────────────
    account_id              STRING    NOT NULL,  -- Reddit ad account ID (t2_xxx or a2_xxx)
    campaign_id             STRING,              -- Reddit campaign ID
    campaign_name           STRING,
    campaign_objective      STRING,
    -- "BRAND_AWARENESS" | "VIDEO_VIEWS" | "TRAFFIC" | "CONVERSIONS"
    -- | "APP_INSTALLS" | "CATALOG_SALES" | "LEAD_GENERATION"

    ad_group_id             STRING,              -- Reddit ad group ID
    ad_group_name           STRING,

    -- ── Time ──────────────────────────────────────────────────────────────────
    date                    DATE      NOT NULL,  -- the reporting day (local time per time_zone_id)
    time_zone_id            STRING,              -- carried forward from the run

    -- ── Financial metrics (NUMERIC — never FLOAT64) ───────────────────────────
    spend                   NUMERIC,             -- USD spend for the day
    cpc                     NUMERIC,             -- cost per click (USD)
    cpm                     NUMERIC,             -- cost per 1,000 impressions (USD)
    ecpm                    NUMERIC,             -- effective CPM (spend / impressions × 1000)

    -- ── Volume metrics (INT64) ─────────────────────────────────────────────────
    impressions             INT64,
    clicks                  INT64,
    conversions             INT64,               -- post-click conversions (1-day)
    view_conversions        INT64,               -- post-view conversions

    -- ── Engagement / video metrics ─────────────────────────────────────────────
    ctr                     FLOAT64,             -- click-through rate (clicks / impressions)
    video_plays             INT64,               -- total video starts
    video_views_25pct       INT64,               -- reached 25% of video
    video_views_50pct       INT64,               -- reached 50%
    video_views_75pct       INT64,               -- reached 75%
    video_views_100pct      INT64,               -- completed view
    video_completion_rate   FLOAT64,             -- video_views_100pct / video_plays

    -- ── Derived ───────────────────────────────────────────────────────────────
    cost_per_conversion     NUMERIC,             -- spend / conversions

    -- ── Audit ─────────────────────────────────────────────────────────────────
    capture_timestamp       TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY campaign_id, ad_group_id
OPTIONS (
    description = "Daily Reddit Ads spend and performance at campaign + ad-group grain. Populated from the Reddit Ads API v3 reports endpoint with DATE × CAMPAIGN × AD_GROUP breakdown. NUMERIC for all financial fields."
);


-- =============================================================================
-- TABLE: reddit_spatial_performance
-- =============================================================================
-- Geo-segmented performance data at country and DMA region grain.
-- Populated from GET /api/v3/ad_accounts/{account_id}/reports with
-- breakdown=DATE_RANGE,CAMPAIGN,COUNTRY or DMA_REGION.
--
-- DMA (Designated Market Area) rows are US-only and may be null for
-- international account runs.
--
-- Primary downstream consumer: Meridian MMM (Task 27) geo parameter.
--   country_code → iso_country_code dimension in geo-level MMM model
--   dma_region   → finer sub-national geo split for US-focused MMM runs
--
-- The date range stored here is the aggregate window (not daily grain)
-- to match how Reddit's geo API collapses dates when COUNTRY or DMA_REGION
-- is the primary breakdown dimension.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.reddit_spatial_performance`
(
    row_id                  STRING    NOT NULL,  -- UUID (generated at ingest time)
    run_id                  STRING    NOT NULL,  -- → reddit_ads_runs.run_id

    -- ── Account & campaign ────────────────────────────────────────────────────
    account_id              STRING    NOT NULL,
    campaign_id             STRING,
    campaign_name           STRING,

    -- ── Reporting window ──────────────────────────────────────────────────────
    date_range_start        DATE      NOT NULL,
    date_range_end          DATE      NOT NULL,
    time_zone_id            STRING,

    -- ── Geographic dimensions ─────────────────────────────────────────────────
    country_code            STRING,
    -- ISO 3166-1 alpha-2 country code (e.g. "US", "GB", "CA").
    -- Always populated when the breakdown includes COUNTRY.

    dma_region              STRING,
    -- DMA region name (e.g. "New York", "Los Angeles") or DMA code.
    -- US only. Null for non-US rows or when only COUNTRY breakdown is requested.

    -- ── Metrics ───────────────────────────────────────────────────────────────
    spend                   NUMERIC,             -- USD spend for the period/geo
    impressions             INT64,
    clicks                  INT64,
    conversions             INT64,
    ctr                     FLOAT64,
    cpm                     NUMERIC,

    -- ── Audit ─────────────────────────────────────────────────────────────────
    capture_timestamp       TIMESTAMP NOT NULL
)
PARTITION BY date_range_start
CLUSTER BY country_code, campaign_id
OPTIONS (
    description = "Geo-segmented Reddit Ads performance by country and DMA region. Aggregate window grain (not daily). Feeds Meridian MMM geo dimension via country_code and dma_region columns."
);
