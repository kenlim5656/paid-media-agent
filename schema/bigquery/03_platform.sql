-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — PLATFORM LAYER
-- =============================================================================
-- Normalized campaign metadata and spend data from any ad platform.
-- The key design principle: one schema, many platforms.
-- Platform-specific fields live in a JSON `platform_data` column rather than
-- forcing every platform into a lowest-common-denominator structure.
--
-- Tables in this file:
--   platform_campaigns     Campaign-level metadata
--   platform_ad_groups     Ad group / line item / ad set metadata
--   platform_ads           Ad / creative metadata
--   platform_daily_spend   Daily performance metrics by campaign
--   platform_daily_spend_ad_group  Daily metrics at ad group level
-- =============================================================================


-- Supported platform values (reference):
-- "google_ads"     Google Ads (search, shopping, video, app)
-- "dv360"          Display & Video 360
-- "sa360"          Search Ads 360
-- "cm360"          Campaign Manager 360 (trafficking + measurement, not buying)
-- "meta"           Meta Ads (Facebook + Instagram)
-- "linkedin"       LinkedIn Campaign Manager
-- "tiktok"         TikTok Ads Manager
-- "snapchat"       Snapchat Ads Manager
-- "pinterest"      Pinterest Ads
-- "twitter_x"      X Ads Manager
-- "microsoft_ads"  Microsoft Advertising
-- "amazon_ads"     Amazon Advertising (DSP + Sponsored)
-- "reddit"         Reddit Ads
-- "custom"         Any other platform


-- -----------------------------------------------------------------------------
-- platform_campaigns
-- One row per campaign per platform. Platform IDs are preserved alongside
-- our internal campaign_id to enable cross-platform reporting.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_campaigns`
(
    campaign_id              STRING    NOT NULL,  -- internal UUID (stable across platform changes)
    platform                 STRING    NOT NULL,
    platform_campaign_id     STRING    NOT NULL,  -- native platform campaign ID
    platform_account_id      STRING,              -- MCC / agency / partner account ID

    -- Identity
    campaign_name            STRING    NOT NULL,
    campaign_name_normalized STRING,              -- lowercase, symbols stripped (for fuzzy matching)

    -- Classification
    objective                STRING,
    -- "awareness" | "consideration" | "traffic" | "lead_generation"
    -- "conversions" | "catalog_sales" | "app_installs" | "brand_safety"
    funnel_stage             STRING,              -- "upper" | "mid" | "lower"
    channel                  STRING,              -- standardized channel (see touchpoint_events)
    buying_type              STRING,              -- "auction" | "reservation" | "programmatic_guaranteed"
    campaign_type            STRING,              -- platform-specific: "search" | "pmax" | "display" | "video" | "shopping" | "app"

    -- Budget
    budget_amount            FLOAT64,
    budget_currency          STRING,
    budget_type              STRING,              -- "daily" | "lifetime" | "monthly"
    daily_budget             FLOAT64,             -- normalized to daily equivalent

    -- Flight
    start_date               DATE,
    end_date                 DATE,                -- null = always-on
    status                   STRING,              -- "active" | "paused" | "ended" | "draft" | "archived"

    -- Team / org context
    team_id                  STRING,              -- → MCP teams data
    brand                    STRING,
    region                   STRING,
    product_line             STRING,

    -- Tracking
    has_utm_tracking         BOOL,
    utm_source               STRING,
    utm_medium               STRING,
    utm_campaign             STRING,
    has_click_id_capture     BOOL,
    has_capi                 BOOL,

    -- Platform-specific fields (extensible, non-breaking)
    platform_data            JSON,
    -- Examples by platform:
    -- google_ads:  {"smart_bidding_strategy": "target_cpa", "target_cpa": 45.00, "ai_max": true}
    -- meta:        {"buying_type": "AUCTION", "special_ad_category": null, "advantage_plus": true}
    -- linkedin:    {"objective_type": "LEAD_GENERATION", "audience_network": false}
    -- dv360:       {"insertion_order_id": "...", "inventory_source": "open_exchange"}

    -- Audit
    first_seen_at            TIMESTAMP,
    last_synced_at           TIMESTAMP NOT NULL,
    ingested_at              TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY platform, status, team_id
OPTIONS (
    description = "Normalized campaign metadata across all ad platforms. One row per campaign."
);


-- -----------------------------------------------------------------------------
-- platform_ad_groups
-- Ad groups (Google Ads), line items (DV360), ad sets (Meta), etc.
-- All normalized to a single structure.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_ad_groups`
(
    ad_group_id              STRING    NOT NULL,
    campaign_id              STRING    NOT NULL,  -- → platform_campaigns.campaign_id
    platform                 STRING    NOT NULL,
    platform_ad_group_id     STRING    NOT NULL,
    platform_campaign_id     STRING    NOT NULL,

    ad_group_name            STRING    NOT NULL,
    status                   STRING,

    -- Targeting summary
    targeting_type           STRING,              -- "audience" | "keyword" | "contextual" | "placement" | "remarketing"
    audience_ids             ARRAY<STRING>,       -- platform audience IDs applied
    audience_description     STRING,              -- human-readable summary

    -- Bidding
    bid_strategy             STRING,              -- "manual_cpc" | "target_cpa" | "target_roas" | "maximize_conversions" | "vcpm"
    bid_amount               FLOAT64,
    target_cpa               FLOAT64,
    target_roas              FLOAT64,

    -- Budget (at ad group level, where applicable)
    budget_amount            FLOAT64,
    budget_type              STRING,

    -- Platform-specific
    platform_data            JSON,

    -- Audit
    last_synced_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_synced_at)
CLUSTER BY campaign_id, platform
OPTIONS (
    description = "Ad group / line item / ad set metadata. Child of platform_campaigns."
);


-- -----------------------------------------------------------------------------
-- platform_ads
-- Individual ads / creatives across all platforms.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_ads`
(
    ad_id                    STRING    NOT NULL,
    ad_group_id              STRING    NOT NULL,  -- → platform_ad_groups.ad_group_id
    campaign_id              STRING    NOT NULL,
    platform                 STRING    NOT NULL,
    platform_ad_id           STRING    NOT NULL,

    ad_name                  STRING,
    ad_type                  STRING,
    -- "rsa" | "esa" | "pmax_asset_group"  (Google)
    -- "image" | "video" | "carousel" | "collection" | "lead_form"  (Meta)
    -- "single_image" | "video" | "carousel" | "conversation"  (LinkedIn)
    -- "top_view" | "in_feed" | "spark_ad"  (TikTok)

    status                   STRING,
    creative_format          STRING,              -- standardized: "image" | "video" | "carousel" | "responsive" | "native" | "html5" | "text"
    creative_size            STRING,
    destination_url          STRING,              -- final landing page URL (without tracking params)

    -- Asset references (links to creative assets, not the assets themselves)
    headline                 STRING,              -- primary headline (for text-based ads)
    description              STRING,
    call_to_action           STRING,
    asset_ids                ARRAY<STRING>,       -- internal asset IDs if tracked

    -- Platform-specific
    platform_data            JSON,

    last_synced_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_synced_at)
CLUSTER BY campaign_id, ad_group_id
OPTIONS (
    description = "Individual ad / creative metadata across all platforms."
);


-- -----------------------------------------------------------------------------
-- platform_daily_spend
-- Daily performance metrics at campaign level. The primary spend/performance
-- table for reporting, pacing, and attribution cost allocation.
-- Platform-agnostic — every row has the same core metrics regardless of source.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_daily_spend`
(
    spend_id                 STRING    NOT NULL,  -- UUID or deterministic hash(platform+campaign_id+date)
    date                     DATE      NOT NULL,
    platform                 STRING    NOT NULL,
    campaign_id              STRING    NOT NULL,  -- → platform_campaigns.campaign_id
    platform_campaign_id     STRING    NOT NULL,

    -- Core spend & volume (every platform)
    spend                    NUMERIC   NOT NULL,  -- use NUMERIC (not FLOAT64) to avoid rounding errors
    currency                 STRING,
    impressions              INT64,
    clicks                   INT64,
    reach                    INT64,              -- unique users reached (where available)

    -- Engagement
    video_views              INT64,              -- platform's
    video_views_25pct        INT64,
    video_views_50pct        INT64,
    video_views_75pct        INT64,
    video_views_100pct       INT64,
    engagements              INT64,              -- likes, shares, comments, saves (social platforms)

    -- Conversions — platform-reported (for reconciliation only; use conversion_events for attribution)
    platform_conversions     NUMERIC,            -- platform's own conversion count
    platform_conversion_value NUMERIC,           -- platform's own conversion value
    platform_attribution_model STRING,           -- what model the platform used (e.g. "7d_click_1d_view")

    -- Derived metrics (can be computed but stored for performance)
    ctr                      FLOAT64,            -- clicks / impressions (rate: FLOAT64 is fine)
    cpc                      NUMERIC,            -- spend / clicks
    cpm                      NUMERIC,            -- spend / impressions * 1000
    platform_cpa             NUMERIC,            -- spend / platform_conversions
    platform_roas            NUMERIC,            -- platform_conversion_value / spend

    -- Budget context (snapshot at time of import)
    daily_budget             NUMERIC,
    budget_utilization_pct   FLOAT64,            -- spend / daily_budget * 100 (rate)

    -- Pacing (populated by the Watchdog/Analyst agents)
    projected_monthly_spend  NUMERIC,
    pacing_status            STRING,             -- "on_pace" | "over_pacing" | "under_pacing"
    pacing_variance_pct      FLOAT64,            -- variance % (rate)

    -- Geographic dimensions (required for Meridian MMM geo-level modeling)
    -- Populated from platform geo reports (Google Ads geo report, Meta location breakdown, etc.)
    geo_country_code         STRING,             -- ISO 3166-1 alpha-2 (e.g., "US")
    geo_state_code           STRING,             -- state / province code (e.g., "CA")
    geo_dma_code             STRING,             -- Nielsen DMA code (e.g., "807" = San Francisco)
    geo_region_name          STRING,             -- human-readable region (e.g., "California")

    -- Platform-specific metrics (non-breaking extension)
    platform_metrics         JSON,
    -- Examples:
    -- google_ads:  {"quality_score_avg": 7.2, "impression_share": 0.68, "search_lost_is_budget": 0.12}
    -- meta:        {"frequency": 2.4, "cpp": 12.50, "thumbstop_rate": 0.22}
    -- linkedin:    {"social_actions": 142, "follows": 12, "job_applications": 0}
    -- dv360:       {"viewability_rate": 0.71, "begin_to_render_rate": 0.88, "active_view_rate": 0.64}

    -- Audit
    ingested_at              TIMESTAMP NOT NULL,
    data_source              STRING               -- "platform_api" | "bigquery_export" | "manual_import"
)
PARTITION BY date
CLUSTER BY platform, campaign_id
OPTIONS (
    description = "Daily spend and performance metrics by campaign across all platforms. NUMERIC types for all financial columns."
);


-- -----------------------------------------------------------------------------
-- platform_daily_spend_ad_group
-- Same structure as platform_daily_spend but at the ad group / line item level.
-- More granular — enables ad group optimization and audience-level analysis.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_daily_spend_ad_group`
(
    spend_id                 STRING    NOT NULL,
    date                     DATE      NOT NULL,
    platform                 STRING    NOT NULL,
    campaign_id              STRING    NOT NULL,
    ad_group_id              STRING    NOT NULL,
    platform_ad_group_id     STRING    NOT NULL,

    spend                    FLOAT64   NOT NULL,
    currency                 STRING,
    impressions              INT64,
    clicks                   INT64,
    video_views              INT64,
    engagements              INT64,
    platform_conversions     FLOAT64,
    platform_conversion_value FLOAT64,

    ctr                      FLOAT64,
    cpc                      FLOAT64,
    cpm                      FLOAT64,
    platform_cpa             FLOAT64,

    platform_metrics         JSON,

    ingested_at              TIMESTAMP NOT NULL,
    data_source              STRING
)
PARTITION BY date
CLUSTER BY campaign_id, ad_group_id
OPTIONS (
    description = "Daily spend and performance metrics at ad group / line item level."
);
