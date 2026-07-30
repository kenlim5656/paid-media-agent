-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — REPORTING LAYER
-- =============================================================================
-- Pre-built SQL views and granular spend tables for common reporting use cases.
-- All views reference the latest completed attribution run automatically.
--
-- New tables in this file:
--   platform_keywords              Keyword metadata (search platforms)
--   platform_daily_spend_ad        Ad-level daily metrics
--   platform_daily_spend_keyword   Keyword-level daily metrics
--
-- Views in this file:
--   v_campaign_performance         Campaign spend + MTA attribution in one row
--   v_pacing_status                Active campaign pacing vs. expected
--   v_roas_comparison              Platform ROAS vs MTA ROAS vs attributed ROI
--   v_channel_efficiency           Cross-channel efficiency for budget decisions
--   v_ad_performance               Ad/creative performance with attribution
--   v_keyword_performance          Keyword spend + platform metrics
--   v_daily_performance            Day-by-day trend view (all campaigns)
--
-- ROAS definitions (use consistently across dashboards):
--   platform_roas     = platform_conversion_value / spend  (platform-reported, inflated)
--   attributed_roas   = credited conversion value / spend  (MTA model, cross-channel truth)
--   margin_roi        = (attributed_value × margin_pct - spend) / spend  (true profitability)
--
-- To populate margin_roi, set platform_data.margin_pct on platform_campaigns rows
-- (e.g. {"margin_pct": 0.65} for a 65% gross margin product). Defaults to NULL if absent.
-- =============================================================================


-- =============================================================================
-- TABLE: platform_keywords
-- =============================================================================
-- Keyword metadata for search platforms. Child of platform_ad_groups.
-- Supported platforms: google_ads, sa360, microsoft_ads
-- Negative keywords are included (negative = TRUE) — important for reporting gaps.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_keywords`
(
    keyword_id               STRING    NOT NULL,  -- internal UUID
    ad_group_id              STRING    NOT NULL,  -- → platform_ad_groups.ad_group_id
    campaign_id              STRING    NOT NULL,  -- → platform_campaigns.campaign_id
    platform                 STRING    NOT NULL,  -- "google_ads" | "sa360" | "microsoft_ads"
    platform_keyword_id      STRING    NOT NULL,  -- native platform keyword ID

    -- Keyword definition
    keyword_text             STRING    NOT NULL,  -- the actual keyword
    match_type               STRING    NOT NULL,
    -- "exact"                Exact match [keyword]
    -- "phrase"               Phrase match "keyword"
    -- "broad"                Broad match keyword (including Smart Bidding variants)
    -- "broad_match_modifier" Deprecated BMM +keyword (kept for historical data)
    -- "negative_exact"       Negative exact -[keyword]
    -- "negative_phrase"      Negative phrase -"keyword"
    -- "negative_broad"       Negative broad -keyword
    negative                 BOOL,

    status                   STRING,             -- "active" | "paused" | "removed"

    -- Bidding
    bid_amount               FLOAT64,            -- max CPC bid (null if automated)
    bid_type                 STRING,             -- "manual_cpc" | "target_cpa" | "target_roas" | "enhanced_cpc"
    target_cpa               FLOAT64,            -- keyword-level target CPA (if set)

    -- Quality metrics (Google Ads / SA360 only)
    quality_score            INT64,              -- 1–10 (NULL for Microsoft Ads)
    expected_ctr_score       STRING,             -- "above_average" | "average" | "below_average"
    ad_relevance_score       STRING,             -- "above_average" | "average" | "below_average"
    landing_page_experience_score STRING,        -- "above_average" | "average" | "below_average"

    -- Platform-specific
    platform_data            JSON,
    -- google_ads:   {"label_ids": ["..."], "url_custom_parameters": {...}}
    -- microsoft_ads: {"bid_adjustment": 0.15, "extended_text_ad_preview": true}

    -- Audit
    first_seen_at            TIMESTAMP,
    last_synced_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(last_synced_at)
CLUSTER BY campaign_id, ad_group_id, match_type
OPTIONS (
    description = "Keyword metadata for search platforms. Includes negatives. Child of platform_ad_groups."
);


-- =============================================================================
-- TABLE: platform_daily_spend_ad
-- =============================================================================
-- Daily performance metrics at the individual ad / creative level.
-- Enables creative performance analysis and ad-level attribution joins.
-- Supported for all platforms that expose ad-level data via API.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_daily_spend_ad`
(
    spend_id                 STRING    NOT NULL,  -- UUID or hash(platform+ad_id+date)
    date                     DATE      NOT NULL,
    platform                 STRING    NOT NULL,
    campaign_id              STRING    NOT NULL,  -- → platform_campaigns.campaign_id
    ad_group_id              STRING    NOT NULL,  -- → platform_ad_groups.ad_group_id
    ad_id                    STRING    NOT NULL,  -- → platform_ads.ad_id
    platform_ad_id           STRING    NOT NULL,

    -- Core metrics
    spend                    FLOAT64   NOT NULL,
    currency                 STRING,
    impressions              INT64,
    clicks                   INT64,
    reach                    INT64,

    -- Engagement
    video_views              INT64,
    video_views_25pct        INT64,
    video_views_50pct        INT64,
    video_views_75pct        INT64,
    video_views_100pct       INT64,
    engagements              INT64,              -- likes, comments, shares, saves
    link_clicks              INT64,              -- distinct from total clicks (meta)

    -- Platform-reported conversions (for reconciliation; use attribution_results for decisions)
    platform_conversions     FLOAT64,
    platform_conversion_value FLOAT64,

    -- Derived
    ctr                      FLOAT64,            -- clicks / impressions
    cpc                      FLOAT64,            -- spend / clicks
    cpm                      FLOAT64,            -- spend / impressions * 1000
    platform_cpa             FLOAT64,            -- spend / platform_conversions
    platform_roas            FLOAT64,            -- platform_conversion_value / spend

    -- Creative quality signals (where available)
    thumbstop_rate           FLOAT64,            -- 3-sec video views / impressions (Meta/TikTok)
    hook_rate                FLOAT64,            -- alias for thumbstop_rate
    frequency                FLOAT64,            -- avg impressions per unique user (Meta)
    relevance_score          FLOAT64,            -- Meta ad relevance score (0–100)

    -- Platform-specific
    platform_metrics         JSON,
    -- meta:     {"canvas_avg_view_pct": 0.84, "social_spend": 12.40}
    -- linkedin: {"viral_impressions": 340, "viral_clicks": 28}
    -- tiktok:   {"average_video_play": 8.2, "profile_visits": 45}
    -- dv360:    {"viewability_rate": 0.72, "begin_to_render_rate": 0.91}

    -- Audit
    ingested_at              TIMESTAMP NOT NULL,
    data_source              STRING
)
PARTITION BY date
CLUSTER BY campaign_id, ad_group_id, ad_id
OPTIONS (
    description = "Daily spend and performance at individual ad/creative level. Enables creative testing analysis."
);


-- =============================================================================
-- TABLE: platform_daily_spend_keyword
-- =============================================================================
-- Daily performance at the keyword level for search campaigns.
-- Also includes search term level data where platform APIs provide it.
-- Supported platforms: google_ads, sa360, microsoft_ads
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.platform_daily_spend_keyword`
(
    spend_id                 STRING    NOT NULL,
    date                     DATE      NOT NULL,
    platform                 STRING    NOT NULL,
    campaign_id              STRING    NOT NULL,
    ad_group_id              STRING    NOT NULL,
    keyword_id               STRING,             -- → platform_keywords.keyword_id (null for search term rows)
    platform_keyword_id      STRING,

    -- Row type
    row_type                 STRING    NOT NULL,
    -- "keyword"              Aggregated by keyword ()
    -- "search_term"          Individual search query that triggered the keyword
    keyword_text             STRING    NOT NULL,  -- keyword or actual search term
    match_type               STRING,
    triggered_keyword_text   STRING,              -- populated when row_type = "search_term"

    -- Core metrics
    spend                    FLOAT64   NOT NULL,
    currency                 STRING,
    impressions              INT64,
    clicks                   INT64,

    -- Derived
    ctr                      FLOAT64,
    cpc                      FLOAT64,
    cpm                      FLOAT64,            -- for display campaigns with keyword targets

    -- Platform-reported conversions
    platform_conversions     FLOAT64,
    platform_conversion_value FLOAT64,
    platform_cpa             FLOAT64,
    platform_roas            FLOAT64,

    -- Search-specific signals
    impression_share         FLOAT64,            -- % of eligible impressions won
    impression_share_lost_budget  FLOAT64,       -- IS lost to budget
    impression_share_lost_rank    FLOAT64,       -- IS lost to rank / quality
    search_volume            INT64,              -- monthly search volume (where available)
    competition_level        STRING,             -- "low" | "medium" | "high"
    avg_position             FLOAT64,            -- deprecated in Google Ads but still in SA360

    -- Quality snapshot at time of import (from platform_keywords)
    quality_score            INT64,
    expected_ctr_score       STRING,
    ad_relevance_score       STRING,
    landing_page_experience_score STRING,

    -- Platform-specific
    platform_metrics         JSON,

    -- Audit
    ingested_at              TIMESTAMP NOT NULL,
    data_source              STRING
)
PARTITION BY date
CLUSTER BY campaign_id, ad_group_id, keyword_id
OPTIONS (
    description = "Daily keyword and search term performance for search platforms. Supports keyword-level and search-term-level rows."
);


-- =============================================================================
-- VIEW: v_campaign_performance
-- =============================================================================
-- One row per campaign. Joins campaign metadata, aggregated spend, and
-- MTA attribution credit from the latest completed run.
--
-- Key columns:
--   platform_roas       Platform's own ROAS (inflated — use for in-platform reference only)
--   attributed_roas     MTA model ROAS (cross-channel truth — use for budget decisions)
--   attributed_pipeline_roas  Pipeline value / spend (B2B: deal_value-based)
--   platform_vs_mta_delta_pct  % difference between platform-reported and MTA conversions
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_campaign_performance` AS

WITH latest_run AS (
    SELECT run_id, model_name, period_start, period_end
    FROM `{project}.{dataset}.attribution_runs`
    WHERE status = 'completed'
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
),

spend_totals AS (
    SELECT
        s.campaign_id,
        SUM(s.spend)                     AS total_spend,
        SUM(s.impressions)               AS total_impressions,
        SUM(s.clicks)                    AS total_clicks,
        SUM(s.video_views)               AS total_video_views,
        SUM(s.engagements)               AS total_engagements,
        SUM(s.platform_conversions)      AS total_platform_conversions,
        SUM(s.platform_conversion_value) AS total_platform_conversion_value,
        MAX(s.date)                      AS last_spend_date,
        MIN(s.date)                      AS first_spend_date,
        COUNT(DISTINCT s.date)           AS days_with_spend
    FROM `{project}.{dataset}.platform_daily_spend` s
    GROUP BY s.campaign_id
),

attribution_by_campaign AS (
    SELECT
        ar.campaign_id,
        lr.model_name,
        SUM(ar.credit_conversions)  AS attributed_conversions,
        SUM(ar.credit_value)        AS attributed_value,
        SUM(ar.credit_deal_value)   AS attributed_deal_value
    FROM `{project}.{dataset}.attribution_results` ar
    JOIN latest_run lr ON ar.run_id = lr.run_id
    WHERE ar.campaign_id IS NOT NULL
    GROUP BY ar.campaign_id, lr.model_name
)

SELECT
    -- Campaign identity
    c.campaign_id,
    c.platform,
    c.platform_campaign_id,
    c.campaign_name,
    c.funnel_stage,
    c.channel,
    c.objective,
    c.campaign_type,
    c.buying_type,
    c.status,

    -- Flight
    c.start_date,
    c.end_date,
    c.budget_amount,
    c.budget_type,
    c.daily_budget,
    c.budget_currency                                        AS currency,

    -- Team / org context
    c.team_id,
    c.brand,
    c.region,
    c.product_line,

    -- Tracking setup (quality flags)
    c.has_utm_tracking,
    c.has_click_id_capture,
    c.has_capi,

    -- Spend totals
    COALESCE(s.total_spend, 0)                               AS total_spend,
    s.total_impressions,
    s.total_clicks,
    s.total_video_views,
    s.total_engagements,
    s.first_spend_date,
    s.last_spend_date,
    s.days_with_spend,

    -- Derived spend metrics
    SAFE_DIVIDE(s.total_clicks, s.total_impressions)         AS ctr,
    SAFE_DIVIDE(s.total_spend, s.total_clicks)               AS cpc,
    SAFE_DIVIDE(s.total_spend * 1000, s.total_impressions)   AS cpm,

    -- Platform-reported conversions (for reference and reconciliation only)
    s.total_platform_conversions                             AS platform_conversions,
    s.total_platform_conversion_value                        AS platform_conversion_value,
    SAFE_DIVIDE(s.total_spend, s.total_platform_conversions) AS platform_cpa,
    SAFE_DIVIDE(s.total_platform_conversion_value, s.total_spend) AS platform_roas,

    -- MTA-attributed metrics (use these for cross-channel decisions)
    COALESCE(a.attributed_conversions, 0)                    AS attributed_conversions,
    a.attributed_value,
    a.attributed_deal_value,
    a.model_name                                             AS attribution_model,
    SAFE_DIVIDE(s.total_spend, a.attributed_conversions)     AS attributed_cpa,
    SAFE_DIVIDE(a.attributed_value, s.total_spend)           AS attributed_roas,
    SAFE_DIVIDE(a.attributed_deal_value, s.total_spend)      AS attributed_pipeline_roas,

    -- Margin-adjusted ROI (requires margin_pct in platform_data)
    -- Set platform_data = JSON '{"margin_pct": 0.65}' on the campaign row to enable
    SAFE_DIVIDE(
        a.attributed_value
            * CAST(JSON_VALUE(c.platform_data, '$.margin_pct') AS FLOAT64)
        - s.total_spend,
        s.total_spend
    )                                                        AS margin_roi,
    CAST(JSON_VALUE(c.platform_data, '$.margin_pct') AS FLOAT64) AS margin_pct,

    -- Attribution vs. platform discrepancy
    SAFE_DIVIDE(
        a.attributed_conversions - s.total_platform_conversions,
        s.total_platform_conversions
    ) * 100                                                  AS platform_vs_mta_delta_pct

FROM `{project}.{dataset}.platform_campaigns` c
LEFT JOIN spend_totals s             ON c.campaign_id = s.campaign_id
LEFT JOIN attribution_by_campaign a  ON c.campaign_id = a.campaign_id;


-- =============================================================================
-- VIEW: v_pacing_status
-- =============================================================================
-- Pacing analysis for all campaigns with an active or paused status and a
-- flight that has started. Computes expected vs. actual spend and flags
-- campaigns that are over- or under-delivering.
--
-- Pacing thresholds (aligned with AGENT.md / budget-pacing skill):
--   overpacing:   pacing_pct > 110%
--   on_pace:      90% ≤ pacing_pct ≤ 110%
--   underpacing:  pacing_pct < 90%
--
-- For always-on campaigns (end_date IS NULL), pacing is computed vs. daily_budget.
-- For flighted campaigns, pacing uses the lifetime budget pro-rated to today.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_pacing_status` AS

WITH spend_to_date AS (
    SELECT
        campaign_id,
        SUM(spend)           AS actual_spend_to_date,
        MIN(date)            AS first_spend_date,
        MAX(date)            AS last_spend_date,
        COUNT(DISTINCT date) AS days_with_spend
    FROM `{project}.{dataset}.platform_daily_spend`
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
    GROUP BY campaign_id
),

spend_last_7d AS (
    SELECT campaign_id, SUM(spend) AS spend_last_7d
    FROM `{project}.{dataset}.platform_daily_spend`
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY campaign_id
),

spend_last_30d AS (
    SELECT campaign_id, SUM(spend) AS spend_last_30d
    FROM `{project}.{dataset}.platform_daily_spend`
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    GROUP BY campaign_id
),

-- Yesterday's spend (single-day signal for immediate pacing alerts)
spend_yesterday AS (
    SELECT campaign_id, SUM(spend) AS spend_yesterday
    FROM `{project}.{dataset}.platform_daily_spend`
    WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY campaign_id
),

campaign_with_pacing AS (
    SELECT
        c.campaign_id,
        c.platform,
        c.campaign_name,
        c.status,
        c.funnel_stage,
        c.channel,
        c.team_id,
        c.brand,
        c.region,
        c.start_date,
        c.end_date,
        c.budget_amount,
        c.budget_type,
        c.daily_budget,
        c.budget_currency                                         AS currency,

        -- Flight progress
        DATE_DIFF(CURRENT_DATE(), c.start_date, DAY) + 1         AS days_elapsed,
        CASE
            WHEN c.end_date IS NOT NULL
            THEN DATE_DIFF(c.end_date, c.start_date, DAY) + 1
            ELSE NULL
        END                                                       AS flight_days_total,
        CASE
            WHEN c.end_date IS NOT NULL
            THEN DATE_DIFF(c.end_date, CURRENT_DATE(), DAY)
            ELSE NULL
        END                                                       AS days_remaining,

        -- Expected spend to date (the denominator of pacing %)
        CASE
            WHEN c.budget_type = 'lifetime' AND c.end_date IS NOT NULL
            THEN c.budget_amount
                * SAFE_DIVIDE(
                    DATE_DIFF(CURRENT_DATE(), c.start_date, DAY) + 1,
                    DATE_DIFF(c.end_date, c.start_date, DAY) + 1
                  )
            WHEN c.budget_type IN ('daily', 'monthly') AND c.daily_budget IS NOT NULL
            THEN c.daily_budget * (DATE_DIFF(CURRENT_DATE(), c.start_date, DAY) + 1)
            ELSE NULL
        END                                                       AS expected_spend_to_date,

        -- Required daily to finish on budget
        CASE
            WHEN c.budget_type = 'lifetime' AND c.end_date IS NOT NULL
            THEN SAFE_DIVIDE(
                    c.budget_amount - COALESCE(s.actual_spend_to_date, 0),
                    GREATEST(DATE_DIFF(c.end_date, CURRENT_DATE(), DAY), 1)
                 )
            ELSE NULL
        END                                                       AS required_daily_spend,

        -- Actuals
        COALESCE(s.actual_spend_to_date, 0)                      AS actual_spend_to_date,
        s.first_spend_date,
        s.last_spend_date,
        s.days_with_spend,
        s7.spend_last_7d,
        s30.spend_last_30d,
        sy.spend_yesterday,
        SAFE_DIVIDE(s7.spend_last_7d, 7)                         AS daily_run_rate_7d,
        SAFE_DIVIDE(s30.spend_last_30d, 30)                      AS daily_run_rate_30d,

        -- Budget remaining (for lifetime budgets)
        CASE
            WHEN c.budget_type = 'lifetime'
            THEN c.budget_amount - COALESCE(s.actual_spend_to_date, 0)
            ELSE NULL
        END                                                       AS budget_remaining

    FROM `{project}.{dataset}.platform_campaigns` c
    LEFT JOIN spend_to_date   s   ON c.campaign_id = s.campaign_id
    LEFT JOIN spend_last_7d   s7  ON c.campaign_id = s7.campaign_id
    LEFT JOIN spend_last_30d  s30 ON c.campaign_id = s30.campaign_id
    LEFT JOIN spend_yesterday sy  ON c.campaign_id = sy.campaign_id
    WHERE c.status IN ('active', 'paused')
      AND c.start_date <= CURRENT_DATE()
)

SELECT
    *,

    -- Pacing percentage
    SAFE_DIVIDE(actual_spend_to_date, expected_spend_to_date) * 100 AS pacing_pct,

    -- Pacing status label
    CASE
        WHEN expected_spend_to_date IS NULL THEN 'no_budget_data'
        WHEN SAFE_DIVIDE(actual_spend_to_date, expected_spend_to_date) * 100 > 110 THEN 'overpacing'
        WHEN SAFE_DIVIDE(actual_spend_to_date, expected_spend_to_date) * 100 < 90  THEN 'underpacing'
        ELSE 'on_pace'
    END                                                              AS pacing_status,

    -- Pacing variance (actual - expected, positive = overpacing)
    actual_spend_to_date - COALESCE(expected_spend_to_date, 0)      AS pacing_variance_amount,

    -- Projected end-of-flight spend at current 7-day run rate
    CASE
        WHEN end_date IS NOT NULL AND daily_run_rate_7d IS NOT NULL
        THEN actual_spend_to_date
            + daily_run_rate_7d * GREATEST(DATE_DIFF(end_date, CURRENT_DATE(), DAY), 0)
        ELSE NULL
    END                                                              AS projected_total_spend,

    -- Is projected spend within 10% of budget?
    CASE
        WHEN budget_amount IS NOT NULL AND end_date IS NOT NULL AND daily_run_rate_7d IS NOT NULL
        THEN ABS(
                SAFE_DIVIDE(
                    (actual_spend_to_date
                        + daily_run_rate_7d * GREATEST(DATE_DIFF(end_date, CURRENT_DATE(), DAY), 0)
                    ) - budget_amount,
                    budget_amount
                )
             ) <= 0.10
        ELSE NULL
    END                                                              AS projected_on_budget

FROM campaign_with_pacing;


-- =============================================================================
-- VIEW: v_roas_comparison
-- =============================================================================
-- Side-by-side comparison of three ROAS / ROI numbers per channel:
--   1. platform_roas    — what each platform reports (always inflated)
--   2. attributed_roas  — MTA model output (cross-channel truth)
--   3. margin_roi       — (attributed_value × margin - spend) / spend (true profitability)
--
-- Useful for: identifying which platform's self-reported numbers are most
-- inflated, and making cross-channel budget allocation decisions.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_roas_comparison` AS

WITH latest_run AS (
    SELECT run_id, model_name, period_start, period_end
    FROM `{project}.{dataset}.attribution_runs`
    WHERE status = 'completed'
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
),

platform_spend_by_channel AS (
    SELECT
        s.platform,
        c.channel,
        c.funnel_stage,
        SUM(s.spend)                         AS total_spend,
        SUM(s.impressions)                   AS total_impressions,
        SUM(s.clicks)                        AS total_clicks,
        SUM(s.platform_conversions)          AS total_platform_conversions,
        SUM(s.platform_conversion_value)     AS total_platform_conversion_value,
        MIN(s.date)                          AS period_start,
        MAX(s.date)                          AS period_end
    FROM `{project}.{dataset}.platform_daily_spend` s
    LEFT JOIN `{project}.{dataset}.platform_campaigns` c USING (campaign_id)
    GROUP BY s.platform, c.channel, c.funnel_stage
),

-- Use attribution_channel_summary for MTA numbers (pre-aggregated, fast)
mta_by_channel AS (
    SELECT
        acs.platform,
        acs.channel,
        acs.conversion_type,
        acs.attributed_conversions,
        acs.attributed_value,
        acs.attributed_deal_value,
        acs.total_spend                      AS mta_reported_spend,
        acs.attributed_cpa,
        acs.attributed_roas,
        acs.attributed_roi,
        acs.credit_share_pct
    FROM `{project}.{dataset}.attribution_channel_summary` acs
    JOIN latest_run lr ON acs.run_id = lr.run_id
),

-- Margin per channel: weighted average from campaigns in that channel
channel_margin AS (
    SELECT
        platform,
        channel,
        AVG(CAST(JSON_VALUE(platform_data, '$.margin_pct') AS FLOAT64)) AS avg_margin_pct
    FROM `{project}.{dataset}.platform_campaigns`
    WHERE JSON_VALUE(platform_data, '$.margin_pct') IS NOT NULL
    GROUP BY platform, channel
)

SELECT
    sp.platform,
    sp.channel,
    sp.funnel_stage,
    sp.period_start,
    sp.period_end,

    -- Spend
    sp.total_spend,
    sp.total_impressions,
    sp.total_clicks,
    SAFE_DIVIDE(sp.total_spend * 1000, sp.total_impressions) AS cpm,
    SAFE_DIVIDE(sp.total_spend, sp.total_clicks)             AS cpc,

    -- 1) Platform-reported (use for in-platform budgeting only)
    sp.total_platform_conversions                            AS platform_conversions,
    sp.total_platform_conversion_value                       AS platform_conversion_value,
    SAFE_DIVIDE(sp.total_spend, sp.total_platform_conversions) AS platform_cpa,
    SAFE_DIVIDE(sp.total_platform_conversion_value, sp.total_spend) AS platform_roas,

    -- 2) MTA-attributed (use for cross-channel decisions)
    mta.conversion_type,
    mta.attributed_conversions,
    mta.attributed_value,
    mta.attributed_deal_value,
    mta.attributed_cpa,
    mta.attributed_roas,
    mta.attributed_roi,
    mta.credit_share_pct,

    -- 3) Margin-adjusted ROI (requires margin_pct on platform_campaigns)
    cm.avg_margin_pct                                        AS margin_pct,
    SAFE_DIVIDE(
        mta.attributed_value * cm.avg_margin_pct - sp.total_spend,
        sp.total_spend
    )                                                        AS margin_roi,

    -- Platform inflation ratio (how much platform over-counts vs MTA)
    SAFE_DIVIDE(
        sp.total_platform_conversions - mta.attributed_conversions,
        mta.attributed_conversions
    ) * 100                                                  AS platform_overcount_pct

FROM platform_spend_by_channel sp
LEFT JOIN mta_by_channel mta  ON sp.platform = mta.platform AND sp.channel = mta.channel
LEFT JOIN channel_margin cm   ON sp.platform = cm.platform  AND sp.channel = cm.channel;


-- =============================================================================
-- VIEW: v_channel_efficiency
-- =============================================================================
-- Rolled-up cross-channel efficiency view for portfolio-level budget decisions.
-- Compares channels on attributed CPA, ROAS, and share of attributed pipeline.
-- One row per channel (aggregated across all campaigns and platforms in that channel).
--
-- Use this view to answer: "Which channels should we put more money into?"
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_channel_efficiency` AS

WITH latest_run AS (
    SELECT run_id, model_name, period_start, period_end
    FROM `{project}.{dataset}.attribution_runs`
    WHERE status = 'completed'
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
),

channel_spend AS (
    SELECT
        c.channel,
        c.funnel_stage,
        SUM(s.spend)            AS total_spend,
        SUM(s.impressions)      AS total_impressions,
        SUM(s.clicks)           AS total_clicks,
        COUNT(DISTINCT c.campaign_id) AS active_campaigns
    FROM `{project}.{dataset}.platform_daily_spend` s
    JOIN `{project}.{dataset}.platform_campaigns` c USING (campaign_id)
    GROUP BY c.channel, c.funnel_stage
),

channel_attribution AS (
    SELECT
        acs.channel,
        acs.model_name,
        SUM(acs.attributed_conversions)  AS attributed_conversions,
        SUM(acs.attributed_value)        AS attributed_value,
        SUM(acs.attributed_deal_value)   AS attributed_deal_value,
        SUM(acs.total_touches)           AS total_touches,
        SUM(acs.unique_entities)         AS unique_entities,
        SUM(acs.first_touch_count)       AS first_touch_count,
        SUM(acs.last_touch_count)        AS last_touch_count,
        SUM(acs.total_spend)             AS attribution_period_spend
    FROM `{project}.{dataset}.attribution_channel_summary` acs
    JOIN latest_run lr ON acs.run_id = lr.run_id
    GROUP BY acs.channel, acs.model_name
),

total_attributed AS (
    SELECT
        SUM(attributed_conversions) AS grand_total_conversions,
        SUM(attributed_value)       AS grand_total_value,
        SUM(attributed_deal_value)  AS grand_total_pipeline
    FROM channel_attribution
)

SELECT
    cs.channel,
    cs.funnel_stage,
    ca.model_name                                            AS attribution_model,

    -- Scale
    cs.active_campaigns,
    cs.total_spend,
    cs.total_impressions,
    cs.total_clicks,

    -- Attribution credit
    ca.attributed_conversions,
    ca.attributed_value,
    ca.attributed_deal_value,
    ca.total_touches,
    ca.unique_entities,
    ca.first_touch_count,
    ca.last_touch_count,

    -- Channel credit share (% of total attributed pipeline)
    SAFE_DIVIDE(ca.attributed_conversions, ta.grand_total_conversions) * 100 AS conversion_share_pct,
    SAFE_DIVIDE(ca.attributed_value, ta.grand_total_value) * 100             AS value_share_pct,
    SAFE_DIVIDE(ca.attributed_deal_value, ta.grand_total_pipeline) * 100     AS pipeline_share_pct,

    -- Efficiency
    SAFE_DIVIDE(cs.total_spend, ca.attributed_conversions)  AS attributed_cpa,
    SAFE_DIVIDE(ca.attributed_value, cs.total_spend)        AS attributed_roas,
    SAFE_DIVIDE(ca.attributed_deal_value, cs.total_spend)   AS attributed_pipeline_roas,

    -- Spend share (for over/under-investment analysis)
    SAFE_DIVIDE(cs.total_spend, SUM(cs.total_spend) OVER ()) * 100 AS spend_share_pct,

    -- Investment efficiency signal
    -- Positive = channel is getting less spend than its attribution share warrants
    -- Negative = channel is getting more spend than its attribution share warrants
    SAFE_DIVIDE(ca.attributed_deal_value, ta.grand_total_pipeline) * 100
        - SAFE_DIVIDE(cs.total_spend, SUM(cs.total_spend) OVER ()) * 100 AS pipeline_vs_spend_gap_pct

FROM channel_spend cs
LEFT JOIN channel_attribution ca  ON cs.channel = ca.channel
CROSS JOIN total_attributed ta;


-- =============================================================================
-- VIEW: v_ad_performance
-- =============================================================================
-- Ad/creative level performance combining spend metrics with MTA attribution.
-- Attribution is joined at the ad_id level — requires the attribution_results
-- table to have ad_id populated (the Analyst agent fills this when stitching).
--
-- Use this view for: creative testing analysis, fatigue detection, and
-- identifying which ad formats drive the most attributed conversions.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_ad_performance` AS

WITH latest_run AS (
    SELECT run_id, model_name
    FROM `{project}.{dataset}.attribution_runs`
    WHERE status = 'completed'
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
),

ad_spend AS (
    SELECT
        da.ad_id,
        da.ad_group_id,
        da.campaign_id,
        da.platform,
        SUM(da.spend)               AS total_spend,
        SUM(da.impressions)         AS total_impressions,
        SUM(da.clicks)              AS total_clicks,
        SUM(da.video_views)         AS total_video_views,
        SUM(da.engagements)         AS total_engagements,
        SUM(da.platform_conversions) AS total_platform_conversions,
        SUM(da.platform_conversion_value) AS total_platform_conversion_value,
        AVG(da.thumbstop_rate)      AS avg_thumbstop_rate,
        AVG(da.frequency)           AS avg_frequency,
        MIN(da.date)                AS first_date,
        MAX(da.date)                AS last_date,
        COUNT(DISTINCT da.date)     AS days_running
    FROM `{project}.{dataset}.platform_daily_spend_ad` da
    GROUP BY da.ad_id, da.ad_group_id, da.campaign_id, da.platform
),

ad_attribution AS (
    SELECT
        ar.ad_id,
        lr.model_name,
        SUM(ar.credit_conversions)  AS attributed_conversions,
        SUM(ar.credit_value)        AS attributed_value,
        SUM(ar.credit_deal_value)   AS attributed_deal_value
    FROM `{project}.{dataset}.attribution_results` ar
    JOIN latest_run lr ON ar.run_id = lr.run_id
    WHERE ar.ad_id IS NOT NULL
    GROUP BY ar.ad_id, lr.model_name
)

SELECT
    -- Ad identity
    a.ad_id,
    a.ad_group_id,
    a.campaign_id,
    a.platform,
    a.ad_name,
    a.ad_type,
    a.creative_format,
    a.creative_size,
    a.headline,
    a.description,
    a.call_to_action,
    a.destination_url,
    a.status                                                 AS ad_status,

    -- Campaign context
    c.campaign_name,
    c.channel,
    c.funnel_stage,
    c.team_id,
    c.brand,

    -- Ad group context
    ag.ad_group_name,
    ag.targeting_type,

    -- Flight
    ds.first_date,
    ds.last_date,
    ds.days_running,

    -- Spend metrics
    COALESCE(ds.total_spend, 0)                              AS total_spend,
    ds.total_impressions,
    ds.total_clicks,
    ds.total_video_views,
    ds.total_engagements,
    SAFE_DIVIDE(ds.total_clicks, ds.total_impressions)       AS ctr,
    SAFE_DIVIDE(ds.total_spend, ds.total_clicks)             AS cpc,
    SAFE_DIVIDE(ds.total_spend * 1000, ds.total_impressions) AS cpm,
    ds.avg_thumbstop_rate,
    ds.avg_frequency,

    -- Platform-reported conversions (reference only)
    ds.total_platform_conversions                            AS platform_conversions,
    SAFE_DIVIDE(ds.total_spend, ds.total_platform_conversions) AS platform_cpa,

    -- MTA attribution
    COALESCE(aa.attributed_conversions, 0)                   AS attributed_conversions,
    aa.attributed_value,
    aa.attributed_deal_value,
    aa.model_name                                            AS attribution_model,
    SAFE_DIVIDE(ds.total_spend, aa.attributed_conversions)   AS attributed_cpa,
    SAFE_DIVIDE(aa.attributed_value, ds.total_spend)         AS attributed_roas

FROM `{project}.{dataset}.platform_ads` a
LEFT JOIN ad_spend          ds  ON a.ad_id = ds.ad_id
LEFT JOIN ad_attribution    aa  ON a.ad_id = aa.ad_id
LEFT JOIN `{project}.{dataset}.platform_campaigns`  c  ON a.campaign_id = c.campaign_id
LEFT JOIN `{project}.{dataset}.platform_ad_groups`  ag ON a.ad_group_id = ag.ad_group_id;


-- =============================================================================
-- VIEW: v_keyword_performance
-- =============================================================================
-- Keyword-level performance for search campaigns. Combines keyword metadata
-- with daily spend aggregates. Attribution credit is estimated at the ad group
-- level (keyword-level attribution requires sessions join — see note below).
--
-- NOTE: True keyword-level attribution (gclid → keyword → credit) requires
-- joining platform_daily_spend_keyword.keyword_text to sessions.gclid via
-- the Google Ads / SA360 keyword performance report. This view provides
-- keyword spend + quality + impression share as a starting point.
-- Use query_account_journey (MCP) or a custom BQ query for gclid-level paths.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_keyword_performance` AS

WITH keyword_spend AS (
    SELECT
        dk.keyword_id,
        dk.ad_group_id,
        dk.campaign_id,
        dk.platform,
        dk.keyword_text,
        dk.match_type,
        dk.row_type,
        SUM(dk.spend)                    AS total_spend,
        SUM(dk.impressions)              AS total_impressions,
        SUM(dk.clicks)                   AS total_clicks,
        SUM(dk.platform_conversions)     AS total_platform_conversions,
        SUM(dk.platform_conversion_value) AS total_platform_conversion_value,
        AVG(dk.impression_share)         AS avg_impression_share,
        AVG(dk.impression_share_lost_budget) AS avg_is_lost_budget,
        AVG(dk.impression_share_lost_rank)   AS avg_is_lost_rank,
        AVG(dk.quality_score)            AS avg_quality_score,
        MIN(dk.date)                     AS first_date,
        MAX(dk.date)                     AS last_date,
        COUNT(DISTINCT dk.date)          AS days_with_spend
    FROM `{project}.{dataset}.platform_daily_spend_keyword` dk
    WHERE dk.row_type = 'keyword'
    GROUP BY dk.keyword_id, dk.ad_group_id, dk.campaign_id, dk.platform,
             dk.keyword_text, dk.match_type, dk.row_type
)

SELECT
    -- Keyword identity
    k.keyword_id,
    k.ad_group_id,
    k.campaign_id,
    k.platform,
    k.keyword_text,
    k.match_type,
    k.negative,
    k.status                                                 AS keyword_status,
    k.bid_amount,
    k.bid_type,
    k.target_cpa                                             AS keyword_target_cpa,

    -- Quality metrics
    k.quality_score,
    k.expected_ctr_score,
    k.ad_relevance_score,
    k.landing_page_experience_score,

    -- Campaign / ad group context
    c.campaign_name,
    c.channel,
    c.funnel_stage,
    c.team_id,
    c.brand,
    c.status                                                 AS campaign_status,
    ag.ad_group_name,
    ag.bid_strategy,

    -- Flight
    ks.first_date,
    ks.last_date,
    ks.days_with_spend,

    -- Spend & volume
    COALESCE(ks.total_spend, 0)                              AS total_spend,
    ks.total_impressions,
    ks.total_clicks,
    SAFE_DIVIDE(ks.total_clicks, ks.total_impressions)       AS ctr,
    SAFE_DIVIDE(ks.total_spend, ks.total_clicks)             AS cpc,
    SAFE_DIVIDE(ks.total_spend * 1000, ks.total_impressions) AS cpm,

    -- Platform-reported performance
    ks.total_platform_conversions                            AS platform_conversions,
    ks.total_platform_conversion_value                       AS platform_conversion_value,
    SAFE_DIVIDE(ks.total_spend, ks.total_platform_conversions) AS platform_cpa,
    SAFE_DIVIDE(ks.total_platform_conversion_value, ks.total_spend) AS platform_roas,

    -- Impression share (competitive health)
    ks.avg_impression_share,
    ks.avg_is_lost_budget,
    ks.avg_is_lost_rank,
    (1 - COALESCE(ks.avg_impression_share, 0)
       - COALESCE(ks.avg_is_lost_budget, 0)
       - COALESCE(ks.avg_is_lost_rank, 0))                   AS is_other_pct

FROM `{project}.{dataset}.platform_keywords` k
LEFT JOIN keyword_spend ks ON k.keyword_id = ks.keyword_id
LEFT JOIN `{project}.{dataset}.platform_campaigns`  c  ON k.campaign_id = c.campaign_id
LEFT JOIN `{project}.{dataset}.platform_ad_groups`  ag ON k.ad_group_id = ag.ad_group_id
WHERE k.negative = FALSE;  -- exclude negative keywords from


-- =============================================================================
-- VIEW: v_daily_performance
-- =============================================================================
-- Day-by-day performance trend for all campaigns. Joins campaign metadata
-- for filtering by platform, team, brand, funnel stage, etc. Useful for
-- time-series analysis, day-of-week patterns, and anomaly detection.
--
-- Note: Attribution credit is not included here — attribution runs are
-- period-level, not daily. Use v_campaign_performance for attribution.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_daily_performance` AS

SELECT
    -- Date
    s.date,
    EXTRACT(DAYOFWEEK FROM s.date)                           AS day_of_week,    -- 1=Sun, 7=Sat
    EXTRACT(WEEK FROM s.date)                                AS week_number,
    FORMAT_DATE('%Y-%m', s.date)                             AS year_month,
    DATE_TRUNC(s.date, WEEK)                                 AS week_start,
    DATE_TRUNC(s.date, MONTH)                                AS month_start,

    -- Dimensions
    s.platform,
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.funnel_stage,
    c.objective,
    c.campaign_type,
    c.status                                                 AS campaign_status,
    c.team_id,
    c.brand,
    c.region,
    c.product_line,

    -- Spend
    s.spend,
    s.currency,
    s.daily_budget,
    SAFE_DIVIDE(s.spend, s.daily_budget) * 100               AS budget_utilization_pct,

    -- Volume
    s.impressions,
    s.clicks,
    s.video_views,
    s.engagements,
    s.reach,

    -- Derived metrics
    s.ctr,
    s.cpc,
    s.cpm,

    -- Platform-reported (for trend analysis, not cross-channel totals)
    s.platform_conversions,
    s.platform_conversion_value,
    s.platform_cpa,
    s.platform_roas,

    -- Pacing snapshot (populated by agents)
    s.pacing_status,
    s.pacing_variance_pct,
    s.projected_monthly_spend,

    -- Audit
    s.data_source,
    s.ingested_at

FROM `{project}.{dataset}.platform_daily_spend` s
LEFT JOIN `{project}.{dataset}.platform_campaigns` c ON s.campaign_id = c.campaign_id;
