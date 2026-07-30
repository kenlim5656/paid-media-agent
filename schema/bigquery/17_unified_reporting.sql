-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — UNIFIED REPORTING LAYER (Task 28)
-- =============================================================================
-- Cross-channel reporting views that blend and normalize performance metrics
-- from all four active paid media channels (Meta, Google Ads, TikTok, Reddit)
-- with downstream web analytics and CRM pipeline data.
--
-- Architecture note on platform storage:
--   Meta, Google Ads, TikTok (and all other non-Reddit platforms) write to the
--   unified platform_daily_spend table (03_platform.sql) with the `platform`
--   column as the discriminator. Reddit Ads writes to a separate
--   reddit_daily_spend table (13_reddit_ads.sql) due to its custom API client.
--   The UNION ALL views in this file merge both sources into a single schema.
--
-- Views in this file:
--   v_unified_daily_spend           — normalized daily spend across all 4 channels
--   v_unified_spatial_performance   — geographic performance consolidation
--   v_reporting_campaign_roi        — 3-tier CPA/ROI: Platform → Traffic → Revenue
--   v_reporting_monthly_pacing      — MTD spend vs. monthly caps + run-rate guidance
--
-- Monetary fields:  NUMERIC throughout (never FLOAT64 for currency amounts)
-- Count fields:     INT64
-- Rate fields:      FLOAT64 (CTR, pacing_pct, etc.)
--
-- Relationships to other schema layers:
--   Reads: platform_daily_spend, platform_campaigns, platform_ad_groups
--          reddit_daily_spend, reddit_spatial_performance
--          sessions, crm_leads_staging, crm_opportunities_staging
--          attribution_results, attribution_runs, attribution_channel_summary
--
--   Also see:
--     06_reporting.sql  — campaign-level, ad-level, keyword-level attribution views
--                         (v_campaign_performance, v_pacing_status, v_roas_comparison,
--                          v_channel_efficiency, v_ad_performance, v_keyword_performance,
--                          v_daily_performance)
--     16_attribution_forensics.sql — v_attribution_correction_weights for data quality
--
-- Usage:
--   bq query --use_legacy_sql=false < 17_unified_reporting.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- VIEW: v_unified_daily_spend
-- =============================================================================
-- Normalized daily spend view at campaign grain across all active channels.
-- Merges platform_daily_spend (Meta, Google Ads, TikTok, and all other
-- platforms in the unified table) with reddit_daily_spend via UNION ALL.
--
-- Reddit data in reddit_daily_spend is at campaign × ad_group grain; this
-- view aggregates it to campaign grain to match the platform_daily_spend rows.
--
-- Output schema (all columns guaranteed non-null after COALESCE):
--   date                DATE       — the reporting day
--   platform            STRING     — "meta" | "google_ads" | "tiktok" | "reddit" | …
--   account_id          STRING     — ad account ID (platform_account_id or Reddit account_id)
--   campaign_id         STRING     — our internal campaign UUID (or Reddit's campaign ID)
--   campaign_name       STRING     — human-readable campaign name
--   ad_group_id         STRING     — NULL (campaign-level view; use platform_daily_spend_ad_group for ad group grain)
--   spend_usd           NUMERIC    — media spend in USD
--   impressions         INT64      — ad impressions served
--   clicks              INT64      — link / headline clicks
--   platform_conversions NUMERIC   — platform's own conversion count (pixel / click attribution)
--
-- IMPORTANT: platform_conversions uses each platform's
-- Do NOT use this column for cross-channel conversion totals — use
-- attribution_results (MTA) or v_reporting_campaign_roi for deduped numbers.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_unified_daily_spend` AS

-- ── Branch 1: All platforms in platform_daily_spend (Meta, Google Ads, TikTok, etc.) ──
-- Excludes 'reddit' to avoid double-counting if Reddit ever writes here.
SELECT
    s.date,
    s.platform,
    COALESCE(c.platform_account_id, 'unknown')      AS account_id,
    s.campaign_id,
    COALESCE(c.campaign_name, s.campaign_id)        AS campaign_name,
    CAST(NULL AS STRING)                            AS ad_group_id,   -- campaign grain
    CAST(COALESCE(s.spend, 0) AS NUMERIC)           AS spend_usd,
    COALESCE(s.impressions, 0)                      AS impressions,
    COALESCE(s.clicks, 0)                           AS clicks,
    CAST(COALESCE(s.platform_conversions, 0) AS NUMERIC) AS platform_conversions
FROM `{project}.{dataset}.platform_daily_spend` s
LEFT JOIN `{project}.{dataset}.platform_campaigns` c
       ON s.campaign_id = c.campaign_id
WHERE s.platform != 'reddit'   -- Reddit comes from reddit_daily_spend below

UNION ALL

-- ── Branch 2: Reddit Ads from reddit_daily_spend ──
-- reddit_daily_spend is at campaign × ad_group grain; aggregate to campaign grain.
-- `conversions` (INT64, post-click) is Reddit's nearest equivalent to platform_conversions.
SELECT
    r.date,
    'reddit'                                        AS platform,
    r.account_id,
    COALESCE(r.campaign_id, 'unknown')              AS campaign_id,
    COALESCE(MAX(r.campaign_name), r.campaign_id)   AS campaign_name,
    CAST(NULL AS STRING)                            AS ad_group_id,
    CAST(SUM(COALESCE(r.spend, 0)) AS NUMERIC)      AS spend_usd,
    SUM(COALESCE(r.impressions, 0))                 AS impressions,
    SUM(COALESCE(r.clicks, 0))                      AS clicks,
    CAST(SUM(COALESCE(r.conversions, 0)) AS NUMERIC) AS platform_conversions
FROM `{project}.{dataset}.reddit_daily_spend` r
GROUP BY
    r.date,
    r.account_id,
    r.campaign_id;


-- =============================================================================
-- VIEW: v_unified_spatial_performance
-- =============================================================================
-- Geographic performance consolidation across all active channels.
--
-- Sources:
--   platform_daily_spend  (Meta / Google Ads / TikTok) — daily grain, geo columns
--     populated when platforms export geo-segmented data into the unified table.
--   reddit_spatial_performance — date-range grain (aggregate window), country + DMA.
--
-- Grain note: platform_daily_spend rows at campaign × date × geo; reddit rows are
-- aggregate over a date range. The `date` column maps to the daily report date
-- for non-Reddit platforms and to date_range_start for Reddit rows. Use
-- `date_range_end` to scope Reddit rows to the correct window when filtering.
--
-- Zero-inflation guard (aligned with Task 24 causal model design):
--   Only rows where spend_usd > 0 OR impressions > 0 are included.
--   This prevents spurious geo entries from padding statistical models.
--
-- Output columns:
--   date            DATE    — daily date (non-Reddit) or range_start (Reddit)
--   date_range_end  DATE    — same as date for daily; range end for Reddit
--   platform        STRING
--   account_id      STRING
--   campaign_id     STRING
--   campaign_name   STRING
--   country_code    STRING  — ISO 3166-1 alpha-2 (e.g. "US", "GB")
--   dma_region      STRING  — DMA code or name (US only; NULL for international)
--   geo_region_name STRING  — state / region name where available
--   spend_usd       NUMERIC
--   impressions     INT64
--   clicks          INT64
--   conversions     NUMERIC — platform-reported (not MTA-deduped)
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_unified_spatial_performance` AS

-- ── Branch 1: platform_daily_spend with geo dimensions ──
-- Filters to rows where geo is populated. Aggregates per (date, platform, campaign, geo)
-- because multiple spend rows may share the same geo breakdown in one day.
SELECT
    s.date,
    s.date                                                  AS date_range_end,   -- daily: same day
    s.platform,
    COALESCE(c.platform_account_id, 'unknown')              AS account_id,
    s.campaign_id,
    COALESCE(c.campaign_name, s.campaign_id)                AS campaign_name,
    COALESCE(s.geo_country_code, 'XX')                      AS country_code,
    s.geo_dma_code                                          AS dma_region,
    s.geo_region_name,
    CAST(SUM(COALESCE(s.spend, 0)) AS NUMERIC)              AS spend_usd,
    SUM(COALESCE(s.impressions, 0))                         AS impressions,
    SUM(COALESCE(s.clicks, 0))                              AS clicks,
    CAST(SUM(COALESCE(s.platform_conversions, 0)) AS NUMERIC) AS conversions
FROM `{project}.{dataset}.platform_daily_spend` s
LEFT JOIN `{project}.{dataset}.platform_campaigns` c
       ON s.campaign_id = c.campaign_id
WHERE s.geo_country_code IS NOT NULL
  AND s.platform != 'reddit'
  AND (s.spend > 0 OR s.impressions > 0)   -- zero-inflation guard
GROUP BY
    s.date,
    s.platform,
    c.platform_account_id,
    s.campaign_id,
    c.campaign_name,
    s.geo_country_code,
    s.geo_dma_code,
    s.geo_region_name

UNION ALL

-- ── Branch 2: Reddit spatial performance ──
-- reddit_spatial_performance has aggregate-window grain (date_range_start → date_range_end).
-- Maps country_code and dma_region directly to the output schema.
SELECT
    r.date_range_start                                      AS date,
    r.date_range_end,
    'reddit'                                                AS platform,
    r.account_id,
    COALESCE(r.campaign_id, 'unknown')                      AS campaign_id,
    COALESCE(r.campaign_name, r.campaign_id)                AS campaign_name,
    COALESCE(r.country_code, 'XX')                          AS country_code,
    r.dma_region,
    CAST(NULL AS STRING)                                    AS geo_region_name,  -- not in Reddit API
    CAST(COALESCE(r.spend, 0) AS NUMERIC)                   AS spend_usd,
    COALESCE(r.impressions, 0)                              AS impressions,
    COALESCE(r.clicks, 0)                                   AS clicks,
    CAST(COALESCE(r.conversions, 0) AS NUMERIC)             AS conversions
FROM `{project}.{dataset}.reddit_spatial_performance` r
WHERE (r.spend > 0 OR r.impressions > 0)   -- zero-inflation guard;


-- =============================================================================
-- VIEW: v_reporting_campaign_roi
-- =============================================================================
-- Campaign-level ROI and CPA view across three business measurement layers.
--
-- The three measurement layers model how different teams within the org
-- interpret performance, and where attribution drift occurs between them:
--
--   Layer 1 — PLATFORM LAYER (ad network native)
--     What the platform's own pixel reports. Inflated due to multi-touch
--     double-counting, view-through inclusion, and attribution window mismatches.
--     Metric: platform_cpa = spend / platform_conversions
--
--   Layer 2 — TRAFFIC LAYER (GA4 / analytics)
--     Paid traffic volume and cost per captured web session or form completion.
--     Campaigns are matched to sessions via the utm_campaign tag (case-insensitive).
--     Only sessions arriving via paid channels (paid utm_medium OR click ID present)
--     are counted. Metric: traffic_cpa_per_session = spend / paid_sessions
--
--   Layer 3 — REVENUE LAYER (CRM pipeline)
--     End-of-funnel outcomes: CRM leads generated, MQL count, closed-won deals, ARR.
--     Path: sessions → crm_leads_staging (via ga4_client_id = ga_client_id)
--           → crm_opportunities_staging (via account_id).
--     Metrics:
--       revenue_cpa_per_lead         = spend / crm_leads_generated
--       revenue_cpa_per_mql          = spend / mql_count
--       revenue_cpa_per_closed_won   = spend / closed_won_count
--       revenue_roas_closed_won      = closed_won_arr / spend
--       open_pipeline_value          = sum of open opportunity ARR linked to this campaign
--
-- CUSTOMIZATION: MQL stage detection uses LOWER(pipeline_stage) LIKE '%mql%'.
--   Adjust the mql_filter CTE to match your CRM's exact pipeline_stage values.
--   Closed-won uses: is_closed = TRUE AND LOWER(pipeline_stage) = 'closed won'.
--
-- Join coverage note:
--   Only leads where crm_leads_staging.ga_client_id IS NOT NULL and matches a
--   session row are included in the revenue layer. Leads sourced via direct CRM
--   entry, manual import, or channels without GA4 tracking will NOT appear here.
--   This view measures digitally-attributed pipeline only.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_reporting_campaign_roi` AS

-- ── CTE 1: Campaign-level spend aggregation ────────────────────────────────
WITH campaign_spend AS (
    SELECT
        u.campaign_id,
        u.platform,
        u.account_id,
        u.campaign_name,
        SUM(u.spend_usd)                                    AS total_spend,
        SUM(u.impressions)                                  AS total_impressions,
        SUM(u.clicks)                                       AS total_clicks,
        CAST(SUM(u.platform_conversions) AS NUMERIC)        AS platform_conversions,
        MIN(u.date)                                         AS period_start,
        MAX(u.date)                                         AS period_end,
        COUNT(DISTINCT u.date)                              AS days_with_spend
    FROM `{project}.{dataset}.v_unified_daily_spend` u
    GROUP BY u.campaign_id, u.platform, u.account_id, u.campaign_name
),

-- ── CTE 2: Campaign metadata (budget, channel, team) ──────────────────────
campaign_meta AS (
    SELECT
        campaign_id,
        channel,
        funnel_stage,
        objective,
        campaign_type,
        status,
        start_date,
        end_date,
        budget_amount,
        budget_type,
        daily_budget,
        team_id,
        brand,
        region,
        product_line,
        utm_campaign,
        utm_source,
        utm_medium,
        has_utm_tracking,
        has_click_id_capture,
        has_capi
    FROM `{project}.{dataset}.platform_campaigns`
),

-- ── CTE 3: Traffic Layer — paid sessions attributed to campaigns ───────────
-- Joins sessions to campaigns via utm_campaign tag (case-insensitive).
-- A session is counted as "paid" if utm_medium is a paid indicator OR a
-- platform click ID (gclid, fbclid, msclkid, ttclid, li_fat_id) is present.
-- De-duplicated at session_id level.
campaign_traffic AS (
    SELECT
        LOWER(TRIM(c.utm_campaign))                             AS utm_campaign_key,
        COUNT(DISTINCT sess.session_id)                         AS paid_sessions,
        COUNT(DISTINCT sess.ga4_client_id)                      AS unique_paid_visitors,
        -- Form completions: sessions where a conversion event exists
        COUNT(DISTINCT ce.conversion_id)                        AS conversion_events_count
    FROM `{project}.{dataset}.sessions` sess
    INNER JOIN `{project}.{dataset}.platform_campaigns` c
            ON LOWER(TRIM(sess.utm_campaign)) = LOWER(TRIM(c.utm_campaign))
    LEFT JOIN `{project}.{dataset}.conversion_events` ce
           ON ce.session_id = sess.session_id
          AND ce.conversion_type IN ('lead', 'lead_form', 'mql', 'sql',
                                     'opportunity_created', 'demo_booked',
                                     'trial_started', 'contact_form',
                                     'content_download', 'webinar_registration')
    WHERE (
        LOWER(sess.utm_medium) IN ('cpc', 'paid', 'paidsocial', 'ppc', 'paid_social',
                                   'paid-social', 'display', 'paiddisplay')
        OR sess.gclid     IS NOT NULL
        OR sess.fbclid    IS NOT NULL
        OR sess.msclkid   IS NOT NULL
        OR sess.ttclid    IS NOT NULL
        OR sess.li_fat_id IS NOT NULL
    )
    GROUP BY LOWER(TRIM(c.utm_campaign))
),

-- ── CTE 4: Revenue Layer — CRM leads and pipeline ─────────────────────────
-- Path: sessions → crm_leads_staging (via ga4_client_id = ga_client_id)
--       → crm_opportunities_staging (via account_id).
-- Only digitally attributed leads (ga_client_id IS NOT NULL in CRM) are counted.
campaign_revenue AS (
    SELECT
        LOWER(TRIM(c.utm_campaign))                             AS utm_campaign_key,
        COUNT(DISTINCT l.account_id)                            AS crm_leads_generated,

        -- MQL count: opportunities at or past MQL stage
        COUNTIF(
            LOWER(o.pipeline_stage) LIKE '%mql%'
            OR LOWER(o.pipeline_stage) LIKE '%marketing qualified%'
        )                                                       AS mql_count,

        -- SQL count: opportunities at SQL or beyond
        COUNTIF(
            LOWER(o.pipeline_stage) LIKE '%sql%'
            OR LOWER(o.pipeline_stage) LIKE '%sales qualified%'
        )                                                       AS sql_count,

        -- Closed-won: revenue-generating outcomes
        COUNTIF(
            o.is_closed = TRUE
            AND LOWER(TRIM(o.pipeline_stage)) = 'closed won'
        )                                                       AS closed_won_count,

        -- Pipeline value: sum of open opportunity ARR linked to this campaign's leads
        CAST(
            SUM(CASE
                WHEN o.is_closed = FALSE
                THEN COALESCE(CAST(o.amount AS FLOAT64), 0.0)
                ELSE 0.0
            END) AS NUMERIC
        )                                                       AS open_pipeline_value,

        -- Closed-won ARR: revenue from won opportunities
        CAST(
            SUM(CASE
                WHEN o.is_closed = TRUE
                     AND LOWER(TRIM(o.pipeline_stage)) = 'closed won'
                THEN COALESCE(CAST(o.amount AS FLOAT64), 0.0)
                ELSE 0.0
            END) AS NUMERIC
        )                                                       AS closed_won_arr
    FROM `{project}.{dataset}.sessions` sess
    INNER JOIN `{project}.{dataset}.platform_campaigns` c
            ON LOWER(TRIM(sess.utm_campaign)) = LOWER(TRIM(c.utm_campaign))
    INNER JOIN `{project}.{dataset}.crm_leads_staging` l
            ON l.ga_client_id = sess.ga4_client_id
    LEFT JOIN `{project}.{dataset}.crm_opportunities_staging` o
           ON LOWER(TRIM(o.account_id)) = LOWER(TRIM(l.account_id))
    WHERE sess.ga4_client_id IS NOT NULL
      AND l.ga_client_id IS NOT NULL
    GROUP BY LOWER(TRIM(c.utm_campaign))
),

-- ── CTE 5: Latest completed MTA run (for attribution comparison) ──────────
latest_mta AS (
    SELECT run_id, model_name, period_start, period_end
    FROM `{project}.{dataset}.attribution_runs`
    WHERE status = 'completed'
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
),

mta_by_campaign AS (
    SELECT
        ar.campaign_id,
        lr.model_name,
        SUM(ar.credit_conversions)                          AS attributed_conversions,
        CAST(SUM(ar.credit_value) AS NUMERIC)               AS attributed_value,
        CAST(SUM(ar.credit_deal_value) AS NUMERIC)          AS attributed_deal_value
    FROM `{project}.{dataset}.attribution_results` ar
    JOIN latest_mta lr ON ar.run_id = lr.run_id
    WHERE ar.campaign_id IS NOT NULL
    GROUP BY ar.campaign_id, lr.model_name
)

-- ── Final SELECT: assemble all three layers ────────────────────────────────
SELECT
    -- ── Campaign identity ──────────────────────────────────────────────────
    sp.campaign_id,
    sp.platform,
    sp.account_id,
    sp.campaign_name,
    meta.channel,
    meta.funnel_stage,
    meta.objective,
    meta.campaign_type,
    meta.status                                             AS campaign_status,
    meta.start_date,
    meta.end_date,
    meta.team_id,
    meta.brand,
    meta.region,
    meta.product_line,
    meta.has_utm_tracking,
    meta.has_click_id_capture,
    meta.has_capi,

    -- ── Reporting period ───────────────────────────────────────────────────
    sp.period_start,
    sp.period_end,
    sp.days_with_spend,

    -- ── Spend & volume ────────────────────────────────────────────────────
    sp.total_spend,
    sp.total_impressions,
    sp.total_clicks,
    SAFE_DIVIDE(CAST(sp.total_clicks AS FLOAT64), NULLIF(sp.total_impressions, 0))
                                                            AS ctr,
    SAFE_DIVIDE(sp.total_spend, NULLIF(sp.total_clicks, 0)) AS cpc,
    SAFE_DIVIDE(sp.total_spend * 1000, NULLIF(sp.total_impressions, 0))
                                                            AS cpm,

    -- ── LAYER 1: Platform native metrics ──────────────────────────────────
    -- Use for in-platform reference and pacing only. Cross-channel totals
    -- will double-count conversions. See IMPORTANT note in view header.
    sp.platform_conversions                                 AS platform_conversions,
    SAFE_DIVIDE(sp.total_spend, NULLIF(sp.platform_conversions, 0))
                                                            AS platform_cpa,
    -- platform_roas is omitted here — use v_roas_comparison for the full 3-ROAS comparison

    -- ── LAYER 2: Traffic layer ────────────────────────────────────────────
    -- Only campaigns with utm_campaign set in platform_campaigns will have
    -- traffic layer data populated. NULL = utm tagging not configured.
    COALESCE(tr.paid_sessions, 0)                           AS paid_sessions,
    COALESCE(tr.unique_paid_visitors, 0)                    AS unique_paid_visitors,
    COALESCE(tr.conversion_events_count, 0)                 AS web_conversion_events,
    SAFE_DIVIDE(sp.total_spend, NULLIF(tr.paid_sessions, 0))
                                                            AS traffic_cpa_per_session,
    SAFE_DIVIDE(sp.total_spend, NULLIF(tr.unique_paid_visitors, 0))
                                                            AS traffic_cpa_per_visitor,
    SAFE_DIVIDE(sp.total_spend, NULLIF(tr.conversion_events_count, 0))
                                                            AS traffic_cpa_per_web_event,

    -- ── LAYER 3: Revenue layer ────────────────────────────────────────────
    -- Digitally attributed CRM outcomes (ga4_client_id → crm_leads_staging).
    -- Leads without GA4 cookie linkage are NOT included (direct entry, outreach, etc.).
    COALESCE(rev.crm_leads_generated, 0)                    AS crm_leads_generated,
    COALESCE(rev.mql_count, 0)                              AS mql_count,
    COALESCE(rev.sql_count, 0)                              AS sql_count,
    COALESCE(rev.closed_won_count, 0)                       AS closed_won_count,
    COALESCE(rev.open_pipeline_value, 0)                    AS open_pipeline_value,
    COALESCE(rev.closed_won_arr, 0)                         AS closed_won_arr,

    SAFE_DIVIDE(sp.total_spend, NULLIF(rev.crm_leads_generated, 0))
                                                            AS revenue_cpa_per_lead,
    SAFE_DIVIDE(sp.total_spend, NULLIF(rev.mql_count, 0))
                                                            AS revenue_cpa_per_mql,
    SAFE_DIVIDE(sp.total_spend, NULLIF(rev.closed_won_count, 0))
                                                            AS revenue_cpa_per_closed_won,
    SAFE_DIVIDE(rev.closed_won_arr, NULLIF(sp.total_spend, 0))
                                                            AS revenue_roas_closed_won,
    SAFE_DIVIDE(rev.open_pipeline_value, NULLIF(sp.total_spend, 0))
                                                            AS pipeline_roas,

    -- Lead funnel conversion rates (%), for funnel health diagnostics
    CAST(
        SAFE_DIVIDE(rev.mql_count, NULLIF(rev.crm_leads_generated, 0)) * 100
        AS FLOAT64
    )                                                       AS lead_to_mql_rate_pct,
    CAST(
        SAFE_DIVIDE(rev.closed_won_count, NULLIF(rev.mql_count, 0)) * 100
        AS FLOAT64
    )                                                       AS mql_to_closed_won_rate_pct,

    -- ── MTA attribution (from latest attribution run) ─────────────────────
    COALESCE(mta.attributed_conversions, 0)                 AS attributed_conversions,
    mta.attributed_value,
    mta.attributed_deal_value,
    mta.model_name                                          AS attribution_model,
    SAFE_DIVIDE(sp.total_spend, NULLIF(mta.attributed_conversions, 0))
                                                            AS attributed_cpa,
    SAFE_DIVIDE(mta.attributed_value, NULLIF(sp.total_spend, 0))
                                                            AS attributed_roas,
    SAFE_DIVIDE(mta.attributed_deal_value, NULLIF(sp.total_spend, 0))
                                                            AS attributed_pipeline_roas,

    -- Platform vs. MTA discrepancy (positive = platform over-reports)
    CAST(
        SAFE_DIVIDE(
            sp.platform_conversions - COALESCE(mta.attributed_conversions, 0),
            NULLIF(mta.attributed_conversions, 0)
        ) * 100 AS FLOAT64
    )                                                       AS platform_vs_mta_delta_pct

FROM campaign_spend sp
LEFT JOIN campaign_meta meta    ON sp.campaign_id = meta.campaign_id
LEFT JOIN campaign_traffic tr   ON LOWER(TRIM(meta.utm_campaign)) = tr.utm_campaign_key
LEFT JOIN campaign_revenue rev  ON LOWER(TRIM(meta.utm_campaign)) = rev.utm_campaign_key
LEFT JOIN mta_by_campaign mta   ON sp.campaign_id = mta.campaign_id;


-- =============================================================================
-- VIEW: v_reporting_monthly_pacing
-- =============================================================================
-- Month-to-date (MTD) budget pacing view for all active campaigns.
-- Distinct from v_pacing_status (06_reporting.sql) which tracks full-flight
-- pacing against lifetime budgets. This view focuses on the CURRENT CALENDAR
-- MONTH, computes the daily run-rate needed to avoid early exhaustion, and
-- surfaces actionable recommended_daily_run_rate_usd.
--
-- Key computed outputs:
--   mtd_spend_usd                — actual spend this calendar month to date
--   daily_run_rate_mtd           — average daily spend over MTD period
--   monthly_cap_usd              — effective monthly budget cap (from campaign config)
--   budget_consumed_pct          — % of monthly cap already spent
--   pacing_velocity_pct          — (actual MTD / expected-at-this-point) × 100
--                                  where expected = monthly_cap × (days_elapsed / days_in_month)
--   recommended_daily_run_rate   — (remaining budget) / (days remaining in month)
--                                  use this to set daily budget caps in the platform
--   projected_month_end_spend    — if current daily rate continues for remaining days
--   mtd_pacing_status            — "over_pacing" | "on_pace" | "under_pacing" | "no_budget_data"
--
-- Pacing thresholds (consistent with v_pacing_status and AGENT.md):
--   over_pacing:  pacing_velocity_pct > 110%
--   on_pace:      90% ≤ pacing_velocity_pct ≤ 110%
--   under_pacing: pacing_velocity_pct < 90%
--
-- monthly_cap_usd derivation by budget_type:
--   monthly:  c.budget_amount (already monthly)
--   daily:    c.daily_budget × days_in_month (projected monthly from daily cap)
--   lifetime: c.budget_amount ÷ flight_months × 1 (pro-rated to current month)
--   NULL:     no budget data available — pacing_status = "no_budget_data"
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_reporting_monthly_pacing` AS

WITH
-- ── Month metadata (derived once, cross-joined) ────────────────────────────
month_metadata AS (
    SELECT
        DATE_TRUNC(CURRENT_DATE(), MONTH)                   AS month_start,
        DATE_ADD(
            DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),
            INTERVAL -1 DAY
        )                                                   AS month_end,
        DATE_DIFF(CURRENT_DATE(),
                  DATE_TRUNC(CURRENT_DATE(), MONTH), DAY) + 1  AS days_elapsed,
        DATE_DIFF(
            DATE_ADD(
                DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),
                INTERVAL -1 DAY
            ),
            CURRENT_DATE(), DAY
        )                                                   AS days_remaining,
        DATE_DIFF(
            DATE_ADD(
                DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),
                INTERVAL -1 DAY
            ),
            DATE_TRUNC(CURRENT_DATE(), MONTH), DAY
        ) + 1                                               AS days_in_month
),

-- ── MTD spend from unified view ────────────────────────────────────────────
mtd_spend AS (
    SELECT
        u.campaign_id,
        u.platform,
        SUM(u.spend_usd)                                    AS mtd_spend_usd,
        SUM(u.platform_conversions)                         AS mtd_platform_conversions,
        SUM(u.impressions)                                  AS mtd_impressions,
        SUM(u.clicks)                                       AS mtd_clicks,
        COUNT(DISTINCT u.date)                              AS days_with_spend_mtd
    FROM `{project}.{dataset}.v_unified_daily_spend` u
    CROSS JOIN month_metadata m
    WHERE u.date BETWEEN m.month_start AND CURRENT_DATE()
    GROUP BY u.campaign_id, u.platform
),

-- ── Prior month spend (for MoM comparison) ────────────────────────────────
prior_month_spend AS (
    SELECT
        u.campaign_id,
        SUM(u.spend_usd)                                    AS prior_month_total_spend
    FROM `{project}.{dataset}.v_unified_daily_spend` u
    CROSS JOIN month_metadata m
    WHERE u.date BETWEEN
        DATE_TRUNC(DATE_SUB(m.month_start, INTERVAL 1 DAY), MONTH)
        AND DATE_SUB(m.month_start, INTERVAL 1 DAY)
    GROUP BY u.campaign_id
),

-- ── Raw pacing computation (intermediate layer to allow alias reuse) ───────
raw_pacing AS (
    SELECT
        c.campaign_id,
        c.platform,
        COALESCE(ms.mtd_spend_usd, 0)                       AS mtd_spend_usd,
        c.campaign_name,
        c.channel,
        c.funnel_stage,
        c.status,
        c.team_id,
        c.brand,
        c.region,
        c.product_line,
        c.start_date,
        c.end_date,
        c.budget_amount,
        c.budget_type,
        CAST(c.daily_budget AS NUMERIC)                     AS daily_budget,

        -- Calendar context
        m.month_start,
        m.month_end,
        m.days_elapsed,
        m.days_remaining,
        m.days_in_month,

        -- MTD metrics
        COALESCE(ms.mtd_impressions, 0)                     AS mtd_impressions,
        COALESCE(ms.mtd_clicks, 0)                          AS mtd_clicks,
        CAST(COALESCE(ms.mtd_platform_conversions, 0) AS NUMERIC) AS mtd_platform_conversions,
        COALESCE(ms.days_with_spend_mtd, 0)                 AS days_with_spend_mtd,

        -- Prior month total for MoM calculation
        COALESCE(pm.prior_month_total_spend, 0)             AS prior_month_total_spend,

        -- Monthly cap: normalized to calendar month regardless of budget_type
        CASE
            WHEN c.budget_type = 'monthly'
                 AND c.budget_amount IS NOT NULL
            THEN c.budget_amount

            WHEN c.budget_type IN ('daily')
                 AND c.daily_budget IS NOT NULL
            THEN CAST(c.daily_budget AS NUMERIC) * m.days_in_month

            WHEN c.budget_type = 'lifetime'
                 AND c.budget_amount IS NOT NULL
                 AND c.start_date IS NOT NULL
                 AND c.end_date IS NOT NULL
                 AND DATE_DIFF(c.end_date, c.start_date, DAY) > 0
            -- Pro-rate lifetime budget to a single month
            THEN c.budget_amount * CAST(m.days_in_month AS NUMERIC)
                 / CAST(DATE_DIFF(c.end_date, c.start_date, DAY) + 1 AS NUMERIC)

            ELSE NULL
        END                                                 AS monthly_cap_usd,

        -- Daily run-rate: average daily spend over MTD period
        SAFE_DIVIDE(
            COALESCE(ms.mtd_spend_usd, 0),
            GREATEST(COALESCE(ms.days_with_spend_mtd, 1), 1)
        )                                                   AS daily_run_rate_mtd,

        -- Pacing velocity (computed here, used in outer SELECT for status)
        -- Expected MTD = monthly_cap × (days_elapsed / days_in_month)
        CASE
            WHEN c.budget_type = 'monthly'
                 AND c.budget_amount IS NOT NULL
            THEN SAFE_DIVIDE(
                    COALESCE(ms.mtd_spend_usd, 0),
                    c.budget_amount
                        * SAFE_DIVIDE(m.days_elapsed, m.days_in_month)
                 ) * 100.0

            WHEN c.budget_type IN ('daily')
                 AND c.daily_budget IS NOT NULL
            THEN SAFE_DIVIDE(
                    COALESCE(ms.mtd_spend_usd, 0),
                    CAST(c.daily_budget AS NUMERIC) * m.days_elapsed
                 ) * 100.0

            WHEN c.budget_type = 'lifetime'
                 AND c.budget_amount IS NOT NULL
                 AND c.start_date IS NOT NULL
                 AND c.end_date IS NOT NULL
                 AND DATE_DIFF(c.end_date, c.start_date, DAY) > 0
            THEN SAFE_DIVIDE(
                    COALESCE(ms.mtd_spend_usd, 0),
                    (c.budget_amount
                     * CAST(m.days_in_month AS NUMERIC)
                     / CAST(DATE_DIFF(c.end_date, c.start_date, DAY) + 1 AS NUMERIC))
                     * SAFE_DIVIDE(m.days_elapsed, m.days_in_month)
                 ) * 100.0

            ELSE NULL
        END                                                 AS pacing_velocity_pct

    FROM `{project}.{dataset}.platform_campaigns` c
    CROSS JOIN month_metadata m
    LEFT JOIN mtd_spend ms          ON c.campaign_id = ms.campaign_id
    LEFT JOIN prior_month_spend pm  ON c.campaign_id = pm.campaign_id
    WHERE c.status IN ('active', 'paused')
      AND c.start_date <= CURRENT_DATE()
)

-- ── Final SELECT: add derived pacing outputs ───────────────────────────────
SELECT
    -- Identity
    rp.campaign_id,
    rp.platform,
    rp.campaign_name,
    rp.channel,
    rp.funnel_stage,
    rp.status                                               AS campaign_status,
    rp.team_id,
    rp.brand,
    rp.region,
    rp.product_line,

    -- Flight
    rp.start_date,
    rp.end_date,
    rp.budget_type,

    -- Calendar
    rp.month_start,
    rp.month_end,
    rp.days_elapsed,
    rp.days_remaining,
    rp.days_in_month,

    -- MTD actuals
    rp.mtd_spend_usd,
    rp.mtd_impressions,
    rp.mtd_clicks,
    rp.mtd_platform_conversions,
    rp.days_with_spend_mtd,

    -- Budget context
    rp.daily_budget,
    rp.budget_amount,
    rp.monthly_cap_usd,

    -- Budget consumed so far (%)
    CAST(
        SAFE_DIVIDE(rp.mtd_spend_usd, NULLIF(rp.monthly_cap_usd, 0)) * 100.0
        AS FLOAT64
    )                                                       AS budget_consumed_pct,

    -- Pacing velocity (actual MTD / expected MTD) × 100
    CAST(rp.pacing_velocity_pct AS FLOAT64)                 AS pacing_velocity_pct,

    -- Pacing status label
    CASE
        WHEN rp.pacing_velocity_pct IS NULL           THEN 'no_budget_data'
        WHEN rp.pacing_velocity_pct > 110.0           THEN 'over_pacing'
        WHEN rp.pacing_velocity_pct < 90.0            THEN 'under_pacing'
        ELSE 'on_pace'
    END                                                     AS mtd_pacing_status,

    -- Pacing variance (spend vs. where we should be in the month)
    rp.mtd_spend_usd - COALESCE(
        rp.monthly_cap_usd
            * SAFE_DIVIDE(rp.days_elapsed, rp.days_in_month),
        0
    )                                                       AS mtd_pacing_variance_usd,

    -- Daily run-rate (average spend per day with spend, over MTD)
    rp.daily_run_rate_mtd,

    -- RECOMMENDED daily run-rate to exhaust monthly cap by end-of-month
    -- = (remaining budget) / (days remaining in month), minimum 0
    CASE
        WHEN rp.monthly_cap_usd IS NOT NULL AND rp.days_remaining > 0
        THEN GREATEST(
                0,
                SAFE_DIVIDE(
                    rp.monthly_cap_usd - rp.mtd_spend_usd,
                    rp.days_remaining
                )
             )
        WHEN rp.monthly_cap_usd IS NOT NULL AND rp.days_remaining = 0
        THEN 0   -- end of month; no remaining spend needed
        ELSE rp.daily_budget   -- fallback: use configured daily budget
    END                                                     AS recommended_daily_run_rate_usd,

    -- Projected end-of-month spend at current MTD run-rate
    rp.mtd_spend_usd
        + rp.daily_run_rate_mtd * CAST(rp.days_remaining AS NUMERIC)
                                                            AS projected_month_end_spend_usd,

    -- Will projected spend land within ±10% of monthly cap?
    CASE
        WHEN rp.monthly_cap_usd IS NOT NULL AND rp.monthly_cap_usd > 0
        THEN ABS(
                SAFE_DIVIDE(
                    (rp.mtd_spend_usd + rp.daily_run_rate_mtd * rp.days_remaining)
                    - rp.monthly_cap_usd,
                    rp.monthly_cap_usd
                )
             ) <= 0.10
        ELSE NULL
    END                                                     AS projected_on_monthly_budget,

    -- Month-over-Month spend comparison
    rp.prior_month_total_spend,
    CAST(
        SAFE_DIVIDE(
            rp.mtd_spend_usd - rp.prior_month_total_spend,
            NULLIF(rp.prior_month_total_spend, 0)
        ) * 100.0 AS FLOAT64
    )                                                       AS spend_mom_delta_pct

FROM raw_pacing rp;
