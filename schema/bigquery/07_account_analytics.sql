-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — ACCOUNT-BASED ANALYTICS LAYER
-- =============================================================================
-- De-anonymizes web analytics by resolving visitor IP addresses to company
-- domains and enriching with firmographic data from IP intelligence providers.
--
-- This layer enables B2B "dark funnel" visibility: instead of "1,247 sessions
-- from unknown visitors," you see "42 sessions from 6 target accounts,
-- 3 of which have open opportunities."
--
-- PRIVACY CONSTRAINTS (non-negotiable):
--   • Raw IP addresses are NEVER stored — resolve to company_domain only
--   • IP resolution happens server-side (sGTM or enrichment job) before ingestion
--   • Only the /24 network prefix (e.g., "203.0.113") is cached for deduplication
--   • The ip_resolution_cache stores ONLY the prefix — never a full address
--   • CRM data is referenced by ID only — no raw PII in this layer
--
-- Resolution flow:
--   Session captured (sGTM / analytics) →
--   IP intelligence call (Clearbit / 6sense / RB2B / ipinfo.io) →
--   company_domain resolved →
--   ip_resolution_cache updated (prefix only) →
--   company_sessions row written (domain + session_id only) →
--   Analyst agent stitches company_domain → identity_entities.company_domain
--
-- Tables in this file:
--   company_profiles          Enriched company firmographics from IP intelligence
--   ip_resolution_cache       /24 prefix → company resolution cache (privacy-safe)
--   company_sessions          De-anonymized sessions (resolved company + session context)
--   company_engagement        Pre-aggregated engagement per company per time period
--   target_account_activity   Daily ABM snapshot: CRM stage + paid media + web intent
--
-- Views in this file:
--   v_target_account_funnel   Ranked target accounts by engagement + pipeline stage
--   v_dark_funnel_coverage    Target accounts with no web presence yet (zero sessions)
-- =============================================================================


-- =============================================================================
-- TABLE: company_profiles
-- =============================================================================
-- Enriched firmographic record for each unique company domain observed.
-- One row per company domain (the canonical identifier for B2B attribution).
-- Populated by IP intelligence enrichment and/or CRM sync.
--
-- Sources: Clearbit Reveal, RB2B, 6sense, ipinfo.io, manual CRM import
-- Refreshed by: Analyst agent enrichment job (periodic) or webhook (real-time)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.company_profiles`
(
    company_id               STRING    NOT NULL,  -- internal UUID (stable)
    company_domain           STRING    NOT NULL,  -- e.g. "acme.com" (primary key for joins)
    company_name             STRING    NOT NULL,

    -- Firmographics
    industry                 STRING,
    -- Standard categories: "Software", "Financial Services", "Healthcare",
    -- "Manufacturing", "Professional Services", "Retail", "Media", "Government", etc.
    industry_group           STRING,              -- broader grouping (e.g., "Technology")
    sub_industry             STRING,              -- more specific (e.g., "SaaS / Cloud")
    naics_code               STRING,
    sic_code                 STRING,

    employee_count           INT64,               -- point estimate
    employee_range           STRING,
    -- "1-10", "11-50", "51-200", "201-500", "501-1000",
    -- "1001-5000", "5001-10000", "10001+"
    annual_revenue           FLOAT64,             -- USD estimate
    annual_revenue_range     STRING,
    -- "<1M", "1M-10M", "10M-50M", "50M-250M", "250M-1B", "1B+"
    company_type             STRING,
    -- "public" | "private" | "nonprofit" | "government" | "subsidiary" | "partnership"
    founded_year             INT64,

    -- Location
    headquarters_country     STRING,              -- ISO 3166-1 alpha-2 (e.g., "US")
    headquarters_country_name STRING,
    headquarters_state       STRING,              -- state/province code
    headquarters_city        STRING,
    headquarters_postal_code STRING,
    headquarters_region      STRING,              -- EMEA, APAC, AMER, etc.

    -- Technology intelligence (from Clearbit / Builtwith / 6sense)
    technologies             ARRAY<STRING>,       -- tech stack (e.g., ["Salesforce", "Slack", "AWS"])
    crm_technology           STRING,              -- "salesforce" | "hubspot" | "dynamics" | "pipedrive" | "other"
    analytics_technology     STRING,              -- "google_analytics" | "adobe" | "mixpanel" | "segment"
    advertising_technology   ARRAY<STRING>,       -- ad tech in use (e.g., ["DV360", "The Trade Desk"])

    -- ABM classification
    is_target_account        BOOL,  -- in the ICP / ABM named account list
    is_icp_fit               BOOL,  -- meets ideal customer profile criteria
    icp_score                FLOAT64,             -- 0–100 ICP fit score (org-defined)
    account_tier             STRING,
    -- "tier_1"   Strategic accounts (highest priority, fully personalized)
    -- "tier_2"   High-priority accounts (semi-personalized outreach)
    -- "tier_3"   Broad target list (programmatic ABM)
    -- "nurture"  Long-term nurture (not yet ICP fit)
    -- "excluded" Competitors, partners, employees — suppress from all ads

    -- CRM linkage
    crm_account_id           STRING,             -- Salesforce / HubSpot account ID
    crm_account_owner        STRING,             -- AE name (non-PII — role, not email)
    crm_pipeline_stage       STRING,
    -- NULL          = not in CRM
    -- "prospect"    = identified, no contact
    -- "engaged"     = SDR / marketing sequence
    -- "mql"         = marketing qualified
    -- "sql"         = sales qualified
    -- "opportunity" = active opportunity
    -- "closed_won"  = customer
    -- "closed_lost" = lost deal
    -- "customer"    = existing customer (for expansion campaigns)
    crm_open_opportunity_count INT64,
    crm_total_deal_value     FLOAT64,            -- sum of open opportunity values (USD)
    crm_last_activity_at     TIMESTAMP,          -- last recorded CRM activity

    -- Identity graph link
    entity_id                STRING,             -- → identity_entities.entity_id (when stitched)

    -- Enrichment metadata
    enrichment_source        STRING,
    -- "clearbit"   Clearbit Reveal / Enrichment API
    -- "rb2b"       RB2B identification
    -- "6sense"     6sense Orbit
    -- "ipinfo"     ipinfo.io Company API
    -- "crm_import" Imported from CRM account list
    -- "manual"     Manually added
    enrichment_confidence    FLOAT64,            -- 0.0–1.0 provider confidence score
    enrichment_method        STRING,
    -- "ip_intelligence"  Resolved from visitor IP
    -- "email_domain"     Inferred from email domain in form submission
    -- "crm_sync"         Matched from CRM account list
    -- "manual"           Manually entered
    last_enriched_at         TIMESTAMP,          -- when firmographics were last refreshed
    enrichment_provider_id   STRING,             -- provider's own company ID (for dedup)

    -- Lifecycle
    first_seen_at            TIMESTAMP,          -- first web session resolved to this company
    last_seen_at             TIMESTAMP,          -- most recent web session
    total_session_count      INT64,              -- lifetime sessions from this company
    created_at               TIMESTAMP NOT NULL,
    updated_at               TIMESTAMP NOT NULL,
    is_active                BOOL
)
PARTITION BY DATE(created_at)
CLUSTER BY is_target_account, account_tier, crm_pipeline_stage
OPTIONS (
    description = "Enriched firmographic record per company domain. Core reference table for account-based analytics. One row per domain."
);


-- =============================================================================
-- TABLE: ip_resolution_cache
-- =============================================================================
-- Privacy-safe cache of /24 network prefix → company resolution results.
-- Reduces redundant API calls to IP intelligence providers.
--
-- PRIVACY NOTE: Only the /24 prefix (first 3 octets) is stored.
--   Full IP: 203.0.113.42   →   Stored as: "203.0.113"
--   This is a network-level identifier, not a user-level identifier.
--   It cannot be used to identify or re-identify an individual.
--
-- Cache TTL: 72 hours by.
-- Resolution confidence degrades over time as corporate networks change.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.ip_resolution_cache`
(
    cache_id                 STRING    NOT NULL,  -- UUID
    ip_prefix                STRING    NOT NULL,  -- /24 prefix only (e.g. "203.0.113") — never full IP
    network_prefix_bits      INT64, -- prefix length (24 = /24 CIDR)

    -- Resolution result
    resolved_company_domain  STRING,             -- NULL if resolution failed (e.g., residential, VPN)
    resolved_company_name    STRING,
    resolution_type          STRING,
    -- "corporate"    Known corporate IP range → company resolved
    -- "residential"  Residential ISP — cannot resolve to company
    -- "vpn"          VPN / proxy detected — skip attribution
    -- "datacenter"   Cloud / CDN infrastructure — exclude from analytics
    -- "bot"          Identified bot / crawler traffic
    -- "unknown"      Cannot classify

    -- Resolution quality
    resolution_confidence    FLOAT64,            -- 0.0–1.0 from the provider
    resolution_provider      STRING,
    -- "clearbit" | "rb2b" | "6sense" | "ipinfo" | "maxmind" | "manual"
    provider_response_ms     INT64,              -- API latency for this lookup

    -- Geographic context (non-PII: country, region only)
    country_code             STRING,             -- ISO 3166-1 alpha-2
    country_name             STRING,
    region                   STRING,             -- state/province

    -- Cache metadata
    resolved_at              TIMESTAMP NOT NULL,
    expires_at               TIMESTAMP NOT NULL, -- resolved_at + TTL (typically 72h)
    hit_count                INT64, -- number of times this cache entry was used
    last_hit_at              TIMESTAMP,

    -- Quality flags
    is_vpn                   BOOL,
    is_datacenter            BOOL,
    is_residential           BOOL,
    is_bot_suspected         BOOL,
    should_exclude_from_analytics BOOL
)
PARTITION BY DATE(resolved_at)
CLUSTER BY ip_prefix, resolved_company_domain
OPTIONS (
    description = "Privacy-safe /24 prefix → company resolution cache. Stores only network prefix, never full IP address."
);


-- =============================================================================
-- TABLE: company_sessions
-- =============================================================================
-- De-anonymized sessions: every session where the visitor was resolved to a
-- company. A subset of the sessions table enriched with company context.
-- Resolution can happen via IP intelligence, email domain match, or CRM sync.
--
-- One row per session where company resolution succeeded.
-- Sessions without company resolution are NOT in this table.
--
-- Joins: company_sessions → sessions (for full session detail)
--        company_sessions → company_profiles (for full firmographics)
--        company_sessions → touchpoint_events (for paid media context)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.company_sessions`
(
    company_session_id       STRING    NOT NULL,  -- UUID
    session_id               STRING    NOT NULL,  -- → sessions.session_id
    company_id               STRING    NOT NULL,  -- → company_profiles.company_id
    company_domain           STRING    NOT NULL,
    company_name             STRING,

    -- Resolution details
    resolution_method        STRING    NOT NULL,
    -- "ip_intelligence"  Resolved via IP → company lookup
    -- "email_domain"     Inferred from hashed email domain captured in form
    -- "crm_match"        Matched to CRM account by CRM identifier
    -- "url_param"        Company passed via URL parameter (e.g., ABM personalization)
    -- "manual"           Manually associated
    resolution_confidence    FLOAT64   NOT NULL,  -- 0.0–1.0
    resolution_provider      STRING,              -- which provider made the call
    ip_prefix                STRING,              -- /24 prefix used for lookup (non-PII)

    -- Session snapshot (denormalized from sessions for query performance)
    session_date             DATE      NOT NULL,
    session_start_at         TIMESTAMP NOT NULL,
    session_duration_seconds INT64,
    page_count               INT64,
    channel_grouping         STRING,
    -- "paid_search" | "paid_social" | "display" | "organic" | "direct" | "email" | "referral"
    entry_url                STRING,
    landing_page             STRING,
    utm_source               STRING,
    utm_medium               STRING,
    utm_campaign             STRING,

    -- Page-level signals (boolean flags for key pages visited in this session)
    visited_pricing          BOOL,
    visited_demo             BOOL,
    visited_contact          BOOL,
    visited_docs             BOOL,
    visited_case_study       BOOL,
    visited_blog             BOOL,
    visited_careers          BOOL,  -- flag for competitor research / job-seekers
    visited_login            BOOL,  -- flag: existing customer

    -- Attribution context (did this session have a paid touchpoint?)
    has_paid_touchpoint      BOOL,
    paid_touchpoint_platform STRING,              -- platform of the paid click/impression
    paid_touchpoint_campaign_id STRING,           -- → platform_campaigns.campaign_id
    paid_click_id_namespace  STRING,              -- namespace of the captured click ID
    paid_click_id_value      STRING,              -- the actual click ID (gclid, fbclid, etc.)

    -- CRM context at time of session (snapshot)
    crm_account_id           STRING,
    crm_pipeline_stage       STRING,             -- pipeline stage at session time
    crm_is_open_opportunity  BOOL,

    -- ABM context
    is_target_account        BOOL,
    account_tier             STRING,

    -- Entity linkage (if session's entity was stitched to the company entity)
    entity_id                STRING,             -- → identity_entities.entity_id

    -- Audit
    resolved_at              TIMESTAMP NOT NULL,
    enriched_by              STRING               -- "analyst_agent" | "sgtm_tag" | "import"
)
PARTITION BY session_date
CLUSTER BY company_domain, channel_grouping, is_target_account
OPTIONS (
    description = "De-anonymized sessions: all sessions where visitor was resolved to a company domain. Core table for account-based web analytics."
);


-- =============================================================================
-- TABLE: company_engagement
-- =============================================================================
-- Pre-aggregated engagement metrics per company per time period.
-- Written by the Analyst agent on a scheduled basis (daily, weekly, monthly).
-- Enables fast "show me all companies that engaged with pricing in the last 30 days"
-- queries without scanning the raw company_sessions table.
--
-- The Analyst agent writes one row per (company, period_type, period_start).
-- Use the 'daily' period_type for current-day reporting.
-- Use 'monthly' for trend analysis and account scoring.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.company_engagement`
(
    engagement_id            STRING    NOT NULL,  -- UUID
    company_id               STRING    NOT NULL,  -- → company_profiles.company_id
    company_domain           STRING    NOT NULL,
    company_name             STRING,

    -- Time period
    period_type              STRING    NOT NULL,  -- "daily" | "weekly" | "monthly" | "rolling_30d"
    period_start             DATE      NOT NULL,
    period_end               DATE      NOT NULL,

    -- ABM classification (snapshot at period end)
    is_target_account        BOOL,
    account_tier             STRING,
    crm_pipeline_stage       STRING,             -- CRM stage at end of period
    crm_is_open_opportunity  BOOL,

    -- Session volume
    total_sessions           INT64,
    unique_session_days      INT64,              -- number of distinct days with sessions
    total_page_views         INT64,

    -- Visitor identity depth
    resolved_entities        INT64,              -- distinct entity_ids resolved to this company
    anonymous_sessions       INT64,              -- sessions with no entity match

    -- Engagement depth signals
    avg_session_duration_seconds FLOAT64,
    avg_pages_per_session    FLOAT64,
    max_pages_in_session     INT64,

    -- Key page visits (count of sessions that included each page type)
    pricing_page_sessions    INT64,
    demo_page_sessions       INT64,
    contact_page_sessions    INT64,
    docs_sessions            INT64,
    case_study_sessions      INT64,
    blog_sessions            INT64,

    -- Channel breakdown (how did they arrive?)
    paid_search_sessions     INT64,
    paid_social_sessions     INT64,
    display_sessions         INT64,
    organic_sessions         INT64,
    direct_sessions          INT64,
    email_sessions           INT64,
    referral_sessions        INT64,

    -- Paid media exposure (sessions with a paid touchpoint)
    paid_sessions            INT64,             -- sessions driven by paid media
    paid_platforms_seen      ARRAY<STRING>,     -- platforms that drove paid sessions
    paid_campaigns_seen      ARRAY<STRING>,     -- campaign_ids that drove paid sessions

    -- Intent signals
    intent_score             FLOAT64,           -- 0–100 composite intent score (org-defined)
    -- Intent score components (store separately for transparency):
    recency_score            FLOAT64,           -- higher when recently active (last 7d = max)
    frequency_score          FLOAT64,           -- higher with more sessions in period
    depth_score              FLOAT64,           -- higher with pricing/demo/contact visits
    content_score            FLOAT64,           -- higher with case study / blog engagement

    -- Engagement trend (period-over-period)
    sessions_prev_period     INT64,             -- same period_type, previous window
    session_growth_pct       FLOAT64,           -- (total_sessions - sessions_prev) / sessions_prev * 100

    -- Suppression status
    is_suppressed_tofu       BOOL,              -- suppressed from top-of-funnel paid ads
    suppression_reason       STRING,            -- "open_opportunity" | "customer" | "competitor" | "manual"

    -- Audit
    generated_at             TIMESTAMP NOT NULL,
    generated_by             STRING               -- "analyst_agent" | "scheduled_job"
)
PARTITION BY period_start
CLUSTER BY company_domain, period_type, is_target_account
OPTIONS (
    description = "Pre-aggregated company engagement per time period. Written by Analyst agent. Core table for account-based reporting and intent scoring."
);


-- =============================================================================
-- TABLE: target_account_activity
-- =============================================================================
-- Daily ABM snapshot for every target account. Combines:
--   - Web engagement (from company_sessions)
--   - CRM pipeline status (from CRM staging tables)
--   - Paid media exposure (from attribution_results)
--   - Audience suppression status (from operator_action_log)
--
-- Written by the Analyst agent as part of the daily attribution run.
-- One row per target account per day — enables trend analysis over time.
-- Non-target accounts are NOT tracked here (see company_engagement for all).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.target_account_activity`
(
    activity_id              STRING    NOT NULL,  -- UUID
    date                     DATE      NOT NULL,
    company_id               STRING    NOT NULL,  -- → company_profiles.company_id
    company_domain           STRING    NOT NULL,
    company_name             STRING    NOT NULL,

    -- Account classification
    account_tier             STRING    NOT NULL,  -- "tier_1" | "tier_2" | "tier_3"
    icp_score                FLOAT64,             -- ICP fit score at this date
    crm_account_id           STRING,
    crm_account_owner        STRING,

    -- CRM pipeline status (snapshot at this date)
    crm_pipeline_stage       STRING,
    crm_open_opportunity_count INT64,
    crm_total_deal_value     FLOAT64,
    crm_last_activity_days_ago INT64,            -- days since last CRM activity
    crm_stage_changed_today  BOOL,  -- did stage change today?
    crm_stage_previous       STRING,             -- previous stage (if changed)
    crm_stage_new            STRING,             -- new stage (if changed)

    -- Web engagement today
    web_sessions_today       INT64,
    web_page_views_today     INT64,
    web_unique_visitors_today INT64,
    visited_pricing_today    BOOL,
    visited_demo_today       BOOL,
    visited_contact_today    BOOL,
    visited_docs_today       BOOL,
    channels_today           ARRAY<STRING>,       -- channel_groupings seen today

    -- Web engagement trailing windows
    web_sessions_7d          INT64,
    web_sessions_30d         INT64,
    web_sessions_90d         INT64,
    pricing_visits_30d       INT64,
    demo_visits_30d          INT64,

    -- Paid media exposure
    paid_touchpoints_today   INT64,
    paid_touchpoints_7d      INT64,
    paid_touchpoints_30d     INT64,
    paid_platforms_30d       ARRAY<STRING>,       -- platforms that touched this account in 30d
    paid_campaigns_active    ARRAY<STRING>,       -- campaign_ids currently running to this account
    last_paid_touchpoint_at  TIMESTAMP,
    last_paid_touchpoint_platform STRING,
    last_paid_touchpoint_channel STRING,

    -- Attribution credit (from latest run covering this period)
    attributed_conversions_30d  FLOAT64,
    attributed_deal_value_30d   FLOAT64,
    last_attribution_run_id     STRING,

    -- Audience suppression
    is_suppressed_tofu       BOOL,  -- suppressed from top-of-funnel
    suppression_platforms    ARRAY<STRING>,      -- which platforms the suppression is active on
    suppression_reason       STRING,             -- "open_opportunity" | "customer" | "competitor"
    suppression_applied_at   TIMESTAMP,

    -- Intent scoring (composite)
    intent_score_today       FLOAT64,            -- 0–100 (recency + frequency + depth + content)
    intent_score_7d_avg      FLOAT64,
    intent_score_30d_avg     FLOAT64,
    intent_spiking           BOOL,
    -- intent_spiking = intent_score_today > (intent_score_30d_avg * 1.5)
    -- Spike threshold: 50% above 30-day average

    -- Signal gaps (what's missing from our data?)
    has_web_presence         BOOL,               -- have we ever seen web sessions?
    has_crm_record           BOOL,               -- does a CRM account exist?
    has_paid_exposure        BOOL,               -- has paid media touched this account?
    has_identified_visitors  BOOL,               -- have any visitors been entity-resolved?
    coverage_completeness_score FLOAT64,
    -- 0.25 per flag: web + crm + paid + identified = 1.0 (full coverage)

    -- Audit
    generated_at             TIMESTAMP NOT NULL,
    generated_by             STRING               -- "analyst_agent" | "scheduled_job"
)
PARTITION BY date
CLUSTER BY account_tier, crm_pipeline_stage, is_suppressed_tofu
OPTIONS (
    description = "Daily ABM snapshot per target account. Combines web engagement, CRM pipeline, paid media exposure, and suppression status."
);


-- =============================================================================
-- VIEW: v_target_account_funnel
-- =============================================================================
-- Ranked view of target accounts by their funnel position and engagement.
-- Shows the most recent snapshot for each target account alongside their
-- 30-day engagement trends and paid media coverage.
--
-- Use this view to answer:
--   "Which target accounts are showing intent signals but haven't converted?"
--   "Which accounts in late pipeline stage need suppression from top-of-funnel?"
--   "Which accounts have we paid to reach but who haven't visited our site?"
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_target_account_funnel` AS

WITH latest_activity AS (
    SELECT *
    FROM `{project}.{dataset}.target_account_activity`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY date DESC) = 1
),

engagement_30d AS (
    SELECT
        company_id,
        total_sessions            AS sessions_30d,
        pricing_page_sessions     AS pricing_visits_30d,
        demo_page_sessions        AS demo_visits_30d,
        contact_page_sessions     AS contact_visits_30d,
        intent_score              AS intent_score_30d,
        paid_sessions             AS paid_sessions_30d,
        paid_platforms_seen       AS paid_platforms_30d
    FROM `{project}.{dataset}.company_engagement`
    WHERE period_type = 'rolling_30d'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY period_start DESC) = 1
)

SELECT
    -- Account identity
    ta.company_id,
    ta.company_domain,
    ta.company_name,
    ta.account_tier,
    ta.icp_score,
    ta.crm_account_id,
    ta.crm_account_owner,
    ta.date                                         AS snapshot_date,

    -- CRM pipeline
    ta.crm_pipeline_stage,
    ta.crm_open_opportunity_count,
    ta.crm_total_deal_value,
    ta.crm_last_activity_days_ago,
    ta.crm_stage_changed_today,
    ta.crm_stage_previous,
    ta.crm_stage_new,

    -- Web engagement (latest day)
    ta.web_sessions_today,
    ta.web_sessions_7d,
    ta.web_sessions_30d,
    ta.visited_pricing_today,
    ta.visited_demo_today,
    ta.visited_contact_today,
    ta.intent_score_today,
    ta.intent_spiking,

    -- 30-day engagement rollup
    e.sessions_30d,
    e.pricing_visits_30d,
    e.demo_visits_30d,
    e.contact_visits_30d,
    e.intent_score_30d,

    -- Paid media
    ta.paid_touchpoints_30d,
    ta.paid_platforms_30d,
    ta.paid_campaigns_active,
    ta.last_paid_touchpoint_at,
    ta.last_paid_touchpoint_channel,
    e.paid_sessions_30d,
    e.paid_platforms_30d                            AS paid_platforms_30d_detail,

    -- Attribution
    ta.attributed_conversions_30d,
    ta.attributed_deal_value_30d,

    -- Suppression
    ta.is_suppressed_tofu,
    ta.suppression_reason,
    ta.suppression_platforms,

    -- Coverage gaps
    ta.has_web_presence,
    ta.has_crm_record,
    ta.has_paid_exposure,
    ta.has_identified_visitors,
    ta.coverage_completeness_score,

    -- Funnel priority score
    -- Weights: high-intent + late-stage + big deal + recent engagement
    (
        -- Pipeline stage score (0-30 pts)
        CASE ta.crm_pipeline_stage
            WHEN 'opportunity' THEN 30
            WHEN 'sql'         THEN 25
            WHEN 'mql'         THEN 20
            WHEN 'engaged'     THEN 15
            WHEN 'prospect'    THEN 10
            WHEN 'customer'    THEN  5  -- lower priority (already won)
            ELSE 0
        END
        -- Intent score contribution (0-25 pts)
        + COALESCE(ta.intent_score_today * 0.25, 0)
        -- Recency of web visit (0-20 pts)
        + CASE
            WHEN ta.web_sessions_today > 0 THEN 20
            WHEN ta.web_sessions_7d > 0    THEN 15
            WHEN ta.web_sessions_30d > 0   THEN 10
            ELSE 0
          END
        -- Key page visits (0-15 pts)
        + CASE WHEN ta.visited_pricing_today THEN 10 ELSE 0 END
        + CASE WHEN ta.visited_demo_today    THEN  8 ELSE 0 END
        + CASE WHEN ta.visited_contact_today THEN  7 ELSE 0 END
        -- Paid exposure (0-10 pts)
        + CASE WHEN ta.has_paid_exposure THEN 10 ELSE 0 END
    )                                               AS funnel_priority_score,

    -- Account tier rank (for sorting)
    CASE ta.account_tier
        WHEN 'tier_1'  THEN 1
        WHEN 'tier_2'  THEN 2
        WHEN 'tier_3'  THEN 3
        WHEN 'nurture' THEN 4
        ELSE 5
    END                                             AS account_tier_rank

FROM latest_activity ta
LEFT JOIN engagement_30d e ON ta.company_id = e.company_id

-- Only target accounts in v_target_account_funnel
-- All companies (including non-target) are in company_engagement / company_sessions
WHERE ta.account_tier IN ('tier_1', 'tier_2', 'tier_3');


-- =============================================================================
-- VIEW: v_dark_funnel_coverage
-- =============================================================================
-- Target accounts with NO web presence detected — the "dark funnel" gap.
-- These are accounts that:
--   1. Are on the target account / ICP list (company_profiles.is_target_account = true)
--   2. Have never been resolved in a web session (no company_sessions rows)
--
-- Useful for: identifying coverage gaps in IP intelligence, understanding
-- which accounts need account-based advertising to create first touch,
-- and measuring the effectiveness of the IP resolution enrichment pipeline.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_dark_funnel_coverage` AS

WITH accounts_with_sessions AS (
    SELECT DISTINCT company_id
    FROM `{project}.{dataset}.company_sessions`
    WHERE session_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

engagement_summary AS (
    SELECT
        company_id,
        SUM(total_sessions)   AS total_sessions_all_time,
        MAX(period_start)     AS last_engagement_period
    FROM `{project}.{dataset}.company_engagement`
    GROUP BY company_id
)

SELECT
    cp.company_id,
    cp.company_domain,
    cp.company_name,
    cp.account_tier,
    cp.icp_score,
    cp.crm_account_id,
    cp.crm_account_owner,
    cp.crm_pipeline_stage,
    cp.crm_total_deal_value,
    cp.industry,
    cp.employee_range,
    cp.headquarters_country,

    -- Coverage status
    CASE
        WHEN aws.company_id IS NOT NULL THEN 'visible'        -- seen in last 90 days
        WHEN e.total_sessions_all_time > 0 THEN 'lapsed'      -- seen before, not recently
        ELSE 'dark'                                            -- never resolved
    END                                                       AS web_presence_status,

    e.total_sessions_all_time,
    e.last_engagement_period,

    -- What do we know?
    CASE WHEN cp.crm_account_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_crm,
    CASE WHEN cp.crm_pipeline_stage IS NOT NULL THEN TRUE ELSE FALSE END AS has_pipeline,

    -- Account priority context
    CASE cp.account_tier
        WHEN 'tier_1'  THEN 1
        WHEN 'tier_2'  THEN 2
        WHEN 'tier_3'  THEN 3
        ELSE 4
    END                                                       AS tier_rank

FROM `{project}.{dataset}.company_profiles` cp
LEFT JOIN accounts_with_sessions aws ON cp.company_id = aws.company_id
LEFT JOIN engagement_summary e       ON cp.company_id = e.company_id
WHERE cp.is_target_account = TRUE
  AND cp.is_active = TRUE
ORDER BY tier_rank ASC, cp.icp_score DESC;
