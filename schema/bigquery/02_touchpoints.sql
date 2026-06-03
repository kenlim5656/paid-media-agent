-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — TOUCHPOINT & CONVERSION LAYER
-- =============================================================================
-- Touchpoints are every marketing interaction an entity has before converting.
-- Conversions are the outcomes we're attributing credit toward.
-- Both reference the identity layer via entity_id and session_id.
--
-- Tables in this file:
--   sessions               Browser/app sessions linking signals to behavior
--   touchpoint_events      Ad exposures, clicks, and site visits
--   conversion_events      Goals, leads, purchases, pipeline milestones
-- =============================================================================


-- -----------------------------------------------------------------------------
-- sessions
-- A session is the bridge between identity signals and behavior.
-- One session can contain multiple touchpoints and conversions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sessions`
(
    session_id          STRING    NOT NULL,  -- platform session ID (e.g. GA4 session_id)
    session_source      STRING    NOT NULL,  -- "ga4" | "adobe" | "segment" | "custom"

    -- Identity linkage (resolved asynchronously after stitching)
    entity_id           STRING,              -- → identity_entities.entity_id (null until stitched)
    entity_type         STRING,              -- "person" | "account"

    -- Session context
    session_start_at    TIMESTAMP NOT NULL,
    session_end_at      TIMESTAMP,
    page_count          INT64,
    channel_grouping    STRING,              -- "paid_search" | "paid_social" | "display" | "organic" | "direct" | "email" | "referral"

    -- Entry point signals (the most important for attribution)
    entry_url           STRING,
    entry_referrer      STRING,
    landing_page        STRING,

    -- Platform click IDs present at session start (denormalized for query performance)
    -- These are the direct attribution signals — kept here for fast joins
    gclid               STRING,
    dclid               STRING,
    fbclid              STRING,
    li_fat_id           STRING,
    ttclid              STRING,
    msclkid             STRING,
    sccid               STRING,
    epik                STRING,
    twclid              STRING,
    amzn_clid           STRING,

    -- Analytics identifiers
    ga4_client_id       STRING,
    ga4_session_id      STRING,
    ecid                STRING,              -- Adobe ECID
    segment_anonymous_id STRING,

    -- UTM parameters (captured as-is from the URL)
    utm_source          STRING,
    utm_medium          STRING,
    utm_campaign        STRING,
    utm_content         STRING,
    utm_term            STRING,
    utm_id              STRING,              -- numeric campaign ID, sometimes used for GMP linking

    -- Custom / org-defined parameters (captured from URL or data layer)
    custom_params       JSON,               -- any additional URL params or data layer values

    -- Device context
    device_type         STRING,              -- "desktop" | "mobile" | "tablet"
    browser             STRING,
    os                  STRING,
    country             STRING,
    region              STRING,

    -- Audit
    ingested_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(session_start_at)
CLUSTER BY entity_id, ga4_client_id, session_source
OPTIONS (
    description = "Sessions bridging identity signals to behavioral events. Contains denormalized click IDs for fast attribution joins."
);


-- -----------------------------------------------------------------------------
-- touchpoint_events
-- Every paid media interaction: ad impressions (where available), clicks,
-- and site engagement events that are part of the attribution path.
-- Platform-agnostic — GMP, Meta, LinkedIn, TikTok all write here.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.touchpoint_events`
(
    touchpoint_id       STRING    NOT NULL,  -- UUID
    entity_id           STRING,              -- → identity_entities.entity_id
    session_id          STRING,              -- → sessions.session_id (null for impression-only)

    -- When & what
    touchpoint_at       TIMESTAMP NOT NULL,
    touchpoint_type     STRING    NOT NULL,
    -- "click"          — ad click that initiated a session
    -- "impression"     — ad served (view-through; requires impression data feed)
    -- "video_view"     — video view event (25%, 50%, 75%, 100% thresholds)
    -- "engagement"     — meaningful on-site event (not a conversion)
    -- "email_open"     — marketing email open (from MAP integration)
    -- "email_click"    — marketing email click

    -- Platform & campaign context
    platform            STRING    NOT NULL,
    -- "google_ads" | "dv360" | "sa360" | "cm360" | "meta" | "linkedin"
    -- "tiktok" | "snapchat" | "pinterest" | "twitter_x" | "microsoft_ads"
    -- "amazon_ads" | "email" | "custom"

    platform_campaign_id   STRING,          -- platform's native campaign ID
    platform_ad_group_id   STRING,          -- ad group / line item / ad set ID
    platform_ad_id         STRING,          -- ad / creative ID
    platform_placement_id  STRING,          -- placement / publisher / site ID

    -- Our internal campaign reference (resolved via platform_campaigns table)
    campaign_id         STRING,             -- → platform_campaigns.campaign_id
    ad_group_id         STRING,             -- → platform_ad_groups.ad_group_id
    ad_id               STRING,             -- → platform_ads.ad_id

    -- Channel classification (standardized across platforms)
    channel             STRING,
    -- "paid_search_brand" | "paid_search_nonbrand" | "paid_search_competitor"
    -- "display_prospecting" | "display_retargeting"
    -- "paid_social_prospecting" | "paid_social_retargeting"
    -- "video_awareness" | "video_retargeting"
    -- "ctv" | "audio" | "dooh"
    -- "email_nurture" | "email_promotional"
    sub_channel         STRING,             -- more granular: "pmax" | "shopping" | "rsa" | "dco" etc.

    -- Click IDs on this specific touchpoint (for cross-referencing)
    click_id_namespace  STRING,             -- which click ID type was captured
    click_id_value      STRING,             -- the value

    -- Creative context
    creative_format     STRING,             -- "image" | "video" | "carousel" | "responsive" | "native" | "html5"
    creative_size       STRING,             -- "300x250" | "9:16" | "16:9" etc.

    -- Funnel stage at time of touchpoint
    funnel_stage        STRING,
    -- "awareness" | "consideration" | "intent" | "conversion" | "retention"

    -- Attribution path position (populated after path assembly)
    path_position       INT64,              -- 1 = first touch, N = last touch
    path_total_touches  INT64,              -- total touches in this path

    -- Audit
    ingested_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING               -- "capi" | "cm360_data_transfer" | "ga4_export" | "platform_api"
)
PARTITION BY DATE(touchpoint_at)
CLUSTER BY entity_id, platform, channel
OPTIONS (
    description = "All paid media touchpoints across all platforms. Platform-agnostic. One row per interaction."
);


-- -----------------------------------------------------------------------------
-- conversion_events
-- The outcomes we're attributing toward. Platform-agnostic.
-- One row per conversion event per entity.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.conversion_events`
(
    conversion_id       STRING    NOT NULL,  -- UUID (use order_id/transaction_id if available)
    entity_id           STRING    NOT NULL,  -- → identity_entities.entity_id
    session_id          STRING,              -- the session in which the conversion occurred

    -- When & what
    converted_at        TIMESTAMP NOT NULL,
    conversion_type     STRING    NOT NULL,
    -- B2C:  "purchase" | "registration" | "subscription" | "trial" | "lead_form"
    -- B2B:  "lead" | "mql" | "sql" | "opportunity_created" | "opportunity_won"
    --       "demo_booked" | "trial_started" | "contract_signed"
    -- Both: "content_download" | "webinar_registration" | "contact_form"

    conversion_name     STRING,              -- human-readable label (e.g. "Enterprise Demo Request")

    -- Value
    conversion_value    FLOAT64,             -- monetary value at time of conversion
    currency            STRING    DEFAULT 'USD',
    is_primary          BOOL      DEFAULT TRUE,  -- primary vs. micro/secondary conversion

    -- Deduplication
    transaction_id      STRING,              -- order ID, opportunity ID, etc. for dedup
    is_deduplicated     BOOL      DEFAULT FALSE,
    dedup_source        STRING,              -- which system is the dedup authority

    -- B2B pipeline context
    crm_lead_id         STRING,
    crm_opportunity_id  STRING,
    crm_account_id      STRING,
    pipeline_stage      STRING,
    deal_value          FLOAT64,             -- full deal/ARR value (may differ from conversion_value)

    -- Platform attribution at time of conversion (platform's own view)
    -- Kept for cross-referencing / discrepancy analysis
    platform_attributed_to   STRING,        -- which platform claimed this conversion
    platform_conversion_name STRING,

    -- Audit
    ingested_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING               -- "crm" | "ga4" | "capi" | "platform_api" | "manual"
)
PARTITION BY DATE(converted_at)
CLUSTER BY entity_id, conversion_type, crm_account_id
OPTIONS (
    description = "All conversion events across all platforms and CRM systems. Deduplication-ready. Platform-agnostic."
);
