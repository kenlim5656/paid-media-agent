-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — ATTRIBUTION LAYER
-- =============================================================================
-- The attribution layer is written by the Analyst agent and read by the
-- Operator agent and the paid-media-mcp (surfacing results to skills).
--
-- Tables in this file:
--   attribution_paths          Assembled multi-touch paths per entity+conversion
--   attribution_runs           Log of every model execution
--   attribution_results        Weighted credit output per touchpoint
--   attribution_channel_summary  Rolled-up channel performance (for reporting)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- attribution_paths
-- The assembled sequence of touchpoints leading to each conversion.
-- Built by the Analyst agent from sessions + touchpoint_events + conversion_events.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.attribution_paths`
(
    path_id             STRING    NOT NULL,  -- UUID
    entity_id           STRING    NOT NULL,  -- → identity_entities.entity_id
    conversion_id       STRING    NOT NULL,  -- → conversion_events.conversion_id
    conversion_type     STRING    NOT NULL,
    converted_at        TIMESTAMP NOT NULL,

    -- Path shape
    total_touches       INT64     NOT NULL,
    path_duration_days  INT64,               -- days from first touch to conversion
    is_single_touch     BOOL,                -- true if only one touchpoint
    is_cross_platform   BOOL,                -- true if touches span >1 platform
    is_cross_device     BOOL,                -- true if touches span >1 device

    -- Path summary (denormalized for fast reporting)
    first_touch_platform  STRING,
    first_touch_channel   STRING,
    first_touch_at        TIMESTAMP,
    last_touch_platform   STRING,
    last_touch_channel    STRING,
    last_touch_at         TIMESTAMP,
    platforms_in_path     ARRAY<STRING>,     -- all unique platforms touched
    channels_in_path      ARRAY<STRING>,     -- all unique channels touched

    -- Ordered touchpoints (array of structs for compact storage)
    touchpoints         ARRAY<STRUCT<
        touchpoint_id   STRING,
        position        INT64,
        platform        STRING,
        channel         STRING,
        touchpoint_type STRING,
        touched_at      TIMESTAMP,
        click_id_namespace STRING,
        click_id_value  STRING
    >>,

    -- Lookback window applied
    lookback_days       INT64,               -- days before conversion included in path

    -- Audit
    assembled_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    assembled_by        STRING               -- "analyst_agent" | "import"
)
PARTITION BY DATE(converted_at)
CLUSTER BY entity_id, conversion_type
OPTIONS (
    description = "Assembled multi-touch paths per entity per conversion. Foundation for all attribution models."
);


-- -----------------------------------------------------------------------------
-- attribution_runs
-- Metadata about each model execution. Every run produces a batch of rows
-- in attribution_results tagged with this run_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.attribution_runs`
(
    run_id              STRING    NOT NULL,  -- UUID
    run_type            STRING    NOT NULL,
    -- "scheduled"      — triggered by cron
    -- "manual"         — triggered by a practitioner via skill
    -- "triggered"      — triggered by an agent condition (e.g. data quality restored)

    model_name          STRING    NOT NULL,
    -- "full_path"          First 30% + Last 30% + Middle 40% equally split
    -- "first_touch"        100% first touch
    -- "last_touch"         100% last touch
    -- "linear"             Equal credit to all touches
    -- "time_decay"         More credit to recent touches (half-life configurable)
    -- "position_based"     Configurable position weights
    -- "markov_chain"       Data-driven via Markov transition probabilities
    -- "shapley_value"      Game-theoretic fair attribution
    -- "custom"             Org-defined model

    model_version       STRING,             -- semver of the model implementation
    model_config        JSON,               -- model parameters (weights, lookback, etc.)
    -- Examples:
    -- full_path:      {"first_touch_pct": 0.30, "last_touch_pct": 0.30}
    -- time_decay:     {"half_life_days": 7}
    -- position_based: {"positions": [{"position": 1, "weight": 0.40}, {"position": -1, "weight": 0.40}], "middle_weight": 0.20}

    -- Scope
    period_start        DATE      NOT NULL,
    period_end          DATE      NOT NULL,
    conversion_types    ARRAY<STRING>,       -- which conversion types were modeled
    entity_types        ARRAY<STRING>,       -- "person" | "account"

    -- Results summary
    paths_modeled       INT64,
    conversions_attributed INT64,
    total_credit        FLOAT64,
    total_conversion_value FLOAT64,

    -- Data quality snapshot at run time
    data_quality_score  FLOAT64,            -- 0.0–1.0 (from Watchdog)
    identity_match_rate FLOAT64,            -- % of sessions that resolved to an entity
    avg_path_length     FLOAT64,

    -- Run metadata
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,
    duration_seconds    INT64,
    status              STRING,             -- "running" | "completed" | "failed" | "superseded"
    error_message       STRING,
    triggered_by        STRING              -- "analyst_agent" | "practitioner" | "schedule"
)
PARTITION BY DATE(started_at)
CLUSTER BY model_name, status
OPTIONS (
    description = "Log of every attribution model run. Each run produces a batch of attribution_results rows."
);


-- -----------------------------------------------------------------------------
-- attribution_results
-- The weighted credit allocation per touchpoint per run.
-- This is the primary output table consumed by the Operator agent and skills.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.attribution_results`
(
    result_id           STRING    NOT NULL,  -- UUID
    run_id              STRING    NOT NULL,  -- → attribution_runs.run_id
    path_id             STRING    NOT NULL,  -- → attribution_paths.path_id
    touchpoint_id       STRING    NOT NULL,  -- → touchpoint_events.touchpoint_id
    conversion_id       STRING    NOT NULL,  -- → conversion_events.conversion_id
    entity_id           STRING    NOT NULL,

    -- Date context
    conversion_date     DATE      NOT NULL,
    touchpoint_date     DATE,

    -- Touchpoint context (denormalized for reporting performance)
    platform            STRING    NOT NULL,
    channel             STRING    NOT NULL,
    campaign_id         STRING,
    ad_group_id         STRING,
    ad_id               STRING,
    touchpoint_type     STRING,             -- "click" | "impression" | "video_view"
    path_position       INT64,             -- 1 = first, path_total_touches = last
    path_total_touches  INT64,

    -- Conversion context
    conversion_type     STRING    NOT NULL,
    conversion_value    NUMERIC,
    deal_value          NUMERIC,

    -- Attribution credit (NUMERIC to prevent floating-point rounding on financial summations)
    credit_weight       NUMERIC   NOT NULL,  -- 0.0–1.0, portion of this conversion credited here
    credit_conversions  NUMERIC   NOT NULL,  -- credit_weight × 1 (fractional conversion count)
    credit_value        NUMERIC,             -- credit_weight × conversion_value
    credit_deal_value   NUMERIC,             -- credit_weight × deal_value (B2B)

    -- Model name (denormalized for easy filtering)
    model_name          STRING    NOT NULL,
    period_start        DATE,
    period_end          DATE,

    -- Ingestion
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY conversion_date
CLUSTER BY model_name, platform, channel, campaign_id
OPTIONS (
    description = "Weighted attribution credit per touchpoint per model run. Primary output of the Analyst agent."
);


-- -----------------------------------------------------------------------------
-- attribution_channel_summary
-- Pre-aggregated channel performance for fast dashboard/reporting queries.
-- Rebuilt on each attribution run. Consumed directly by the paid-media-mcp
-- and skills without requiring the full touchpoint-level join.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.attribution_channel_summary`
(
    summary_id              STRING    NOT NULL,
    run_id                  STRING    NOT NULL,
    model_name              STRING    NOT NULL,
    period_start            DATE      NOT NULL,
    period_end              DATE      NOT NULL,

    -- Dimensions
    platform                STRING    NOT NULL,
    channel                 STRING    NOT NULL,
    conversion_type         STRING    NOT NULL,
    funnel_stage            STRING,

    -- Volume metrics
    total_touches           INT64,
    unique_entities         INT64,           -- unique persons/accounts touched
    first_touch_count       INT64,           -- times this channel was first touch
    last_touch_count        INT64,           -- times this channel was last touch

    -- Attribution credit (from the model)
    attributed_conversions  NUMERIC   NOT NULL,
    attributed_value        NUMERIC,
    attributed_deal_value   NUMERIC,
    credit_share_pct        FLOAT64,         -- share percentage (rate: FLOAT64 ok)

    -- Spend (joined from platform_daily_spend for the same period)
    total_spend             NUMERIC,
    currency                STRING    DEFAULT 'USD',

    -- Efficiency (requires both attribution credit and spend)
    attributed_cpa          NUMERIC,         -- spend / attributed_conversions
    attributed_roas         NUMERIC,         -- attributed_value / spend
    attributed_roi          NUMERIC,         -- (attributed_value - spend) / spend

    -- vs. platform-reported (for discrepancy tracking)
    platform_conversions    NUMERIC,
    platform_cpa            NUMERIC,
    attribution_vs_platform_delta_pct FLOAT64, -- percentage delta (rate: FLOAT64 ok)

    -- Generated
    generated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY period_start
CLUSTER BY model_name, platform, channel
OPTIONS (
    description = "Pre-aggregated attribution results by channel. Rebuilt on each run. Fast read for dashboards and skills."
);
