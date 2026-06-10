-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — AGENT OUTPUT LAYER
-- =============================================================================
-- Tables written by the autonomous agents (Watchdog, Analyst, Operator).
-- The paid-media-mcp reads these tables to surface agent insights into skills.
-- This creates the feedback loop: agents run → write here → MCP reads → skills see it.
--
-- Tables in this file:
--   watchdog_alerts            Data quality alerts from the Watchdog agent
--   watchdog_capture_rate_log  Hourly signal capture rate history
--   analyst_insights           Model outputs and anomaly findings
--   operator_action_log        Media actions taken or proposed by the Operator
--   operator_pending_approvals Actions awaiting human approval
-- =============================================================================


-- -----------------------------------------------------------------------------
-- watchdog_alerts
-- Every data quality issue the Watchdog detects. Surfaced by the MCP so
-- practitioners see active alerts in their skill sessions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.watchdog_alerts`
(
    alert_id            STRING    NOT NULL,  -- UUID
    alert_type          STRING    NOT NULL,
    -- "signal_capture_drop"    — gclid/fbclid/etc. capture rate fell below threshold
    -- "null_crm_fields"        — spike in missing media fields on CRM records
    -- "conversion_gap"         — conversion volume anomaly (spike or drop)
    -- "spend_anomaly"          — unexpected spend change (platform outage, runaway budget)
    -- "identity_match_decline" — % of sessions resolving to an entity dropped
    -- "data_freshness"         — table not updated within expected window
    -- "capi_match_rate"        — CAPI/enhanced conversion match rate dropped
    -- "dedup_failure"          — evidence of duplicate conversion counting

    severity            STRING    NOT NULL,  -- "info" | "warning" | "critical"
    status              STRING    NOT NULL,  -- "open" | "acknowledged" | "resolved" | "suppressed"

    -- What broke
    affected_namespace  STRING,             -- which signal namespace (if applicable)
    affected_platform   STRING,
    affected_campaign_id STRING,

    -- Metric details
    metric_name         STRING,             -- e.g. "gclid_capture_rate_pct"
    metric_value        FLOAT64,            -- current value
    threshold_value     FLOAT64,            -- the threshold that was breached
    baseline_value      FLOAT64,            -- what it was before (for context)
    variance_pct        FLOAT64,            -- % change from baseline

    -- Diagnosis
    description         STRING    NOT NULL, -- what happened
    probable_cause      STRING,             -- agent's diagnosis
    recommended_action  STRING,             -- what to do about it

    -- Timing
    detected_at         TIMESTAMP NOT NULL,
    resolved_at         TIMESTAMP,
    acknowledged_by     STRING,
    acknowledged_at     TIMESTAMP,

    -- Notification
    alert_sent          BOOL,
    alert_sent_at       TIMESTAMP,
    alert_channel       STRING,             -- "slack" | "email" | "pagerduty"

    -- Metadata
    run_id              STRING,             -- Watchdog run that detected this
    context             JSON                -- additional diagnostic data
)
PARTITION BY DATE(detected_at)
CLUSTER BY alert_type, severity, status
OPTIONS (
    description = "Data quality alerts from the Watchdog agent. Active alerts surface in skill sessions via the MCP."
);


-- -----------------------------------------------------------------------------
-- watchdog_capture_rate_log
-- Time series of signal capture rates. Used for trend detection and
-- threshold calibration. Also powers the Watchdog's anomaly detection.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.watchdog_capture_rate_log`
(
    log_id              STRING    NOT NULL,
    logged_at           TIMESTAMP NOT NULL,
    measurement_window_hours INT64 NOT NULL,

    namespace_id        STRING    NOT NULL,  -- which signal was measured
    platform            STRING,

    -- Volume
    total_events        INT64     NOT NULL,  -- total touchpoints/sessions in window
    events_with_signal  INT64     NOT NULL,  -- events where this signal was present
    capture_rate_pct    FLOAT64   NOT NULL,  -- events_with_signal / total_events * 100

    -- vs. baseline
    baseline_capture_rate_pct FLOAT64,      -- rolling 7-day average
    variance_from_baseline_pct FLOAT64,
    is_anomaly          BOOL,

    run_id              STRING               -- Watchdog run ID
)
PARTITION BY DATE(logged_at)
CLUSTER BY namespace_id, platform
OPTIONS (
    description = "Hourly signal capture rate time series. Powers Watchdog anomaly detection."
);


-- -----------------------------------------------------------------------------
-- analyst_insights
-- Findings from the Analyst agent: model outputs, anomalies, recommendations.
-- Narrative insights separate from the structured attribution_results data.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.analyst_insights`
(
    insight_id          STRING    NOT NULL,  -- UUID
    run_id              STRING,             -- attribution_runs.run_id (if tied to a model run)
    insight_type        STRING    NOT NULL,
    -- "attribution_summary"    — top-level model output narrative
    -- "channel_anomaly"        — unusual channel performance vs. prior period
    -- "path_pattern"           — notable touchpoint sequence discovered
    -- "model_readiness"        — assessment of whether to upgrade attribution model
    -- "stitching_quality"      — identity match rate assessment
    -- "budget_efficiency"      — attributed CPA/ROAS vs. spend analysis
    -- "incrementality_signal"  — incrementality test result or opportunity
    -- "audience_overlap"       — significant audience overlap detected

    period_start        DATE,
    period_end          DATE,
    affected_platform   STRING,
    affected_channel    STRING,
    affected_campaign_id STRING,

    -- The insight
    headline            STRING    NOT NULL,  -- one sentence summary
    detail              STRING,             -- full analysis narrative
    data_points         JSON,               -- supporting metrics
    confidence          STRING,             -- "high" | "medium" | "low"

    -- Action potential
    has_recommendation  BOOL,
    recommendation      STRING,
    estimated_impact    STRING,             -- e.g. "~15% reduction in attributed CPA"
    priority            STRING,             -- "high" | "medium" | "low"

    -- Status
    status              STRING,  -- "new" | "reviewed" | "actioned" | "dismissed"
    reviewed_at         TIMESTAMP,
    actioned_by         STRING,

    -- Timing
    generated_at        TIMESTAMP NOT NULL
)
PARTITION BY DATE(generated_at)
CLUSTER BY insight_type, priority, status
OPTIONS (
    description = "Narrative insights and recommendations from the Analyst agent. Surfaced in skill sessions via MCP."
);


-- -----------------------------------------------------------------------------
-- operator_action_log
-- Every media action the Operator agent took or proposed.
-- Immutable audit trail — never update or delete.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.operator_action_log`
(
    action_id           STRING    NOT NULL,  -- UUID
    action_type         STRING    NOT NULL,
    -- "budget_reallocation"     — moved budget between line items/campaigns
    -- "budget_pause"            — paused spend on a line item/campaign
    -- "budget_resume"           — resumed paused spend
    -- "budget_adjustment"       — changed daily/lifetime budget
    -- "audience_exclusion"      — added audience to exclusion list
    -- "audience_inclusion"      — added audience to targeting
    -- "bid_adjustment"          — changed bid strategy or target
    -- "creative_suppression"    — paused specific ads/creatives
    -- "frequency_cap_update"    — changed frequency capping
    -- "campaign_status_change"  — paused or activated a campaign

    -- Target of the action
    platform            STRING    NOT NULL,
    platform_entity_type STRING,            -- "campaign" | "ad_group" | "line_item" | "audience"
    platform_entity_id  STRING    NOT NULL,
    campaign_id         STRING,             -- → platform_campaigns.campaign_id

    -- The change
    field_changed       STRING,             -- e.g. "daily_budget" | "status" | "audience_exclusion_list"
    value_before        STRING,             -- previous value (as string for flexibility)
    value_after         STRING,             -- new value
    change_magnitude    FLOAT64,            -- numeric magnitude where applicable (e.g. budget delta)
    change_magnitude_pct FLOAT64,           -- % change

    -- Rationale (from the Analyst insights that drove this)
    rationale           STRING    NOT NULL,
    insight_id          STRING,             -- → analyst_insights.insight_id (if driven by an insight)
    attribution_run_id  STRING,             -- → attribution_runs.run_id (the model run that informed this)

    -- Execution
    execution_mode      STRING    NOT NULL, -- "autonomous" | "pending_approval" | "dry_run"
    status              STRING    NOT NULL,
    -- "proposed"        — staged, not yet executed (pending_approval mode)
    -- "approved"        — approved by human, awaiting execution
    -- "executed"        — successfully applied to the platform
    -- "partial"         — multi-channel action: some channels applied, others failed
    --                     (see operator_action_log.platform_response for detail)
    -- "failed"          — execution attempted but failed
    -- "rejected"        — rejected by human reviewer
    -- "superseded"      — a newer action replaced this one before execution

    -- Guardrail compliance
    guardrail_check_passed BOOL,
    guardrail_notes     STRING,             -- which guardrails were checked and their results

    -- Human review
    requires_approval   BOOL      NOT NULL,
    approved_by         STRING,
    approved_at         TIMESTAMP,
    rejected_by         STRING,
    rejected_at         TIMESTAMP,
    rejection_reason    STRING,

    -- Timing
    proposed_at         TIMESTAMP NOT NULL,
    executed_at         TIMESTAMP,
    rolled_back_at      TIMESTAMP,          -- if the action was later reversed

    -- Platform response
    platform_response   JSON                -- API response from the platform
)
PARTITION BY DATE(proposed_at)
CLUSTER BY platform, action_type, status
OPTIONS (
    description = "Immutable audit trail of all Operator agent media actions. Append-only."
);


-- -----------------------------------------------------------------------------
-- operator_pending_approvals
-- View of actions awaiting human approval. The MCP surfaces this to practitioners
-- in their skill sessions. Updated (not appended) as approvals are processed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.operator_pending_approvals`
(
    action_id           STRING    NOT NULL,  -- → operator_action_log.action_id
    platform            STRING    NOT NULL,
    action_type         STRING    NOT NULL,
    platform_entity_id  STRING    NOT NULL,
    campaign_id         STRING,

    -- Summary for approval UI / skill display
    summary             STRING    NOT NULL,  -- human-readable one-liner: "Reallocate $500 from EMEA Brand to EMEA Retargeting"
    rationale           STRING    NOT NULL,
    estimated_impact    STRING,

    -- Value at stake
    spend_at_risk       FLOAT64,            -- budget involved in this action
    change_magnitude_pct FLOAT64,

    -- Timing
    proposed_at         TIMESTAMP NOT NULL,
    expires_at          TIMESTAMP,          -- auto-reject if not reviewed by this time
    proposed_by         STRING
)
OPTIONS (
    description = "Current pending approval queue for Operator agent actions. Surfaced to practitioners via MCP."
);
