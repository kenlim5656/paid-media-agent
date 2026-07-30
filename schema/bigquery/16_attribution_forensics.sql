-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — ATTRIBUTION FORENSICS LAYER (Task 37)
-- =============================================================================
-- Forensic audit pipeline that identifies structural tracking anomalies,
-- CRM data overwrites, and phantom conversion events across the media stack.
-- Correction weights produced by this layer feed directly into the MMM
-- pipeline (meridian_data_loader.py) and multi-touch attribution runs.
--
-- Tables / views in this file:
--   data_attribution_anomalies      — audit table: one row per detected anomaly
--   v_attribution_correction_weights — diagnostic view: channel/geo/week
--                                      correction multipliers for MMM calibration
--
-- Three forensic tests (run by tools/attribution_verifier.py):
--   1. Orphaned Token Test:      CRM lead has offline LeadSource but paid click
--                                token (gclid/fbclid/msclkid/ttclid) on session
--   2. Timestamp Divergence:     Lead source was reassigned to offline label
--                                AFTER the paid click token was captured
--   3. Phantom Conversion Test:  Platform pixel reports conversions that have
--                                no matching record in CRM within the attribution
--                                window (usually 7 days)
--
-- Downstream integration:
--   → tools/meridian_data_loader.py applies v_attribution_correction_weights
--     multipliers to KPI tensors before MMM packaging (Component 4)
--   → audit_data_attribution_cleanliness Analyst agent tool surfaces findings
--     with a data cleanliness score and per-workflow recommendations
--
-- Privacy / compliance:
--   • No raw PII (emails, names, IP addresses) is stored in this layer.
--   • crm_lead_id stores the CRM account_id / lead record ID (opaque ID), not
--     personally identifiable information.
--   • session_id is an opaque analytics identifier — no IP or device fingerprint.
--
-- Usage:
--   bq query --use_legacy_sql=false < 16_attribution_forensics.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: data_attribution_anomalies
-- =============================================================================
-- Audit log of every attribution anomaly detected by the forensic engine.
-- Written by tools/attribution_verifier.py AttributionVerifier.run_audit().
-- One row per detected anomaly — multiple anomaly types share this table.
--
-- anomaly_type_enum values:
--   "orphaned_token"       — paid click token + offline CRM lead source
--   "timestamp_divergence" — lead source changed to offline AFTER click was captured
--   "phantom_conversion"   — platform-reported conversion with no CRM match
--
-- detection_method values:
--   "sql_orphan_scan"        — join sessions→CRM leads for token/source mismatch
--   "timestamp_compare"      — compare session click timestamp vs lead modified date
--   "pixel_crm_mismatch"     — compare platform_daily_spend vs conversion_events count
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.data_attribution_anomalies`
(
    anomaly_id                STRING    NOT NULL,  -- UUID
    run_id                    STRING    NOT NULL,  -- UUID — groups all anomalies from one audit run

    -- ── Affected record identity ──────────────────────────────────────────────
    crm_lead_id               STRING,
    -- CRM account_id or lead record ID of the affected lead.
    -- NULL for phantom_conversion anomalies (no matching CRM record exists).
    -- This is an opaque system ID — NOT an email or personal identifier.

    flagged_platform          STRING,
    -- The paid media platform implicated in the anomaly.
    -- "google_ads" | "meta" | "microsoft_ads" | "tiktok" | "linkedin" |
    -- "utm_paid" (UTM-only; no click ID) | "unknown"

    -- ── Anomaly classification ────────────────────────────────────────────────
    anomaly_type_enum         STRING    NOT NULL,
    -- "orphaned_token" | "timestamp_divergence" | "phantom_conversion"

    token_type                STRING,
    -- Which tracking token was found: "gclid" | "fbclid" | "msclkid" |
    -- "ttclid" | "li_fat_id" | "utm_paid" | NULL (phantom_conversion)

    -- ── Channel attribution context ────────────────────────────────────────────
    crm_lead_source           STRING,
    -- The LeadSource value stored in the CRM at detection time.
    -- Examples: "Content Syndication", "SDR Cold Outreach", "Webinar Ingestion"
    -- NULL for phantom_conversion (no CRM lead found).

    claimed_channel           STRING,
    -- What the CRM claims as the attribution channel (usually = crm_lead_source).
    -- This is the channel that will receive incorrect credit if uncorrected.

    expected_channel          STRING,
    -- What the forensic engine infers the attribution channel should be,
    -- based on the click token or UTM data.
    -- NULL for phantom_conversion (no basis for CRM attribution).

    -- ── Financial impact ──────────────────────────────────────────────────────
    estimated_pipeline_value  NUMERIC,
    -- Estimated open pipeline ARR/ACV associated with this lead at detection time.
    -- Source: crm_opportunities_staging.amount (open opportunities only).
    -- NULL if no open opportunity is linked to this lead.
    -- NUMERIC, not FLOAT64, per financial field convention.

    -- ── Detection metadata ────────────────────────────────────────────────────
    confidence_score          FLOAT64,
    -- Detection confidence from 0.0 to 1.0.
    -- orphaned_token:       0.85 (structural match — high confidence)
    -- timestamp_divergence: 0.70–0.95 (scales with lag duration)
    -- phantom_conversion:   0.60–0.95 (scales with gap magnitude)

    session_id                STRING,
    -- Analytics session ID from the sessions table. Links the anomaly to the
    -- specific session that carried the click token. NULL for phantom conversions.

    detection_method          STRING,
    -- "sql_orphan_scan" | "timestamp_compare" | "pixel_crm_mismatch"

    -- ── Geo and time context ──────────────────────────────────────────────────
    anomaly_week              DATE,
    -- Calendar date of the anomaly event (session date, overwrite date, or
    -- platform conversion date). Used for weekly aggregation in the view.
    -- Named anomaly_week rather than anomaly_date because the correction view
    -- groups by DATE_TRUNC(anomaly_week, WEEK(MONDAY)).

    geo_country_code          STRING,
    -- ISO 3166-1 country code from the session or platform spend record.
    -- "XX" when geo cannot be resolved.

    -- ── Audit ────────────────────────────────────────────────────────────────
    detected_timestamp        TIMESTAMP NOT NULL,
)
PARTITION BY DATE(detected_timestamp)
CLUSTER BY flagged_platform, anomaly_type_enum, geo_country_code;


-- =============================================================================
-- VIEW: v_attribution_correction_weights
-- =============================================================================
-- Diagnostic view that aggregates data_attribution_anomalies by channel,
-- geography, and ISO week. Produces a correction_multiplier vector (0.60–1.0)
-- designed to adjust raw platform-reported conversion signals before they
-- are packaged into the Meridian MMM KPI tensor.
--
-- Correction multiplier design rationale:
--   Each anomaly type has a severity weight:
--     phantom_conversion:   1.00  (platform over-reports conversions — direct bias)
--     timestamp_divergence: 0.70  (wrong channel credited — attribution noise)
--     orphaned_token:       0.50  (mislabeled but real conversion — lower priority)
--
--   The multiplier is:
--     1.0 - LEAST(phantom_share_weighted, 0.40)
--   where phantom_share_weighted is the weighted anomaly fraction relative
--   to total platform-reported conversions for the same channel/geo/week.
--   Floor: 0.60 — never discount more than 40% of a channel's conversions.
--
--   A multiplier of 1.0 = fully clean; 0.60 = severe contamination detected.
--
-- quality_tier classification:
--   "clean"        — no phantom conversions, ≤ 2 total anomalies in the window
--   "degraded"     — some anomalies but no phantoms; mild attribution drift
--   "contaminated" — phantom conversions detected OR high anomaly density
--
-- Consumed by:
--   tools/meridian_data_loader.load_attribution_correction_weights()
--   → applied to KPI tensor when apply_attribution_correction=True
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_attribution_correction_weights`
AS
WITH deduped_anomalies AS (
    -- Deduplicate across multiple audit runs: for each unique
    -- (lead, anomaly type, platform, week) keep the most recent detection.
    -- Phantom conversions have no crm_lead_id so use (platform, geo, week).
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    COALESCE(
                        crm_lead_id,
                        CONCAT(COALESCE(flagged_platform, 'unknown'), '|',
                               COALESCE(geo_country_code, 'XX'), '|',
                               CAST(anomaly_week AS STRING))
                    ),
                    anomaly_type_enum,
                    COALESCE(flagged_platform, 'unknown'),
                    anomaly_week
                ORDER BY detected_timestamp DESC
            ) AS rn
        FROM `{project}.{dataset}.data_attribution_anomalies`
        WHERE anomaly_week >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    )
    WHERE rn = 1
),

anomaly_weekly AS (
    -- Aggregate anomaly counts and severity per channel / geo / week
    SELECT
        COALESCE(flagged_platform, 'unknown')           AS channel,
        COALESCE(geo_country_code, 'XX')                AS geo_country_code,
        DATE_TRUNC(anomaly_week, WEEK(MONDAY))          AS week_start,

        COUNT(*)                                        AS anomaly_count,
        COUNTIF(anomaly_type_enum = 'orphaned_token')        AS orphaned_token_count,
        COUNTIF(anomaly_type_enum = 'timestamp_divergence')  AS timestamp_divergence_count,
        COUNTIF(anomaly_type_enum = 'phantom_conversion')    AS phantom_conversion_count,

        -- Pipeline value at risk: sum of estimated_pipeline_value across anomalies
        CAST(
            SUM(COALESCE(CAST(estimated_pipeline_value AS FLOAT64), 0.0))
            AS NUMERIC
        )                                               AS estimated_at_risk_pipeline,

        -- Weighted severity: phantom=1.0, divergence=0.7, orphan=0.5
        -- Used to compute anomaly density vs platform conversions
        SUM(
            COALESCE(confidence_score, 0.75) * CASE anomaly_type_enum
                WHEN 'phantom_conversion'   THEN 1.00
                WHEN 'timestamp_divergence' THEN 0.70
                WHEN 'orphaned_token'       THEN 0.50
                ELSE 0.40
            END
        )                                               AS weighted_severity_sum
    FROM deduped_anomalies
    WHERE anomaly_week IS NOT NULL
    GROUP BY 1, 2, 3
),

platform_weekly AS (
    -- Platform-reported conversions for the same channel/geo/week windows.
    -- Used as denominator for anomaly density calculation.
    SELECT
        c.platform                                      AS channel,
        COALESCE(s.geo_country_code, 'XX')              AS geo_country_code,
        DATE_TRUNC(s.date, WEEK(MONDAY))                AS week_start,
        SUM(CAST(s.platform_conversions AS FLOAT64))    AS total_platform_conversions
    FROM `{project}.{dataset}.platform_daily_spend` s
    LEFT JOIN `{project}.{dataset}.platform_campaigns` c
           ON s.campaign_id = c.campaign_id
    WHERE s.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      AND c.platform IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    a.channel,
    a.geo_country_code,
    a.week_start,

    -- ── Anomaly breakdown ─────────────────────────────────────────────────────
    a.anomaly_count,
    a.orphaned_token_count,
    a.timestamp_divergence_count,
    a.phantom_conversion_count,
    a.estimated_at_risk_pipeline,
    a.weighted_severity_sum,

    -- ── Platform conversion baseline ──────────────────────────────────────────
    COALESCE(p.total_platform_conversions, 0.0)         AS total_platform_conversions,

    -- ── Anomaly density per 1,000 platform conversions ────────────────────────
    -- Measures how saturated a channel/geo/week is with anomalies relative to volume.
    -- High density in a low-conversion channel = higher contamination risk.
    ROUND(
        SAFE_DIVIDE(a.weighted_severity_sum,
                    NULLIF(p.total_platform_conversions, 0)) * 1000,
        4
    )                                                   AS anomaly_density_per_1k,

    -- ── Correction multiplier ────────────────────────────────────────────────
    -- Applied to the KPI tensor in meridian_data_loader before MMM packaging.
    -- Logic:
    --   phantom_share = phantom conversions / (platform_conversions + phantom_count)
    --   divergence_share = timestamp_divergence_count × 0.7 / denominator
    --   correction = GREATEST(0.60, 1.0 - LEAST(phantom_share + divergence_share, 0.40))
    -- Floor at 0.60: never discount more than 40% of conversions for one channel.
    ROUND(
        GREATEST(
            0.60,
            1.0 - LEAST(
                SAFE_DIVIDE(
                    a.phantom_conversion_count
                    + a.timestamp_divergence_count * 0.70,
                    NULLIF(
                        COALESCE(p.total_platform_conversions, 0.0) + a.anomaly_count,
                        0
                    )
                ),
                0.40  -- cap correction impact: never discount more than 40%
            )
        ),
        4
    )                                                   AS correction_multiplier,

    -- ── Quality tier ─────────────────────────────────────────────────────────
    CASE
        WHEN a.phantom_conversion_count = 0 AND a.anomaly_count <= 2 THEN 'clean'
        WHEN a.phantom_conversion_count > 0 OR a.anomaly_count > 10  THEN 'contaminated'
        ELSE 'degraded'
    END                                                 AS quality_tier,

    CURRENT_TIMESTAMP()                                 AS computed_at
FROM anomaly_weekly a
LEFT JOIN platform_weekly p
       ON p.channel         = a.channel
      AND p.geo_country_code = a.geo_country_code
      AND p.week_start       = a.week_start;
