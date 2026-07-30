-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — INCREMENTALITY LAYER (Task 22)
-- =============================================================================
-- Tables and views for tracking incrementality experiments and lift results.
-- Written by agents/analyst/agent.py _tool_run_incrementality_analysis().
--
-- Tables in this file:
--   incrementality_experiments    — experiment design registry (one row per test)
--   incrementality_lift_results   — measured lift outcomes per experiment per channel
--
-- Views in this file:
--   v_incrementality_roi_priors   — latest significant result per channel,
--                                   formatted for direct Meridian prior injection
--
-- Relationship to other schema layers:
--   Inputs:  platform_daily_spend (03_platform.sql) — spend + impressions by geo/channel
--            conversion_events (02_touchpoints.sql) — KPI outcomes
--            attribution_results (04_attribution.sql) — optional path-level KPI
--
--   Feeds:   mmm_runs (08_mmm.sql) → roi_priors_used (Task 27 Bayesian calibration hook)
--            tiktok_ads_client.py → push_domain_suppression()
--            google_ads_client.py → push_domain_suppression()
--
-- iROAS definition:
--   iROAS = incremental_conversions * avg_conversion_value / incremental_spend
--   Where incremental = test group outcome - counterfactual (control rate × test exposure)
--   This is the causal lift estimate, not the reported platform ROAS.
--
-- Bayesian calibration workflow:
--   1. Run run_incrementality_analysis() in the Analyst agent
--   2. Results write here with roi_prior_mu / roi_prior_sigma (log-normal parameters)
--   3. v_incrementality_roi_priors selects latest significant results per channel
--   4. run_mmm_pipeline() auto-reads that view when roi_priors=None
--   5. Meridian's MCMC posterior is anchored to experimentally measured lift
--
-- Usage:
--   bq query --use_legacy_sql=false < 09_incrementality.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: incrementality_experiments
-- =============================================================================
-- One row per experiment design. Created when run_incrementality_analysis()
-- is called with a new experiment_id. Idempotent — safe to rerun.
--
-- The experiment record captures the "what and how" of the test design.
-- Lift results (the "what happened") live in incrementality_lift_results.
--
-- Methodology definitions:
--   geo_holdout     — geographic market split: some regions receive treatment,
--                     others serve as control (gold standard for digital/paid media)
--   conversion_lift — time-based split: pre-period vs. treatment period for same channels
--                     (less clean than geo split; susceptible to seasonality confounds)
--   brand_lift      — survey-based lift measurement (Google Brand Lift, Meta Brand Survey)
--                     Results entered manually from platform-provided reports.
--   synthetic_control — DID/SCM approach using synthetic control constructed from
--                       weighted combination of non-treated markets
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.incrementality_experiments`
(
    experiment_id            STRING    NOT NULL,  -- user-defined ID, e.g. "google_ads_geo_holdout_2026_q2"

    -- ── Experiment identity ────────────────────────────────────────────────────
    channel                  STRING    NOT NULL,
    -- Matches platform field in platform_daily_spend: "google_ads", "meta", "tiktok", "linkedin"
    platform                 STRING,              -- raw platform name (may differ from channel rollup)
    test_name                STRING,              -- human-readable description

    -- ── Design ────────────────────────────────────────────────────────────────
    methodology              STRING    NOT NULL,
    -- "geo_holdout" | "conversion_lift" | "brand_lift" | "synthetic_control"
    test_group_ids           JSON,                -- campaign/region IDs in test group (as JSON array)
    control_group_ids        JSON,                -- campaign/region IDs in control group

    -- ── Timeline ──────────────────────────────────────────────────────────────
    test_date_from           DATE,                -- start of treatment period
    test_date_to             DATE,                -- end of treatment period
    control_date_from        DATE,                -- start of comparison/baseline period
    control_date_to          DATE,                -- end of comparison/baseline period

    -- ── Statistical power design ──────────────────────────────────────────────
    kpi                      STRING,              -- "conversions" | "revenue" | "sessions"
    min_detectable_effect    FLOAT64,             -- minimum lift % this test can detect at target power
    target_power             FLOAT64,             -- statistical power target, typically 0.80
    test_sample_size         INT64,               -- planned test group size (impressions or users)
    control_sample_size      INT64,               -- planned control group size

    -- ── Status ────────────────────────────────────────────────────────────────
    status                   STRING    NOT NULL,
    -- "designed"     — planned, not yet started
    -- "running"      — test is live
    -- "completed"    — test ended, results computed
    -- "invalidated"  — test contaminated (e.g., geo bleed-over, holiday disruption)
    invalidation_reason      STRING,              -- explanation if status = "invalidated"

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_by               STRING,              -- "analyst_agent" or analyst name
    notes                    STRING,              -- free-form notes about design decisions
    created_at               TIMESTAMP NOT NULL,
    updated_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY channel, methodology
OPTIONS (
    description = "Incrementality experiment design registry. One row per test. Results live in incrementality_lift_results."
);


-- =============================================================================
-- TABLE: incrementality_lift_results
-- =============================================================================
-- One row per measurement per experiment. A single experiment may have multiple
-- result rows (e.g., midpoint read, final read, re-analysis after cleanup).
--
-- The most important columns for downstream use:
--   roi_prior_mu, roi_prior_sigma  → Meridian ModelSpec calibration (Task 27)
--   is_active = TRUE               → this result is used in the MMM auto-wiring
--   is_significant = TRUE          → p_value < (1 - confidence_level)
--
-- ROI prior parameterization (log-normal):
--   roi_prior_mu    = posterior mean of log(ROI) after Bayesian update
--   roi_prior_sigma = posterior std of log(ROI) after Bayesian update
--   E[ROI] ≈ exp(roi_prior_mu + roi_prior_sigma² / 2)
--
--   For calibrated channels:   roi_prior_sigma ≈ 0.10–0.25  (tight)
--   For uncalibrated channels: roi_prior_sigma = 0.9         (weak,)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.incrementality_lift_results`
(
    result_id                STRING    NOT NULL,  -- UUID, auto-generated
    experiment_id            STRING    NOT NULL,  -- → incrementality_experiments.experiment_id

    -- ── Channel identity ──────────────────────────────────────────────────────
    channel                  STRING    NOT NULL,
    methodology              STRING    NOT NULL,
    measurement_date         DATE      NOT NULL,  -- date analysis was run

    -- ── Measurement window ────────────────────────────────────────────────────
    test_date_from           DATE,
    test_date_to             DATE,
    control_date_from        DATE,
    control_date_to          DATE,
    measurement_window_days  INT64,               -- actual days measured (may differ from planned)
    kpi                      STRING,              -- "conversions" | "revenue" | "sessions"

    -- ── Raw data inputs ────────────────────────────────────────────────────────
    kpi_test                 FLOAT64,             -- total KPI units in test group
    kpi_control              FLOAT64,             -- total KPI units in control group
    exposed_test             FLOAT64,             -- exposure count in test group (impressions)
    exposed_control          FLOAT64,             -- exposure count in control group
    spend_test               NUMERIC,             -- total spend in test group (USD, NUMERIC for precision)
    spend_control            NUMERIC,             -- total spend in control/comparison group
    avg_conversion_value     NUMERIC,             -- $ value per conversion (for iROAS)

    -- ── Lift estimation ────────────────────────────────────────────────────────
    cvr_test                 FLOAT64,             -- conversion rate (kpi / exposed) in test group
    cvr_control              FLOAT64,             -- conversion rate in control group
    lift_pct                 FLOAT64,             -- incremental lift: (cvr_test - cvr_control) / cvr_control
    lift_pct_lower_90        FLOAT64,             -- 90% CI lower bound on lift_pct
    lift_pct_upper_90        FLOAT64,             -- 90% CI upper bound on lift_pct

    -- ── iROAS estimation ──────────────────────────────────────────────────────
    incremental_conversions  FLOAT64,             -- (cvr_test - cvr_control) × exposed_test
    incremental_spend        NUMERIC,             -- total test spend attributable to incremental effect
    iroas_mean               FLOAT64,             -- point estimate of incremental ROAS (natural scale)
    iroas_std                FLOAT64,             -- std dev of iROAS estimate (from CI)
    iroas_lower_90           FLOAT64,             -- 90% CI lower bound
    iroas_upper_90           FLOAT64,             -- 90% CI upper bound

    -- ── Statistical significance ───────────────────────────────────────────────
    z_score                  FLOAT64,             -- two-proportion z-test statistic
    p_value                  FLOAT64,             -- one-tailed p-value (H1: test > control)
    confidence_level         FLOAT64,             -- e.g. 0.90 (90% CI)
    is_significant           BOOL      NOT NULL,  -- p_value < (1 - confidence_level)

    -- ── Bayesian prior parameters for Meridian (Task 27) ──────────────────────
    -- Computed via log-normal Bayesian update from the experimental likelihood.
    -- Log-normal conjugate update: combines weak, sigma=0.9)
    -- with the experimental likelihood (log(iROAS_mean), se_log_iroas) to produce
    -- a tighter posterior suitable for Meridian's MCMC prior specification.
    --
    -- These are the values read by v_incrementality_roi_priors and
    -- injected into Meridian ModelSpec as roi_mu_m / roi_sigma_m.
    roi_prior_mu             FLOAT64   NOT NULL,  -- posterior mean of log(ROI) — log-normal location
    roi_prior_sigma          FLOAT64   NOT NULL,  -- posterior std of log(ROI)  — log-normal scale

    -- ── Activation ────────────────────────────────────────────────────────────
    -- is_active controls whether this result is used for Meridian calibration.
    -- Only one result should be active per channel at a time.
    -- The analyst agent sets is_active=TRUE when calling run_incrementality_analysis
    -- with mark_active=True (). Old results are not deactivated automatically;
    -- the v_incrementality_roi_priors view uses QUALIFY to pick the latest.
    is_active                BOOL      NOT NULL,

    -- ── Metadata ──────────────────────────────────────────────────────────────
    notes                    STRING,              -- analyst notes, caveats, limitations
    created_by               STRING,              -- "analyst_agent" or analyst name
    created_at               TIMESTAMP NOT NULL
)
PARTITION BY measurement_date
CLUSTER BY channel, experiment_id
OPTIONS (
    description = "Incrementality lift results. One row per analysis per experiment. roi_prior_mu/sigma feed Meridian's Bayesian calibration via v_incrementality_roi_priors."
);


-- =============================================================================
-- VIEW: v_incrementality_roi_priors
-- =============================================================================
-- Latest active, significant lift result per channel.
-- This is the primary view read by tools/meridian_analyst_engine._get_roi_priors_from_bq()
-- to auto-populate roi_priors when run_mmm_pipeline() is called without explicit priors.
--
-- Selection criteria:
--   is_active = TRUE          — analyst has confirmed this result for calibration
--   is_significant = TRUE     — passed the statistical significance test
--   QUALIFY by latest measurement_date — most recent result wins per channel
--
-- Output is formatted for direct use as the roi_priors dict in Meridian:
--   roi_prior_mu, roi_prior_sigma → ModelSpec.roi_mu_m / roi_sigma_m
--   source (experiment_id)        → written to mmm_channel_contributions.roi_prior_source
--
-- If a channel has no active significant results, it is absent from this view.
-- Absent channels receive the weak, sigma=0.9) in Meridian.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_incrementality_roi_priors` AS

SELECT
    ilr.channel,
    ilr.experiment_id                           AS source,
    ilr.methodology,

    -- ── Meridian prior parameters ──────────────────────────────────────────────
    -- These map directly to MeridianAnalystEngine.roi_priors dict:
    --   roi_priors[channel] = {"mu": roi_prior_mu, "sigma": roi_prior_sigma, "source": source}
    ilr.roi_prior_mu,
    ilr.roi_prior_sigma,

    -- ── Human-readable context ─────────────────────────────────────────────────
    ilr.iroas_mean,
    ilr.iroas_lower_90,
    ilr.iroas_upper_90,
    ilr.lift_pct,
    ilr.p_value,
    ilr.confidence_level,
    ilr.measurement_date,

    -- ── Experiment context ─────────────────────────────────────────────────────
    ie.test_name,
    ie.test_date_from                           AS experiment_test_from,
    ie.test_date_to                             AS experiment_test_to,
    ilr.kpi,
    ilr.spend_test,
    ilr.incremental_conversions,

    -- ── Uncertainty signal ────────────────────────────────────────────────────
    -- Wide CI (iroas_upper_90 / iroas_lower_90 > 5) → consider more samples before
    -- tightening the prior further. Tight CI = high confidence from experiment.
    SAFE_DIVIDE(ilr.iroas_upper_90, GREATEST(ilr.iroas_lower_90, 0.01))
                                                AS iroas_ci_ratio,

    -- Pre-formatted JSON for diagnostic logging in the MMM engine
    TO_JSON_STRING(STRUCT(
        ilr.roi_prior_mu    AS mu,
        ilr.roi_prior_sigma AS sigma,
        ilr.experiment_id   AS source
    ))                                          AS meridian_prior_json

FROM `{project}.{dataset}.incrementality_lift_results` ilr
LEFT JOIN `{project}.{dataset}.incrementality_experiments` ie
       ON ilr.experiment_id = ie.experiment_id

WHERE ilr.is_active = TRUE
  AND ilr.is_significant = TRUE

-- Pick the most recent active+significant result per channel
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ilr.channel
    ORDER BY ilr.measurement_date DESC, ilr.created_at DESC
) = 1

ORDER BY ilr.channel;
