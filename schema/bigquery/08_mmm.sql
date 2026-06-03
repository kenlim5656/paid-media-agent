-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — MMM LAYER (Task 27)
-- =============================================================================
-- Output tables for the Google Meridian Bayesian Media Mix Model.
-- Written by tools/meridian_analyst_engine.py after each model run.
--
-- Tables in this file:
--   mmm_runs                  — one row per model run: metadata + diagnostics
--   mmm_channel_contributions — one row per channel per run: ROI + contribution
--
-- Relationship to other schema layers:
--   Inputs:  platform_daily_spend (03_platform.sql) — geo × channel × day spend
--            ga4_sessions (sessions table) — baseline traffic control variable
--            conversion_events (02_touchpoints.sql) — KPI (conversion count / value)
--
--   Feeds:   incrementality_lift_results (Task 22 — future) — will write iROAS
--            estimates back here as roi_prior_injected priors for future MMM runs
--            (Task 22 Bayesian calibration hook, see meridian_analyst_engine.py)
--
-- ROAS / ROI definition in this layer:
--   roi_mean = posterior mean of (attributed KPI value / spend) per channel
--   This is the MMM-estimated causal contribution, distinct from MTA roi
--   in the attribution layer (04_attribution.sql) which uses path-level credit.
--
-- Usage:
--   bq query --use_legacy_sql=false < 08_mmm.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: mmm_runs
-- =============================================================================
-- One row per Meridian model run. Records the full execution context:
-- input date range, tensor shape, MCMC hyperparameters, convergence diagnostics,
-- and the roi_priors dictionary used (for Task 22 calibration traceability).
--
-- Convergence interpretation:
--   r_hat_max < 1.05   — excellent convergence
--   r_hat_max < 1.10   — acceptable convergence
--   r_hat_max ≥ 1.10   — poor convergence; increase n_draws or n_adapt
--
--   ess_bulk_min > 400 — reliable posterior estimates
--   ess_bulk_min < 100 — unreliable; increase n_draws
--
--   n_divergences = 0  — ideal
--   n_divergences > 0  — increase target_accept toward 0.95
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.mmm_runs`
(
    run_id                   STRING    NOT NULL,  -- UUID, matches artifact directory name
    run_started_at           TIMESTAMP NOT NULL,
    status                   STRING    NOT NULL,
    -- "completed"    successful run with posterior samples
    -- "failed"       exception during sampling (check Cloud Logging)
    -- "converged"    completed + r_hat_max < 1.05 (set by post-run checker)
    -- "needs_review" completed + r_hat_max ≥ 1.10 (flag for analyst review)

    -- ── Data window ──────────────────────────────────────────────────────────
    date_from                DATE      NOT NULL,  -- earliest date in extraction window
    date_to                  DATE      NOT NULL,  -- latest date in extraction window

    -- ── Tensor shape ─────────────────────────────────────────────────────────
    n_geos                   INT64     NOT NULL,  -- G: number of geographic regions
    n_weeks                  INT64     NOT NULL,  -- T: number of weekly time periods
    n_channels               INT64     NOT NULL,  -- C: number of media channels
    geo_index                JSON      NOT NULL,  -- ordered list of geo labels
    channel_index            JSON      NOT NULL,  -- ordered list of channel labels

    -- ── MCMC configuration ───────────────────────────────────────────────────
    n_draws                  INT64,               -- post-warmup draws per chain
    n_chains                 INT64,               -- number of parallel MCMC chains
    n_adapt                  INT64,               -- warmup/adaptation draws per chain
    elapsed_seconds          FLOAT64,             -- wall-clock time for sampling

    -- ── Convergence diagnostics ───────────────────────────────────────────────
    r_hat_max                FLOAT64,             -- max R-hat across all parameters (< 1.1 = converged)
    r_hat_mean               FLOAT64,             -- mean R-hat across all parameters
    ess_bulk_min             FLOAT64,             -- minimum bulk ESS across all parameters
    n_divergences            INT64,               -- NUTS divergence count (0 = ideal)

    -- ── Model hyperparameters ─────────────────────────────────────────────────
    max_lag                  INT64,               -- adstock carry-over window (weeks)
    n_time_knots             INT64,               -- spline knots for time trend

    -- ── Bayesian calibration (Task 22 hook) ──────────────────────────────────
    -- Records which channels had experimentally-measured ROI priors injected.
    -- NULL = all channels used weak).
    -- After Task 22: this JSON will contain the experiment IDs and iROAS estimates
    -- that were used to calibrate the priors for this run.
    roi_priors_used          JSON,
    -- Example after Task 22:
    -- {
    --   "google_ads": {"mu": 0.45, "sigma": 0.15, "source": "geo_holdout_2026_q1"},
    --   "meta":       {"mu": 0.28, "sigma": 0.20, "source": "meta_lift_2025_q4"}
    -- }

    -- ── Input data summary ────────────────────────────────────────────────────
    kpi_total                FLOAT64,             -- total KPI units in the modeling window
    spend_total_usd          NUMERIC,             -- total media spend across all channels/geos
    media_zero_pct           FLOAT64,             -- % of [geo×week×channel] cells with 0 impressions

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(run_started_at)
CLUSTER BY status
OPTIONS (
    description = "One row per Meridian MMM run. Records tensor shape, MCMC config, convergence diagnostics, and Task 22 calibration traceability."
);


-- =============================================================================
-- TABLE: mmm_channel_contributions
-- =============================================================================
-- One row per media channel per model run.
-- Contains the posterior ROI distribution and media contribution percentage
-- estimated by Meridian. Use this table for budget allocation decisions and
-- cross-channel reporting in the MCP / Claude Code skills.
--
-- ROI interpretation:
--   roi_mean = expected dollars of KPI value per dollar of media spend
--   roi_p5 / roi_p95 = 90% Bayesian credible interval (not a frequentist CI)
--   Wide intervals (roi_p95/roi_p5 > 5) → inject incrementality priors (Task 22)
--
-- contribution_pct interpretation:
--   Share of total attributed KPI value driven by this channel vs. all channels.
--   Channels with high contribution_pct but low roi_mean are scale-efficient but
--   may be over-invested relative to their marginal return curve.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.mmm_channel_contributions`
(
    contribution_id          STRING    NOT NULL,  -- UUID
    run_id                   STRING    NOT NULL,  -- → mmm_runs.run_id

    -- ── Channel identity ──────────────────────────────────────────────────────
    channel                  STRING    NOT NULL,
    -- Matches channel_index values: "google_ads", "meta", "tiktok", "linkedin", etc.

    -- ── Input data summary ────────────────────────────────────────────────────
    total_spend_usd          NUMERIC,             -- total spend for this channel in the modeling window
    total_impressions        FLOAT64,             -- total impressions (raw media volume input)

    -- ── ROI posterior distribution (90% credible interval) ────────────────────
    roi_mean                 FLOAT64,             -- posterior mean ROI (primary decision metric)
    roi_p5                   FLOAT64,             -- 5th percentile of posterior (lower bound)
    roi_p50                  FLOAT64,             -- posterior median (more robust than mean)
    roi_p95                  FLOAT64,             -- 95th percentile (upper bound)
    contribution_pct         FLOAT64,             -- % of total attributed KPI value from this channel

    -- ── Task 22 calibration traceability ─────────────────────────────────────
    -- Flags whether this channel's ROI prior was calibrated from an incrementality
    -- experiment (Task 22) or is using the
    roi_prior_injected       BOOL,
    roi_prior_source         STRING,              -- experiment/test ID from incrementality_lift_results
    roi_prior_mu             FLOAT64,             -- the mu value injected (NULL if)
    roi_prior_sigma          FLOAT64,             -- the sigma value injected (NULL if)

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY run_id, channel
OPTIONS (
    description = "Per-channel ROI posterior and contribution % from each Meridian MMM run. Core output for budget allocation decisions."
);


-- =============================================================================
-- VIEW: v_mmm_latest_roi
-- =============================================================================
-- Latest ROI estimates per channel from the most recent converged MMM run.
-- This is the primary view surfaced by the MCP mmm tools and /paid-media/mmm skill.
--
-- Only includes runs with status IN ('completed', 'converged') to exclude
-- failed or unconverged runs from the decision-making surface.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_mmm_latest_roi` AS

WITH latest_run AS (
    SELECT run_id, date_from, date_to, r_hat_max, n_draws, n_chains,
           roi_priors_used, spend_total_usd, kpi_total, n_geos, n_weeks
    FROM `{project}.{dataset}.mmm_runs`
    WHERE status IN ('completed', 'converged')
    QUALIFY ROW_NUMBER() OVER (ORDER BY run_started_at DESC) = 1
)

SELECT
    cc.channel,
    cc.total_spend_usd,
    cc.total_impressions,
    cc.roi_mean,
    cc.roi_p5,
    cc.roi_p50,
    cc.roi_p95,
    cc.contribution_pct,

    -- Width of 90% credible interval as a signal of estimate uncertainty
    cc.roi_p95 - cc.roi_p5                                     AS roi_ci_width,

    -- Calibration flag — was this channel grounded by a real experiment?
    cc.roi_prior_injected,
    cc.roi_prior_source,

    -- Run context
    lr.run_id,
    lr.date_from,
    lr.date_to,
    lr.r_hat_max,
    lr.n_geos,
    lr.n_weeks,
    lr.spend_total_usd                                         AS total_modeled_spend,
    lr.kpi_total                                               AS total_modeled_kpi

FROM `{project}.{dataset}.mmm_channel_contributions` cc
JOIN latest_run lr ON cc.run_id = lr.run_id
ORDER BY cc.roi_mean DESC;
