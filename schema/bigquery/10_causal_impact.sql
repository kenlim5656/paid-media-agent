-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — CAUSAL IMPACT LAYER (Task 24)
-- =============================================================================
-- Output tables for the Bayesian Structural Time Series (BSTS) causal impact
-- engine in tools/causal_analyst_engine.py.
--
-- Tables in this file:
--   causal_impact_runs      — one row per analysis run: metadata + diagnostics
--   causal_impact_metrics   — cumulative and point-in-time effect estimates
--
-- Relationship to other schema layers:
--   Inputs:  platform_daily_spend (03_platform.sql) — target + control series
--            attribution_results  (04_attribution.sql) — path-level KPI as target
--            ga4_sessions (sessions table) — web traffic as control variable
--
--   Complements:
--     incrementality_lift_results (09_incrementality.sql) — experimental iROAS
--     (causal_impact = observational; incrementality = experimental)
--
-- Method distinction:
--   Incrementality testing (Task 22): prospective A/B / geo holdout design
--   Causal impact (Task 24): retrospective analysis of unplanned events
--   Both feed Meridian MMM (Task 27) roi_priors from different evidence types.
--
-- BSTS model:
--   LocalLinearTrend + Seasonal(7) + LinearRegression(control_series)
--   Fit on pre-period data via HMC (JAX-backed TFP: tfp.substrates.jax.sts).
--   Counterfactual = model forecast for the post-intervention window.
--   absolute_effect = actual - counterfactual_mean
--   posterior_tail_probability = P(counterfactual ≥ actual) — Bayesian p-value
--
-- Usage:
--   bq query --use_legacy_sql=false < 10_causal_impact.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: causal_impact_runs
-- =============================================================================
-- One row per BSTS causal impact run. Records the full analysis context:
-- what was measured, what period, what model was used, and whether inference
-- converged (R-hat diagnostic).
--
-- R-hat convergence (same interpretation as mmm_runs):
--   r_hat_max < 1.05  — excellent convergence
--   r_hat_max < 1.10  — acceptable
--   r_hat_max ≥ 1.10  — re-run with more draws or a longer pre-period
--
-- pre:post ratio requirements:
--   The BSTS engine enforces a minimum 4:1 ratio of pre-period to post-period
--   observations. Shorter pre-periods risk structural trend mis-estimation.
--   Minimum recommended: 28 pre-period days for a 7-day post window.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.causal_impact_runs`
(
    run_id                   STRING    NOT NULL,  -- UUID

    -- ── What was analyzed ─────────────────────────────────────────────────────
    target_metric            STRING    NOT NULL,
    -- "conversions" | "sessions" | "revenue" | "spend" | "impressions"
    target_channel           STRING,              -- e.g. "google_ads", "meta"; NULL = cross-channel
    target_geo               STRING,              -- ISO country code; NULL = all geos
    intervention_description STRING,              -- text description of the marketing event

    -- ── Analysis windows ──────────────────────────────────────────────────────
    pre_period_from          DATE      NOT NULL,  -- start of pre-intervention baseline
    pre_period_to            DATE      NOT NULL,  -- end of pre-intervention baseline (day before event)
    post_period_from         DATE      NOT NULL,  -- start of intervention / post period
    post_period_to           DATE      NOT NULL,  -- end of intervention window
    n_pre_periods            INT64,               -- number of pre-period time points (days)
    n_post_periods           INT64,               -- number of post-period time points (days)
    pre_post_ratio           FLOAT64,             -- n_pre / n_post (must be ≥ 4.0)

    -- ── Model configuration ───────────────────────────────────────────────────
    model_components         JSON,
    -- List of STS components used. Example:
    -- ["LocalLinearTrend", "Seasonal(7)", "LinearRegression(meta,linkedin)"]
    control_series_names     JSON,                -- list of control channel/series names used
    n_control_series         INT64,               -- K — number of control covariates
    zero_smoothing_applied   BOOL,  -- whether rolling-mean smoothing was applied

    -- ── MCMC configuration ────────────────────────────────────────────────────
    n_draws                  INT64,               -- post-warmup HMC draws per chain
    n_chains                 INT64,               -- number of parallel chains
    n_warmup                 INT64,               -- HMC warmup steps per chain

    -- ── Convergence diagnostics ───────────────────────────────────────────────
    r_hat_max                FLOAT64,             -- max R-hat across STS parameters (< 1.1 converged)
    elapsed_seconds          FLOAT64,             -- wall-clock sampling time

    -- ── Execution metadata ────────────────────────────────────────────────────
    status                   STRING    NOT NULL,
    -- "completed"           — inference finished, results written
    -- "failed"              — exception during sampling
    -- "insufficient_data"   — pre:post ratio < 4, or too few observations
    -- "no_effect"           — posterior_tail_prob ≥ 0.10 (no significant effect)
    error_message            STRING,              -- populated on failure

    -- ── Analyst context ───────────────────────────────────────────────────────
    analyst_notes            STRING,              -- free-form context or caveats
    created_by               STRING,              -- "analyst_agent" or analyst name
    created_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY target_channel, target_metric
OPTIONS (
    description = "One row per BSTS causal impact run. Tracks analysis windows, model config, MCMC diagnostics, and convergence for retrospective marketing event analysis."
);


-- =============================================================================
-- TABLE: causal_impact_metrics
-- =============================================================================
-- Effect estimates from each causal impact run.
-- Stores both cumulative (aggregate over post period) and daily (point-in-time)
-- rows. The cumulative row is the primary decision-making surface.
--
-- Effect interpretation:
--   absolute_effect > 0  — marketing event had a positive effect vs. counterfactual
--   absolute_effect < 0  — marketing event had a negative effect (e.g. spend halt)
--   posterior_tail_probability < 0.10 → significant (≥ 90% certainty)
--
-- Counterfactual interpretation:
--   counterfactual_mean = E[what would have happened without the event]
--   The BSTS model estimates this from pre-period structure + control covariates.
--   Wide CI (counterfactual_upper - counterfactual_lower) → high model uncertainty;
--   consider adding more pre-period data or better control series.
--
-- Bayesian p-value (posterior_tail_probability):
--   P(effect ≤ 0 | data) for one-sided test (H1: event had positive effect)
--   Equivalent to the frequentist p-value in interpretation but derived from
--   the posterior predictive distribution rather than a sampling distribution.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.causal_impact_metrics`
(
    metric_id                STRING    NOT NULL,  -- UUID
    run_id                   STRING    NOT NULL,  -- → causal_impact_runs.run_id

    -- ── Period classification ─────────────────────────────────────────────────
    period_type              STRING    NOT NULL,
    -- "cumulative"  — aggregate over the entire post period (primary decision row)
    -- "average_daily" — average daily effect over the post period
    -- "daily"       — point-in-time row (one per post-period date)
    period_date              DATE,                -- populated for period_type = "daily"

    -- ── Observed vs. counterfactual ────────────────────────────────────────────
    actual_value             FLOAT64,             -- observed metric value (sum for cumulative, daily for daily)
    counterfactual_mean      FLOAT64,             -- E[counterfactual] from BSTS posterior
    counterfactual_lower_90  FLOAT64,             -- 5th percentile of counterfactual posterior
    counterfactual_upper_90  FLOAT64,             -- 95th percentile of counterfactual posterior

    -- ── Effect estimates ──────────────────────────────────────────────────────
    absolute_effect          FLOAT64,             -- actual - counterfactual_mean
    absolute_effect_lower_90 FLOAT64,             -- 5th percentile of (actual - counterfactual)
    absolute_effect_upper_90 FLOAT64,             -- 95th percentile of (actual - counterfactual)
    relative_effect_pct      FLOAT64,             -- absolute_effect / counterfactual_mean × 100

    -- ── Statistical significance ───────────────────────────────────────────────
    -- posterior_tail_probability: P(counterfactual ≥ actual | pre-period data)
    -- = fraction of MCMC samples where the counterfactual exceeds the actual
    -- Low values (< 0.10) indicate the observed effect is unlikely to be noise.
    posterior_tail_probability FLOAT64,           -- Bayesian p-value analog (one-tailed)
    is_significant           BOOL,                -- posterior_tail_probability < 0.10
    -- Statistical Certainty Index = 1 - posterior_tail_probability (reported to user)
    -- e.g. p=0.04 → 96% certainty

    -- ── Uncertainty signal ────────────────────────────────────────────────────
    -- Wide CI → model is uncertain; narrow CI → strong structural identification
    -- Ratio > 3 suggests considering more pre-period data or better control series
    counterfactual_ci_width  FLOAT64,             -- counterfactual_upper_90 - counterfactual_lower_90

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY run_id, period_type
OPTIONS (
    description = "Causal effect estimates per BSTS run. Stores cumulative, average_daily, and per-day rows. posterior_tail_probability is the Bayesian p-value analog."
);
