# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
The Analyst — Attribution Modeling Agent.
Runs daily. Stitches identities and executes the MTA model.
Writes structured output to paid-media-schema tables so the paid-media-mcp
can surface results in interactive skill sessions.
"""
import json
import math
import uuid
import structlog
from datetime import datetime, timezone
from typing import Any

from agents.base import BaseAgent
from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Statistical helpers (Task 22) ──────────────────────────────────────────────

# Lookup table for normal quantile z_{1-α/2} at common confidence levels.
# Used by run_incrementality_analysis for CI construction (no scipy required).
_CI_Z_TABLE = {0.80: 1.282, 0.85: 1.440, 0.90: 1.645, 0.95: 1.960, 0.99: 2.576}

# Default Bayesian prior parameters for iROAS (log-normal distribution).
# Matches DEFAULT_ROI_PRIOR_MU / DEFAULT_ROI_PRIOR_SIGMA in meridian_analyst_engine.py.
# Weak priors → data drives the posterior; experiment narrows sigma to 0.10–0.25.
_DEFAULT_PRIOR_MU    = 0.2   # log-normal location: E[ROI] ≈ exp(0.2 + 0.9²/2) ≈ 1.83
_DEFAULT_PRIOR_SIGMA = 0.9   # log-normal scale:    very wide, observational only


def _z_for_confidence(confidence_level: float) -> float:
    """
    Return z_{1-α/2} for two-sided CI at the given confidence level.

    Uses the lookup table for common values; linear interpolation otherwise.
    No external dependencies (no scipy).
    """
    if confidence_level in _CI_Z_TABLE:
        return _CI_Z_TABLE[confidence_level]
    # Linear interpolation between nearest table entries
    levels = sorted(_CI_Z_TABLE.keys())
    for i in range(len(levels) - 1):
        lo, hi = levels[i], levels[i + 1]
        if lo <= confidence_level <= hi:
            frac = (confidence_level - lo) / (hi - lo)
            return _CI_Z_TABLE[lo] + frac * (_CI_Z_TABLE[hi] - _CI_Z_TABLE[lo])
    return 1.645  # fallback to 90%


SYSTEM = """You are the Analyst, an attribution modeling agent for a paid media pipeline.

You are platform-agnostic. The identity graph holds signals from all ad platforms
(Google, Meta, LinkedIn, TikTok, etc.) and analytics tools (GA4, Adobe, Segment).

Your daily run sequence — follow it in order:
1. Call `enrich_sessions` — resolve anonymous sessions to company domains (account-based analytics).
2. Call `start_attribution_run` — registers the run in BigQuery, returns a run_id.
3. Call `stitch_identities` — merges new signals into the identity graph.
4. Call `run_mta_model` with the run_id — computes Full-Path attribution and writes results.
5. Call `build_channel_summary` with the run_id — aggregates to channel level for the MCP.
6. Call `write_analyst_insight` — surface the most important finding as a structured insight.
7. Call `complete_attribution_run` with the run_id — marks run as completed.

Default model: Full-Path (first touch 30%, last touch 30%, middle touches split 40%).
Data-driven models: use run_shapley_model or run_markov_model instead of run_mta_model
when data volume allows (> 1,000 converted paths). Shapley is more accurate but slower;
Markov is faster for many channels. Always compare results to the Full-Path baseline.
Write efficient SQL. Prefer incremental queries over full scans."""


class AnalystAgent(BaseAgent):
    name = "analyst"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "enrich_sessions",
            "description": (
                "Resolve anonymous web sessions to company domains using IP intelligence. "
                "Reads recent sessions from sgtm_request_logs that haven't been enriched yet, "
                "calls the IP intelligence provider (Clearbit / ipinfo.io) for each, and writes "
                "results to: ip_resolution_cache (/24 prefix only — never raw IPs), "
                "company_profiles (upsert), company_sessions (de-anonymized session), and "
                "company_engagement (rolling-30d aggregation). "
                "Run first in the daily sequence before identity stitching and attribution."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_hours": {
                        "type": "integer",
                        "description": "How many hours back to look for unenriched sessions. Default: 48.",
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Max sessions to process in this run. Default: 1000.",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "run_bigquery_query",
            "description": "Execute a SELECT query. Returns up to 200 rows.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "params": {"type": "object", "additionalProperties": True},
                },
                "required": ["sql"],
            },
        },
        {
            "name": "start_attribution_run",
            "description": (
                "Register a new attribution model run in BigQuery. "
                "Returns a run_id that must be passed to all subsequent tools."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "enum": ["full_path", "first_touch", "last_touch", "linear", "time_decay", "position_based"],
                        "description": "Attribution model to run",
                    },
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Conversion types to include, e.g. ['opportunity_created', 'purchase']",
                    },
                },
                "required": ["model_name", "period_start", "period_end"],
            },
        },
        {
            "name": "stitch_identities",
            "description": (
                "Run the identity stitching job: match session signals to canonical entities "
                "using email domain, IP routing, and deterministic click ID co-occurrence. "
                "Writes matches to identity_entity_signals and identity_entities. "
                "Platform-agnostic — works across all signal namespaces."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_days": {"type": "integer", "default": 1},
                },
                "required": [],
            },
        },
        {
            "name": "run_mta_model",
            "description": (
                "Compute Full-Path MTA weights for all conversion paths in the date range. "
                "Inserts touchpoint-level credit rows into attribution_results. "
                "Returns path count and conversion count."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string", "description": "From start_attribution_run"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "model_config": {
                        "type": "object",
                        "description": "Model parameters. For full_path: {first_touch_pct, last_touch_pct}",
                        "additionalProperties": True,
                    },
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "build_channel_summary",
            "description": (
                "Aggregate attribution_results into attribution_channel_summary — "
                "the pre-computed channel-level view read by the paid-media-mcp and skills. "
                "Joins with platform_daily_spend to compute attributed CPA/ROAS."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string"},
                    "period_end":   {"type": "string"},
                    "model_name":   {"type": "string"},
                },
                "required": ["run_id", "period_start", "period_end", "model_name"],
            },
        },
        {
            "name": "run_shapley_model",
            "description": (
                "Compute Shapley value attribution — the game-theoretic fair attribution model. "
                "Gives each channel credit equal to its average marginal contribution across all "
                "coalition orderings. Eliminates first-touch / last-touch bias. "
                "Recommended when path count > 1,000. "
                "Writes results to attribution_results and attribution_channel_summary."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "e.g. ['opportunity_created', 'purchase']",
                    },
                    "max_channels": {"type": "integer", "description": "Cap on channels (default from settings)"},
                    "max_paths":    {"type": "integer", "description": "Sample size cap (default from settings)"},
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "run_markov_model",
            "description": (
                "Compute Markov chain attribution — transition-matrix based model. "
                "Each channel's credit is proportional to its removal effect: "
                "the drop in overall conversion probability when that channel is removed. "
                "Faster than Shapley for many channels. Good for long B2B paths. "
                "Writes results to attribution_results and attribution_channel_summary."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "max_paths": {"type": "integer", "description": "Sample size cap (default from settings)"},
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "write_analyst_insight",
            "description": (
                "Persist a structured insight or recommendation to the analyst_insights table. "
                "Call once per run with the most important finding."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":         {"type": "string"},
                    "insight_type":   {
                        "type": "string",
                        "enum": [
                            "attribution_summary", "channel_anomaly", "path_pattern",
                            "model_readiness", "stitching_quality", "budget_efficiency",
                            "incrementality_signal", "audience_overlap",
                        ],
                    },
                    "headline":       {"type": "string", "description": "One sentence summary"},
                    "detail":         {"type": "string", "description": "Full analysis narrative"},
                    "priority":       {"type": "string", "enum": ["high", "medium", "low"]},
                    "recommendation": {"type": "string"},
                    "estimated_impact": {"type": "string"},
                    "period_start":   {"type": "string"},
                    "period_end":     {"type": "string"},
                    "data_points":    {"type": "object", "additionalProperties": True},
                },
                "required": ["run_id", "insight_type", "headline", "priority"],
            },
        },
        {
            "name": "run_mmm_model",
            "description": (
                "Run a Google Meridian Bayesian Media Mix Model over a specified date range. "
                "Extracts geo-level spend + impressions from platform_daily_spend, aggregates "
                "to weekly [Geo × Time × Channel] tensors, runs MCMC posterior sampling with "
                "the JAX/NumPyro backend, and writes ROI estimates + diagnostics to "
                "mmm_runs and mmm_channel_contributions in BigQuery. "
                "Returns per-channel ROI posterior summaries and convergence diagnostics. "
                "Requires the 'paid-media-agent[mmm]' optional dependencies. "
                "Runtime: 35–45 minutes on Cloud Run (4 vCPU, 16 GB RAM). "
                "IMPORTANT: roi_priors is the Task 22 calibration hook — when incrementality "
                "testing results are available, pass them here to anchor the Bayesian priors "
                "to experimentally measured lift rather than observational data alone."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "date_from": {
                        "type": "string",
                        "description": "Start date for data extraction. Format: YYYY-MM-DD. Minimum 78 weeks (18 months) recommended; 104 weeks (2 years) is ideal.",
                    },
                    "date_to": {
                        "type": "string",
                        "description": "End date for data extraction. Format: YYYY-MM-DD. Typically today or the last complete Sunday.",
                    },
                    "platforms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Restrict to specific platforms (e.g. ['google_ads', 'meta', 'tiktok']). Omit to include all platforms with geo data.",
                    },
                    "geo_allowlist": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Restrict to specific ISO country codes (e.g. ['US', 'CA', 'GB']). Omit to include all geos passing the impressions threshold.",
                    },
                    "roi_priors": {
                        "type": "object",
                        "description": (
                            "Task 22 Bayesian calibration hook. Pass experimentally measured ROI priors "
                            "to anchor the model to real lift data rather than observational patterns. "
                            "Format: {\"google_ads\": {\"mu\": 0.45, \"sigma\": 0.15, \"source\": \"geo_holdout_2026_q1\"}}. "
                            "Omit to use weakly informative defaults (full observational inference)."
                        ),
                        "additionalProperties": True,
                    },
                    "n_draws": {
                        "type": "integer",
                        "description": "MCMC draws per chain. Default 500. Reduce to 250 to stay within Cloud Run 60-min timeout for large datasets.",
                    },
                    "n_chains": {
                        "type": "integer",
                        "description": "Parallel MCMC chains. Default 4 (matches XLA_FLAGS device count). Do not exceed 4 on CPU Cloud Run.",
                    },
                },
                "required": ["date_from", "date_to"],
            },
        },
        {
            "name": "run_incrementality_analysis",
            "description": (
                "Run a Bayesian incrementality analysis on a geo holdout or conversion lift experiment. "
                "Computes incremental lift (iROAS) by comparing test vs. control group performance, "
                "applies a log-normal Bayesian posterior update to convert the experimental result into "
                "Meridian-compatible prior parameters (roi_prior_mu / roi_prior_sigma), and writes "
                "the result to incrementality_lift_results in BigQuery. "
                "When mark_active=True (default), the result auto-injects into the next run_mmm_model "
                "call via v_incrementality_roi_priors — no manual roi_priors dict construction needed. "
                "Also enables the CRM domain suppression workflow: once incrementality_lift_results "
                "is populated, push_domain_suppression() in both tiktok_ads_client and "
                "google_ads_client auto-fetches CRM emails via crm_client.get_crm_emails_by_domain(). "
                "Supported methodologies: geo_holdout (geographic split), conversion_lift (time-based "
                "pre/post comparison), brand_lift (manual platform study input)."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "experiment_id": {
                        "type": "string",
                        "description": (
                            "Unique experiment identifier. Use a descriptive slug, e.g. "
                            "'google_ads_geo_holdout_2026_q2'. Created automatically if new."
                        ),
                    },
                    "channel": {
                        "type": "string",
                        "description": (
                            "Platform channel name matching the platform field in platform_daily_spend. "
                            "e.g. 'google_ads', 'meta', 'tiktok', 'linkedin'."
                        ),
                    },
                    "methodology": {
                        "type": "string",
                        "enum": ["geo_holdout", "conversion_lift", "brand_lift", "synthetic_control"],
                        "description": (
                            "geo_holdout: geographic market split (gold standard). "
                            "conversion_lift: pre/post time-based comparison. "
                            "brand_lift: survey-based; provide kpi_test/kpi_control manually. "
                            "synthetic_control: DID approach; provide kpi inputs manually."
                        ),
                    },
                    "test_date_from": {
                        "type": "string",
                        "description": "Start of the treatment period. Format: YYYY-MM-DD.",
                    },
                    "test_date_to": {
                        "type": "string",
                        "description": "End of the treatment period. Format: YYYY-MM-DD.",
                    },
                    "control_date_from": {
                        "type": "string",
                        "description": (
                            "Start of the control/comparison period. "
                            "geo_holdout: leave blank (uses same dates as test period). "
                            "conversion_lift: pre-experiment baseline start date."
                        ),
                    },
                    "control_date_to": {
                        "type": "string",
                        "description": (
                            "End of the control/comparison period. "
                            "conversion_lift: pre-experiment baseline end date."
                        ),
                    },
                    "test_regions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "ISO country/region codes in the treatment group (geo_holdout only). "
                            "e.g. ['US', 'CA']. Must match geo_country_code in platform_daily_spend."
                        ),
                    },
                    "control_regions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "ISO country/region codes in the control group (geo_holdout only). "
                            "e.g. ['GB', 'AU']. Should have similar baseline CVR to test_regions."
                        ),
                    },
                    "avg_conversion_value": {
                        "type": "number",
                        "description": (
                            "Average dollar value per conversion. Used to compute iROAS. "
                            "Use average deal value for B2B (e.g. 25000) or "
                            "average order value for e-commerce (e.g. 120). Default: 1.0."
                        ),
                    },
                    "kpi_test": {
                        "type": "number",
                        "description": (
                            "Total KPI (conversions) in test group. "
                            "Provide directly for brand_lift / synthetic_control, "
                            "or omit to let the tool query from platform_daily_spend."
                        ),
                    },
                    "kpi_control": {
                        "type": "number",
                        "description": "Total KPI (conversions) in control group. See kpi_test.",
                    },
                    "exposed_test": {
                        "type": "number",
                        "description": (
                            "Total impressions in test group. Used as exposure denominator "
                            "for conversion rate calculation. Omit to fetch from BQ."
                        ),
                    },
                    "exposed_control": {
                        "type": "number",
                        "description": "Total impressions in control group. Omit to fetch from BQ.",
                    },
                    "spend_test_input": {
                        "type": "number",
                        "description": "Total spend (USD) in test group. Omit to fetch from BQ.",
                    },
                    "spend_control_input": {
                        "type": "number",
                        "description": "Total spend (USD) in control/baseline group. Omit to fetch from BQ.",
                    },
                    "confidence_level": {
                        "type": "number",
                        "description": "Statistical confidence level. Default 0.90 (90% CI). Use 0.95 for high-stakes decisions.",
                    },
                    "mark_active": {
                        "type": "boolean",
                        "description": (
                            "If True (default), set is_active=TRUE on this result, "
                            "enabling auto-injection into the next run_mmm_model call. "
                            "Set False to store without activating (e.g. for data quality review)."
                        ),
                    },
                    "notes": {
                        "type": "string",
                        "description": "Optional analyst notes about design decisions, caveats, or data quality issues.",
                    },
                },
                "required": ["experiment_id", "channel", "methodology", "test_date_from", "test_date_to"],
            },
        },
        {
            "name": "analyze_marketing_intervention",
            "description": (
                "Run a Bayesian Structural Time Series (BSTS) causal impact analysis to "
                "quantify the retrospective effect of an unplanned marketing event. "
                "Fetches daily target metric + control series from platform_daily_spend, "
                "fits a LocalLinearTrend + Seasonal(7) + LinearRegression model on the "
                "pre-intervention baseline via JAX-backed HMC (tfp.substrates.jax.sts), "
                "projects the counterfactual for the post-intervention window, and computes: "
                "absolute_effect, relative_effect_pct, and posterior_tail_probability "
                "(Bayesian p-value analog). "
                "Returns a structured result dict AND a formatted Markdown summary table "
                "suitable for direct agent response. "
                "Writes results to causal_impact_runs and causal_impact_metrics in BigQuery. "
                "Use cases: spend halts, creative rotation changes, influencer spikes, "
                "platform outages, geo-level A/B tests with retrospective measurement. "
                "Requires minimum 4:1 pre:post observation ratio. "
                "Requires 'paid-media-agent[causal]' optional dependencies."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "target_metric": {
                        "type": "string",
                        "enum": ["conversions", "impressions", "spend", "clicks"],
                        "description": (
                            "The metric to analyze. Maps to platform_daily_spend columns: "
                            "conversions → platform_conversions, "
                            "impressions → impressions, "
                            "spend → spend (NUMERIC), "
                            "clicks → clicks."
                        ),
                    },
                    "target_channel": {
                        "type": "string",
                        "description": (
                            "Platform to analyze, e.g. 'google_ads', 'meta', 'tiktok'. "
                            "Must match the platform field in platform_daily_spend. "
                            "Omit to analyze the aggregate across all channels."
                        ),
                    },
                    "target_geo": {
                        "type": "string",
                        "description": (
                            "ISO country code filter (e.g. 'US', 'GB'). "
                            "Omit to include all geos. "
                            "Must match geo_country_code in platform_daily_spend."
                        ),
                    },
                    "pre_period_from": {
                        "type": "string",
                        "description": (
                            "Start of pre-intervention baseline window. Format: YYYY-MM-DD. "
                            "Should be at least 4× the post-period length for stable BSTS inference."
                        ),
                    },
                    "pre_period_to": {
                        "type": "string",
                        "description": (
                            "End of pre-intervention baseline (day BEFORE the event). "
                            "Format: YYYY-MM-DD."
                        ),
                    },
                    "post_period_from": {
                        "type": "string",
                        "description": "Start of the intervention / event window. Format: YYYY-MM-DD.",
                    },
                    "post_period_to": {
                        "type": "string",
                        "description": "End of the intervention / event window. Format: YYYY-MM-DD.",
                    },
                    "control_channels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional list of platform names to use as control covariates "
                            "(e.g. ['meta', 'linkedin'] when analyzing a Google Ads outage). "
                            "Control channels must NOT have been affected by the same event. "
                            "Including strong controls tightens the counterfactual CI significantly."
                        ),
                    },
                    "intervention_description": {
                        "type": "string",
                        "description": (
                            "Human-readable description of the marketing event being analyzed, "
                            "e.g. 'Google Ads paused for 12 days due to billing issue', "
                            "'Meta creative rotation — UGC vs. studio assets'."
                        ),
                    },
                    "n_draws": {
                        "type": "integer",
                        "description": (
                            "HMC posterior draws per chain. Default 200. "
                            "Reduce to 100 for quick checks; increase to 500 for final analysis. "
                            "Runtime: ~1–5 minutes on 4-vCPU Cloud Run for typical series length."
                        ),
                    },
                    "analyst_notes": {
                        "type": "string",
                        "description": "Optional notes about design decisions, data quality, or interpretation caveats.",
                    },
                },
                "required": [
                    "target_metric", "pre_period_from", "pre_period_to",
                    "post_period_from", "post_period_to",
                ],
            },
        },
        {
            "name": "complete_attribution_run",
            "description": "Mark an attribution run as completed or failed in attribution_runs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":                  {"type": "string"},
                    "status":                  {"type": "string", "enum": ["completed", "failed"]},
                    "paths_modeled":           {"type": "integer"},
                    "conversions_attributed":  {"type": "integer"},
                    "identity_match_rate":     {"type": "number"},
                    "avg_path_length":         {"type": "number"},
                    "error_message":           {"type": "string"},
                },
                "required": ["run_id", "status"],
            },
        },
        {
            "name": "fetch_market_momentum_signals",
            "description": (
                "Ingest external trend and social signal data for a set of target keywords. "
                "Queries Google Trends (relative search interest, 0–100 normalised index) "
                "and Reddit (post frequency + engagement scoring via PRAW) for each keyword, "
                "writes clean rows to BigQuery (social_trend_signals + social_mentions_staging), "
                "and computes Month-over-Month signal velocity (current vs. prior window). "
                "Returns a structured dict of metric arrays for backend consumption and a "
                "Markdown table summarising the highest-velocity search terms and active topics "
                "for direct presentation to the user. "
                "Requires pip install 'paid-media-agent[social]' and REDDIT_CLIENT_ID / "
                "REDDIT_CLIENT_SECRET environment variables for Reddit ingestion."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Target keyword or phrase strings to track. "
                            "1–20 keywords supported. Each keyword is queried against "
                            "Google Trends and all specified subreddits. "
                            "Example: ['B2B marketing automation', 'intent data', 'CDP vs DMP']"
                        ),
                    },
                    "subreddits": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Target subreddit names (without r/ prefix) to search. "
                            "If omitted, uses a curated default list of business subreddits: "
                            "marketing, SEO, PPC, sales, analytics, SaaS, entrepreneur, startups. "
                            "Example: ['marketing', 'analytics', 'SEO']"
                        ),
                    },
                    "lookback_days": {
                        "type": "integer",
                        "default": 30,
                        "description": (
                            "Length of the current signal window in days (default 30). "
                            "The prior window (for MoM velocity) covers the equivalent period "
                            "immediately preceding this window. "
                            "Longer windows (90 days) provide more stable velocity estimates; "
                            "shorter windows (14 days) are more responsive to recent spikes."
                        ),
                    },
                    "geo_code": {
                        "type": "string",
                        "default": "US",
                        "description": (
                            "ISO 3166-1 alpha-2 country code for Google Trends geo filter. "
                            "Examples: 'US', 'GB', 'DE', 'CA'. Empty string = worldwide."
                        ),
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["google_trends", "reddit"]},
                        "description": (
                            "Which data sources to ingest. Defaults to both. "
                            "Specify ['google_trends'] to skip Reddit (useful when PRAW "
                            "credentials are not yet configured)."
                        ),
                    },
                },
                "required": ["keywords"],
            },
        },
        {
            "name": "ingest_and_analyze_market_signals",
            "description": (
                "Ingest competitor web content and extract a structured AI-inferred "
                "strategic messaging profile via the Market Signals Engine (Task 36). "
                "\n\n"
                "Pipeline:\n"
                "  1. Fetch raw text from each source URL (landing pages, press releases, "
                "blog posts) via httpx. Also accepts 'text://<payload>' scheme for direct "
                "text input without HTTP fetch.\n"
                "  2. Normalize text through the Task 25 pipeline: strip HTML, tracking "
                "pixels, data URIs, emojis, and collapse whitespace.\n"
                "  3. Write normalized text blocks to market_signals_staging in BigQuery.\n"
                "  4. Pass combined text through a Claude inference call with the resolved "
                "evaluation prompt (private_market_intelligence.md if present, else "
                "public fallback) to extract: core value prop, positioning angle, "
                "target audience, primary keywords, messaging pillars, CTA patterns, "
                "and counter-positioning hooks.\n"
                "  5. Write the inferred profile to competitor_messaging_vectors in BigQuery.\n"
                "  6. Log the run to market_signals_runs.\n"
                "\n"
                "Returns a dual payload:\n"
                "  backend_result  — run_id, vector, signal_count, prompt_source\n"
                "  markdown_summary — comparison matrix with competitor hooks, keyword "
                "frequencies, and a Counter-Positioning Action Guide\n"
                "  competitor_context_for_copy — pre-formatted context string for "
                "injection into Task 26 generate_creative_campaign_brief() calls\n"
                "\n"
                "Stealth Execution Gateway:\n"
                "  If agents/analyst/skills/private_market_intelligence.md exists locally, "
                "its contents are used as the master evaluation prompt (proprietary "
                "heuristics, custom scoring, market-specific terminology). If absent, "
                "falls back to a generic semantic extraction prompt. The private file is "
                "gitignored and never committed or persisted to BigQuery."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "source_urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of publicly accessible HTTP/HTTPS URLs to analyze. "
                            "Accepts landing pages, press releases, blog posts, and pricing pages. "
                            "Also supports 'text://<content>' for direct text injection "
                            "without an HTTP fetch. "
                            "Maximum 25 URLs per run. "
                            "Example: ['https://competitor.com', 'https://competitor.com/pricing']"
                        ),
                        "minItems": 1,
                        "maxItems": 25,
                    },
                    "competitor_name": {
                        "type": "string",
                        "description": (
                            "Canonical label for the competitor. Used as the primary key "
                            "in BigQuery for filtering vectors. "
                            "Optionally include domain in parens for auto-resolution: "
                            "'Acme Corp (acme.com)'. "
                            "Example: 'Salesforce', 'HubSpot (hubspot.com)'"
                        ),
                    },
                    "category": {
                        "type": "string",
                        "description": (
                            "Optional market category for scoping the inference prompt. "
                            "Helps the LLM apply domain-specific heuristics. "
                            "Examples: 'B2B SaaS', 'marketing automation', 'revenue intelligence', "
                            "'ecommerce', 'cybersecurity'. "
                            "Also used when retrieving previously analyzed competitors."
                        ),
                    },
                },
                "required": ["source_urls", "competitor_name"],
            },
        },
        {
            "name": "audit_data_attribution_cleanliness",
            "description": (
                "Run the Attribution Forensic Verification Engine to detect CRM data overwrites, "
                "tracking anomalies, and phantom conversions across the paid media stack (Task 37). "
                "\n\n"
                "Three forensic tests are executed sequentially:\n"
                "  1. Orphaned Token Test — scans crm_leads_staging for leads whose analytics "
                "session carried a paid click token (gclid, fbclid, msclkid, ttclid, li_fat_id) "
                "but whose CRM LeadSource is assigned to an offline label such as 'Content "
                "Syndication', 'SDR Cold Outreach', or 'Webinar Ingestion'. Flags each match "
                "as an attribution overwrite with 0.85 confidence.\n"
                "  2. Timestamp Divergence Test — where a lead modification timestamp is available "
                "(systemmodstamp or lead_source_updated_at), compares the click token capture "
                "time to the LeadSource assignment time. If the offline label was applied AFTER "
                "the click token was captured, classifies the event as an explicit overwrite. "
                "Confidence scales with the overwrite lag in hours. Skips gracefully if no "
                "modification timestamp column exists in crm_leads_staging.\n"
                "  3. Phantom Conversion Test — cross-references platform_daily_spend "
                "(platform-reported conversions per channel/geo/day) against conversion_events "
                "(CRM-matched conversions). Flags days where platform pixels claim significantly "
                "more conversions than the CRM holds within a configurable attribution window "
                "(default 7 days). Ordered by phantom gap magnitude.\n"
                "\n"
                "Dual payload:\n"
                "  backend — anomaly rows streamed to data_attribution_anomalies in BigQuery; "
                "summary dict with total anomaly count, at-risk pipeline value, and per-type "
                "breakdown; correction multipliers available via v_attribution_correction_weights\n"
                "  markdown_summary — audit brief with a 0–100 data cleanliness score, "
                "anomaly matrix by type and platform, overwrite source breakdown, and "
                "actionable CRM integration fix recommendations\n"
                "\n"
                "MMM calibration hook:\n"
                "  After running this tool, the next run_mmm_model call can pass "
                "apply_attribution_correction=True to the meridian_data_loader to automatically "
                "apply the per-channel/geo/week correction multipliers from "
                "v_attribution_correction_weights before MMM tensor packaging."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_days": {
                        "type": "integer",
                        "default": 90,
                        "description": (
                            "How far back to scan CRM and session data (default 90 days). "
                            "Longer windows surface more historical overwrites but increase "
                            "BigQuery query cost. 90 days is the recommended production default. "
                            "Use 30 days for lightweight weekly health checks."
                        ),
                    },
                    "attribution_window_days": {
                        "type": "integer",
                        "default": 7,
                        "description": (
                            "Grace window for the Phantom Conversion test (default 7 days). "
                            "Platform conversions may precede CRM record creation by up to "
                            "this many days due to data pipeline latency. Increase to 14 days "
                            "for B2B stacks with slow CRM sync cadences."
                        ),
                    },
                    "lead_source_offline_patterns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Override the default offline LeadSource label set. Provide the full "
                            "list of your org's offline and non-paid LeadSource values, e.g. "
                            "['Content Syndication', 'SDR Cold Outreach', 'Webinar Ingestion', "
                            "'Direct Mail', 'Trade Show']. "
                            "If omitted, uses the built-in default set of ~20 common offline labels."
                        ),
                    },
                },
                "required": [],
            },
        },
    ]

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_run_mmm_model(
        self,
        date_from: str,
        date_to: str,
        platforms: list[str] | None = None,
        geo_allowlist: list[str] | None = None,
        roi_priors: dict | None = None,
        n_draws: int = 500,
        n_chains: int = 4,
    ) -> dict:
        """
        Run the Meridian Bayesian MMM pipeline end-to-end.

        Delegates to tools/meridian_analyst_engine.run_mmm_pipeline() which handles
        data extraction (Component 1), model construction, MCMC sampling, artifact
        persistence, and BigQuery write-back (Component 2).

        The roi_priors parameter is the Task 22 calibration hook. When
        incrementality_lift_results is populated by Task 22, the Analyst agent should
        read that table first, then pass the results here as roi_priors to ground the
        MMM posteriors in experimentally measured lift:

            lift_rows = bq.run_query("SELECT channel, iROAS_mean, iROAS_std, experiment_id
                                      FROM incrementality_lift_results
                                      WHERE is_active = TRUE")
            roi_priors = {
                row["channel"]: {
                    "mu":     row["iROAS_mean"],
                    "sigma":  row["iROAS_std"],
                    "source": row["experiment_id"],
                }
                for row in lift_rows
            }
        """
        try:
            from tools.meridian_analyst_engine import run_mmm_pipeline
        except ImportError as exc:
            return {
                "status": "dependency_missing",
                "error": str(exc),
                "fix": "Install MMM dependencies: pip install 'paid-media-agent[mmm]'",
                "note": (
                    "The MMM engine requires google-meridian, jax[cpu], numpyro, numpy, "
                    "pandas, and pyarrow. These are not included in the core install to "
                    "keep the base agent lightweight."
                ),
            }

        try:
            result = run_mmm_pipeline(
                date_from=date_from,
                date_to=date_to,
                platforms=platforms,
                geo_allowlist=geo_allowlist,
                roi_priors=roi_priors,
                n_draws=n_draws,
                n_chains=n_chains,
                write_to_bq=True,
            )
            return {
                "status": "completed",
                "run_id": result.get("run_id"),
                "r_hat_max": result.get("r_hat_max"),
                "n_divergences": result.get("n_divergences"),
                "elapsed_seconds": result.get("elapsed_seconds"),
                "roi_summary": result.get("roi_summary", {}),
                "data_shape": result.get("data_shape", {}),
                "artifact_path": result.get("artifact_path"),
                "bq_tables_written": ["mmm_runs", "mmm_channel_contributions"],
                "convergence_note": (
                    "Converged (R-hat < 1.1)."
                    if (result.get("r_hat_max") or 99.0) < 1.1
                    else f"WARNING: R-hat={result.get('r_hat_max')} ≥ 1.1. "
                         "Increase n_draws or n_adapt for better convergence."
                ),
            }
        except ValueError as exc:
            return {
                "status": "data_insufficient",
                "error": str(exc),
                "fix": (
                    "Common causes: (1) geo_country_code not populated — run migration 001, "
                    "(2) date range too short — use at least 78 weeks, "
                    "(3) too few geos — lower min_weekly_impressions in load_meridian_data()."
                ),
            }
        except Exception as exc:
            log.error("analyst.mmm_failed", error=str(exc))
            return {
                "status": "failed",
                "error": str(exc),
                "note": "Check Cloud Logging for the full traceback. Common issues: JAX OOM (reduce n_draws or geos), timeout (reduce n_draws to 250).",
            }

    def _tool_enrich_sessions(
        self,
        lookback_hours: int | None = None,
        batch_size: int | None = None,
    ) -> dict:
        """
        Resolve anonymous sessions to company domains and write account analytics records.
        Delegates to EnrichmentJob which handles IP intelligence + all DB writes.
        Raw IP addresses are processed entirely within EnrichmentJob.run() and never
        stored, logged, or returned from this method.
        """
        from agents.analyst.enrichment import EnrichmentJob
        job = EnrichmentJob()
        return job.run(
            lookback_hours=lookback_hours,
            batch_size=batch_size,
        )

    def _tool_run_bigquery_query(self, sql: str, params: dict | None = None) -> dict:
        rows = bq.run_query(sql, params)
        return {"rows": rows[:200], "total_rows": len(rows)}

    def _tool_start_attribution_run(
        self,
        model_name: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
    ) -> dict:
        run_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()
        model_config = {
            "full_path":      {"first_touch_pct": 0.30, "last_touch_pct": 0.30},
            "first_touch":    {},
            "last_touch":     {},
            "linear":         {},
            "time_decay":     {"half_life_days": 7},
            "position_based": {"first_touch_pct": 0.40, "last_touch_pct": 0.40},
        }.get(model_name, {})

        row = {
            "run_id":             run_id,
            "run_type":           "scheduled",
            "model_name":         model_name,
            "model_version":      "1.0.0",
            "model_config":       str(model_config),
            "period_start":       period_start,
            "period_end":         period_end,
            "conversion_types":   conversion_types or [],
            "entity_types":       ["person", "account"],
            "paths_modeled":      None,
            "conversions_attributed": None,
            "total_credit":       None,
            "total_conversion_value": None,
            "data_quality_score": None,
            "identity_match_rate": None,
            "avg_path_length":    None,
            "started_at":         now,
            "completed_at":       None,
            "duration_seconds":   None,
            "status":             "running",
            "error_message":      None,
            "triggered_by":       "analyst_agent",
        }
        errors = bq.insert_rows("attribution_runs", [row])
        if errors:
            log.warning("analyst.run_insert_error", errors=errors)
        log.info("analyst.run_started", run_id=run_id, model=model_name, period=f"{period_start}→{period_end}")
        return {"run_id": run_id, "model_name": model_name, "period_start": period_start, "period_end": period_end}

    def _tool_stitch_identities(self, lookback_days: int = 1) -> dict:
        """
        Identity stitching using the paid-media-schema tables.
        Phase 1: domain matching (sessions → CRM leads → entity)
        Phase 2: click ID co-occurrence (sessions with matching click IDs → same entity)
        Writes to identity_entities and identity_entity_signals.
        """
        now = datetime.now(timezone.utc).isoformat()

        # ── Phase 1: Email-domain → account stitching ─────────────────────────
        # Match sessions to CRM leads via email domain, then roll up to entity.
        domain_sql = f"""
        MERGE {bq.table_ref('identity_entity_signals')} AS target
        USING (
            SELECT
                COALESCE(e.entity_id, GENERATE_UUID())   AS entity_id,
                'analytics_cookie.google.ga4_client_id'  AS namespace_id,
                s.ga4_client_id                          AS identifier_value,
                'probabilistic'                          AS match_method,
                0.65                                     AS confidence_score,
                MIN(s.session_start_at)                  AS first_observed_at,
                MAX(s.session_start_at)                  AS last_observed_at,
                COUNT(*)                                 AS observation_count
            FROM {bq.table_ref('sessions')} s
            JOIN {bq.table_ref('crm_leads_staging')} l
              ON SPLIT(l.email, '@')[SAFE_OFFSET(1)] = s.entry_referrer  -- domain match
                 OR s.ga4_client_id = l.ga_client_id                     -- direct match
            LEFT JOIN {bq.table_ref('identity_entity_signals')} e
              ON e.namespace_id = 'crm_id.salesforce.account_id'
             AND e.identifier_value = l.account_id
            WHERE DATE(s.session_start_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
              AND s.ga4_client_id IS NOT NULL
              AND l.account_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        ) AS source
        ON target.namespace_id = source.namespace_id
           AND target.identifier_value = source.identifier_value
        WHEN MATCHED THEN UPDATE SET
            last_observed_at  = source.last_observed_at,
            observation_count = target.observation_count + source.observation_count
        WHEN NOT MATCHED THEN INSERT (
            entity_id, namespace_id, identifier_value, match_method,
            confidence_score, stitched_by, stitched_at,
            first_observed_at, last_observed_at, observation_count, is_active
        ) VALUES (
            source.entity_id, source.namespace_id, source.identifier_value,
            source.match_method, source.confidence_score, 'analyst_agent', '{now}',
            source.first_observed_at, source.last_observed_at, source.observation_count, TRUE
        )
        """
        domain_rows = bq.run_dml(domain_sql)

        # ── Phase 2: Click ID co-occurrence ───────────────────────────────────
        # Sessions sharing the same click ID in the same conversion path → same entity.
        # For each click ID present in sessions, ensure entity_signals has an entry.
        click_id_namespaces = [
            ("gclid",     "platform_click_id.google.gclid",       0.95),
            ("dclid",     "platform_click_id.google.dclid",       0.95),
            ("fbclid",    "platform_click_id.meta.fbclid",        0.90),
            ("li_fat_id", "platform_click_id.linkedin.li_fat_id", 0.92),
            ("ttclid",    "platform_click_id.tiktok.ttclid",      0.90),
            ("msclkid",   "platform_click_id.microsoft.msclkid",  0.95),
        ]
        click_rows = 0
        for col, ns_id, conf in click_id_namespaces:
            sql = f"""
            MERGE {bq.table_ref('identity_entity_signals')} AS target
            USING (
                SELECT
                    COALESCE(existing.entity_id, GENERATE_UUID()) AS entity_id,
                    '{ns_id}'   AS namespace_id,
                    s.{col}     AS identifier_value,
                    'deterministic' AS match_method,
                    {conf}      AS confidence_score,
                    MIN(s.session_start_at) AS first_observed_at,
                    MAX(s.session_start_at) AS last_observed_at,
                    COUNT(*)    AS observation_count
                FROM {bq.table_ref('sessions')} s
                LEFT JOIN {bq.table_ref('identity_entity_signals')} existing
                  ON existing.namespace_id = '{ns_id}'
                 AND existing.identifier_value = s.{col}
                WHERE DATE(s.session_start_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                  AND s.{col} IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5
            ) AS source
            ON target.namespace_id = source.namespace_id
               AND target.identifier_value = source.identifier_value
            WHEN MATCHED THEN UPDATE SET
                last_observed_at  = source.last_observed_at,
                observation_count = target.observation_count + source.observation_count
            WHEN NOT MATCHED THEN INSERT (
                entity_id, namespace_id, identifier_value, match_method,
                confidence_score, stitched_by, stitched_at,
                first_observed_at, last_observed_at, observation_count, is_active
            ) VALUES (
                source.entity_id, source.namespace_id, source.identifier_value,
                source.match_method, source.confidence_score, 'analyst_agent', '{now}',
                source.first_observed_at, source.last_observed_at, source.observation_count, TRUE
            )
            """
            try:
                click_rows += bq.run_dml(sql)
            except Exception as exc:
                log.warning("analyst.stitch_click_id_error", namespace=ns_id, error=str(exc))

        # ── Phase 3: Sync entity table ─────────────────────────────────────────
        # Ensure identity_entities has a row for every entity referenced in signals.
        entity_sync_sql = f"""
        MERGE {bq.table_ref('identity_entities')} AS target
        USING (
            SELECT
                entity_id,
                'account' AS entity_type,
                COUNT(*)  AS signal_count,
                COUNTIF(match_method = 'deterministic') AS deterministic_signal_count,
                MIN(first_observed_at) AS first_seen_at,
                MAX(last_observed_at)  AS last_seen_at
            FROM {bq.table_ref('identity_entity_signals')}
            WHERE is_active = TRUE
            GROUP BY entity_id
        ) AS source
        ON target.entity_id = source.entity_id
        WHEN MATCHED THEN UPDATE SET
            signal_count = source.signal_count,
            deterministic_signal_count = source.deterministic_signal_count,
            last_seen_at = source.last_seen_at,
            confidence_tier = CASE
                WHEN source.deterministic_signal_count >= 1 THEN 'high'
                WHEN source.signal_count >= 2 THEN 'medium'
                ELSE 'low'
            END,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            entity_id, entity_type, signal_count, deterministic_signal_count,
            confidence_tier, first_seen_at, last_seen_at, is_active, created_at, updated_at
        ) VALUES (
            source.entity_id, source.entity_type, source.signal_count,
            source.deterministic_signal_count,
            CASE WHEN source.deterministic_signal_count >= 1 THEN 'high'
                 WHEN source.signal_count >= 2 THEN 'medium' ELSE 'low' END,
            source.first_seen_at, source.last_seen_at, TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """
        entity_rows = bq.run_dml(entity_sync_sql)

        log.info("analyst.stitch_complete", domain_rows=domain_rows, click_rows=click_rows, entity_rows=entity_rows)
        return {
            "domain_stitched":  domain_rows,
            "click_id_stitched": click_rows,
            "entities_synced":  entity_rows,
            "lookback_days":    lookback_days,
        }

    def _tool_run_mta_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        model_config: dict | None = None,
    ) -> dict:
        cfg = model_config or {}
        first_pct = float(cfg.get("first_touch_pct", 0.30))
        last_pct  = float(cfg.get("last_touch_pct", 0.30))
        mid_pct   = round(1.0 - first_pct - last_pct, 4)

        now = datetime.now(timezone.utc).isoformat()

        # Delete any stale results for this run (idempotent re-runs)
        bq.run_dml(f"""
            DELETE FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
        """)

        # Compute and insert Full-Path weighted credit
        insert_sql = f"""
        INSERT INTO {bq.table_ref('attribution_results')} (
            result_id, run_id, path_id, touchpoint_id, conversion_id, entity_id,
            conversion_date, touchpoint_date, platform, channel, campaign_id,
            touchpoint_type, path_position, path_total_touches,
            conversion_type, conversion_value, deal_value,
            credit_weight, credit_conversions, credit_value, credit_deal_value,
            model_name, period_start, period_end, created_at
        )
        WITH paths AS (
            SELECT
                ies.entity_id,
                t.touchpoint_id,
                t.touchpoint_at,
                t.platform,
                t.channel,
                t.campaign_id,
                t.touchpoint_type,
                c.conversion_id,
                c.converted_at,
                c.conversion_type,
                c.conversion_value,
                c.deal_value,
                ROW_NUMBER() OVER (
                    PARTITION BY ies.entity_id, c.conversion_id
                    ORDER BY t.touchpoint_at ASC
                ) AS touch_seq,
                COUNT(*) OVER (
                    PARTITION BY ies.entity_id, c.conversion_id
                ) AS total_touches,
                -- Stable path_id per entity+conversion
                TO_HEX(MD5(CONCAT(ies.entity_id, c.conversion_id))) AS path_id
            FROM {bq.table_ref('touchpoint_events')} t
            JOIN {bq.table_ref('identity_entity_signals')} ies
              ON ies.namespace_id = 'analytics_cookie.google.ga4_client_id'
             AND ies.identifier_value = t.session_id
            JOIN {bq.table_ref('conversion_events')} c
              ON c.entity_id = ies.entity_id
             AND c.converted_at > t.touchpoint_at
             AND DATE(c.converted_at) BETWEEN '{period_start}' AND '{period_end}'
            WHERE DATE(t.touchpoint_at) BETWEEN
                  DATE_SUB(DATE '{period_start}', INTERVAL 90 DAY)
                  AND '{period_end}'
        )
        SELECT
            GENERATE_UUID()                         AS result_id,
            '{run_id}'                              AS run_id,
            path_id,
            touchpoint_id,
            conversion_id,
            entity_id,
            DATE(converted_at)                      AS conversion_date,
            DATE(touchpoint_at)                     AS touchpoint_date,
            platform,
            channel,
            campaign_id,
            touchpoint_type,
            touch_seq                               AS path_position,
            total_touches                           AS path_total_touches,
            conversion_type,
            COALESCE(conversion_value, 0)           AS conversion_value,
            COALESCE(deal_value, 0)                 AS deal_value,
            CASE
                WHEN total_touches = 1 THEN 1.0
                WHEN touch_seq = 1           THEN {first_pct}
                WHEN touch_seq = total_touches THEN {last_pct}
                ELSE {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_weight,
            CASE
                WHEN total_touches = 1 THEN 1.0
                WHEN touch_seq = 1           THEN {first_pct}
                WHEN touch_seq = total_touches THEN {last_pct}
                ELSE {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_conversions,
            CASE
                WHEN total_touches = 1 THEN COALESCE(conversion_value, 0)
                WHEN touch_seq = 1           THEN COALESCE(conversion_value, 0) * {first_pct}
                WHEN touch_seq = total_touches THEN COALESCE(conversion_value, 0) * {last_pct}
                ELSE COALESCE(conversion_value, 0) * {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_value,
            CASE
                WHEN total_touches = 1 THEN COALESCE(deal_value, 0)
                WHEN touch_seq = 1           THEN COALESCE(deal_value, 0) * {first_pct}
                WHEN touch_seq = total_touches THEN COALESCE(deal_value, 0) * {last_pct}
                ELSE COALESCE(deal_value, 0) * {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_deal_value,
            'full_path'                             AS model_name,
            DATE '{period_start}'                   AS period_start,
            DATE '{period_end}'                     AS period_end,
            TIMESTAMP '{now}'                       AS created_at
        FROM paths
        """
        rows_inserted = bq.run_dml(insert_sql)

        # Quick summary stats
        stats = bq.run_query(f"""
            SELECT
                COUNT(DISTINCT path_id)     AS paths,
                COUNT(DISTINCT conversion_id) AS conversions,
                ROUND(SUM(credit_conversions), 1) AS total_credit
            FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
        """)

        log.info("analyst.mta_complete", run_id=run_id, rows=rows_inserted)
        return {
            "run_id":          run_id,
            "rows_inserted":   rows_inserted,
            "paths_modeled":   stats[0].get("paths", 0) if stats else 0,
            "conversions":     stats[0].get("conversions", 0) if stats else 0,
            "total_credit":    stats[0].get("total_credit", 0) if stats else 0,
        }

    def _tool_build_channel_summary(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        model_name: str,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()

        bq.run_dml(f"""
            DELETE FROM {bq.table_ref('attribution_channel_summary')}
            WHERE run_id = '{run_id}'
        """)

        insert_sql = f"""
        INSERT INTO {bq.table_ref('attribution_channel_summary')} (
            summary_id, run_id, model_name, period_start, period_end,
            platform, channel, conversion_type,
            total_touches, unique_entities,
            first_touch_count, last_touch_count,
            attributed_conversions, attributed_value, attributed_deal_value, credit_share_pct,
            total_spend,
            attributed_cpa, attributed_roas,
            platform_conversions, platform_cpa,
            attribution_vs_platform_delta_pct,
            generated_at
        )
        WITH base AS (
            SELECT
                platform, channel, conversion_type,
                COUNT(*)                        AS total_touches,
                COUNT(DISTINCT entity_id)       AS unique_entities,
                COUNTIF(path_position = 1)      AS first_touch_count,
                COUNTIF(path_position = path_total_touches) AS last_touch_count,
                SUM(credit_conversions)         AS attributed_conversions,
                SUM(credit_value)               AS attributed_value,
                SUM(credit_deal_value)          AS attributed_deal_value,
                SUM(SUM(credit_conversions)) OVER () AS grand_total_credit
            FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
            GROUP BY platform, channel, conversion_type
        ),
        with_spend AS (
            SELECT
                b.*,
                COALESCE(SUM(s.spend), 0) AS total_spend,
                COALESCE(SUM(s.platform_conversions), 0) AS platform_conversions
            FROM base b
            LEFT JOIN {bq.table_ref('platform_daily_spend')} s
              ON s.platform = b.platform
             AND s.date BETWEEN '{period_start}' AND '{period_end}'
            GROUP BY
                b.platform, b.channel, b.conversion_type,
                b.total_touches, b.unique_entities,
                b.first_touch_count, b.last_touch_count,
                b.attributed_conversions, b.attributed_value,
                b.attributed_deal_value, b.grand_total_credit
        )
        SELECT
            GENERATE_UUID()     AS summary_id,
            '{run_id}'          AS run_id,
            '{model_name}'      AS model_name,
            DATE '{period_start}' AS period_start,
            DATE '{period_end}'   AS period_end,
            platform, channel, conversion_type,
            total_touches, unique_entities,
            first_touch_count, last_touch_count,
            attributed_conversions,
            attributed_value,
            attributed_deal_value,
            SAFE_DIVIDE(attributed_conversions, grand_total_credit) * 100 AS credit_share_pct,
            total_spend,
            SAFE_DIVIDE(total_spend, NULLIF(attributed_conversions, 0)) AS attributed_cpa,
            SAFE_DIVIDE(attributed_value, NULLIF(total_spend, 0))       AS attributed_roas,
            platform_conversions,
            SAFE_DIVIDE(total_spend, NULLIF(platform_conversions, 0))   AS platform_cpa,
            SAFE_DIVIDE(attributed_conversions - platform_conversions,
                        NULLIF(platform_conversions, 0)) * 100          AS attribution_vs_platform_delta_pct,
            TIMESTAMP '{now}'   AS generated_at
        FROM with_spend
        """
        rows = bq.run_dml(insert_sql)

        top = bq.run_query(f"""
            SELECT channel, platform, attributed_conversions, credit_share_pct,
                   attributed_cpa, attributed_roas
            FROM {bq.table_ref('attribution_channel_summary')}
            WHERE run_id = '{run_id}'
            ORDER BY attributed_conversions DESC
            LIMIT 10
        """)

        log.info("analyst.summary_built", run_id=run_id, rows=rows)
        return {"rows_written": rows, "top_channels": top}

    def _tool_write_analyst_insight(
        self,
        run_id: str,
        insight_type: str,
        headline: str,
        priority: str,
        detail: str | None = None,
        recommendation: str | None = None,
        estimated_impact: str | None = None,
        period_start: str | None = None,
        period_end: str | None = None,
        data_points: dict | None = None,
    ) -> dict:
        insight_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "insight_id":        insight_id,
            "run_id":            run_id,
            "insight_type":      insight_type,
            "period_start":      period_start,
            "period_end":        period_end,
            "affected_platform": None,
            "affected_channel":  None,
            "headline":          headline,
            "detail":            detail,
            "data_points":       str(data_points) if data_points else None,
            "confidence":        "high",
            "has_recommendation": recommendation is not None,
            "recommendation":    recommendation,
            "estimated_impact":  estimated_impact,
            "priority":          priority,
            "status":            "new",
            "reviewed_at":       None,
            "actioned_by":       None,
            "generated_at":      now,
        }
        errors = bq.insert_rows("analyst_insights", [row])
        log.info("analyst.insight_written", insight_id=insight_id, type=insight_type)
        return {"insight_id": insight_id, "errors": len(errors)}

    def _tool_run_shapley_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
        max_channels: int | None = None,
        max_paths: int | None = None,
    ) -> dict:
        from tools.attribution_models import load_paths, compute_shapley, write_model_results
        max_ch  = max_channels or settings.shapley_max_channels
        max_p   = max_paths    or settings.shapley_max_paths

        log.info("analyst.shapley_start", run_id=run_id, period=f"{period_start}→{period_end}")
        paths = load_paths(period_start, period_end, conversion_types, max_p)
        if not paths:
            return {"error": "No conversion paths found for the specified period and conversion types."}

        weights = compute_shapley(paths, max_channels=max_ch)
        result  = write_model_results(run_id, "shapley_value", period_start, period_end, paths, weights)

        log.info("analyst.shapley_complete", run_id=run_id, paths=len(paths))
        return result

    def _tool_run_markov_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
        max_paths: int | None = None,
    ) -> dict:
        from tools.attribution_models import load_paths, compute_markov, write_model_results
        max_p = max_paths or settings.shapley_max_paths

        log.info("analyst.markov_start", run_id=run_id, period=f"{period_start}→{period_end}")
        paths = load_paths(period_start, period_end, conversion_types, max_p)
        if not paths:
            return {"error": "No conversion paths found for the specified period and conversion types."}

        weights = compute_markov(paths)
        result  = write_model_results(run_id, "markov_chain", period_start, period_end, paths, weights)

        log.info("analyst.markov_complete", run_id=run_id, paths=len(paths))
        return result

    def _tool_run_incrementality_analysis(  # noqa: C901  (complex but deliberately structured)
        self,
        experiment_id: str,
        channel: str,
        methodology: str,
        test_date_from: str,
        test_date_to: str,
        control_date_from: str | None = None,
        control_date_to: str | None = None,
        test_regions: list[str] | None = None,
        control_regions: list[str] | None = None,
        avg_conversion_value: float = 1.0,
        kpi_test: float | None = None,
        kpi_control: float | None = None,
        exposed_test: float | None = None,
        exposed_control: float | None = None,
        spend_test_input: float | None = None,
        spend_control_input: float | None = None,
        confidence_level: float = 0.90,
        mark_active: bool = True,
        notes: str | None = None,
    ) -> dict:
        """
        Bayesian incrementality analysis engine (Task 22).

        Statistical methodology:
          1. Fetch test / control group data from platform_daily_spend (or use manual inputs).
          2. Compute conversion rates (CVR = conversions / impressions).
          3. Two-proportion z-test for statistical significance (one-tailed, H1: test > control).
          4. Point estimate of iROAS = incremental_conversions × avg_value / incremental_spend.
          5. 90% CI on iROAS via delta method on the CVR difference.
          6. Log-normal Bayesian posterior update:
               Prior:       log(ROI) ~ N(mu₀=0.2, σ₀=0.9)    [weak observational default]
               Likelihood:  log(iROAS_est) with SE from CI width
               Posterior:   precision-weighted Gaussian average
               → roi_prior_mu / roi_prior_sigma  for Meridian ModelSpec

        Meridian calibration hook (Task 27):
          When mark_active=True and the result is statistically significant,
          this result populates v_incrementality_roi_priors, which
          run_mmm_pipeline() reads automatically — closing the Task 22 → Task 27
          calibration loop without any manual dict construction.
        """
        valid_methods = {"geo_holdout", "conversion_lift", "brand_lift", "synthetic_control"}
        if methodology not in valid_methods:
            return {"error": f"Invalid methodology: {methodology!r}. Must be one of: {valid_methods}"}

        # Sanitize channel for SQL (should be a simple platform identifier)
        safe_channel = channel.replace("'", "''")
        z_alpha = _z_for_confidence(confidence_level)
        now_str = datetime.now(timezone.utc).isoformat()

        # ── 1. Fetch test group data ───────────────────────────────────────────
        test_days = 1
        if kpi_test is None or exposed_test is None:
            test_geo_clause = ""
            if test_regions:
                region_list = ", ".join(
                    f"'{r.replace(chr(39), chr(39)*2)}'" for r in test_regions
                )
                test_geo_clause = f"AND geo_country_code IN ({region_list})"

            test_sql = f"""
            SELECT
                SUM(CAST(spend AS FLOAT64))       AS total_spend,
                SUM(impressions)                   AS total_impressions,
                SUM(platform_conversions)          AS total_conversions,
                COUNT(DISTINCT date)               AS n_days
            FROM {bq.table_ref('platform_daily_spend')}
            WHERE date BETWEEN '{test_date_from}' AND '{test_date_to}'
              AND platform = '{safe_channel}'
              {test_geo_clause}
            """
            try:
                test_rows = bq.run_query(test_sql)
            except Exception as exc:
                return {"error": f"Failed to fetch test group data from BQ: {exc}",
                        "hint": "Verify channel matches platform field in platform_daily_spend."}

            if not test_rows or test_rows[0].get("total_impressions") is None:
                return {
                    "error": "No data found for test group.",
                    "channel": channel,
                    "date_range": f"{test_date_from} → {test_date_to}",
                    "hint": (
                        "Verify 'channel' matches the platform column in platform_daily_spend. "
                        "Common values: 'google_ads', 'meta', 'tiktok', 'linkedin'."
                    ),
                }

            tr = test_rows[0]
            kpi_test     = float(tr.get("total_conversions")  or 0.0)
            exposed_test = float(tr.get("total_impressions")  or 0.0)
            test_days    = int(tr.get("n_days") or 1)
            if spend_test_input is None:
                spend_test_input = float(tr.get("total_spend") or 0.0)

        spend_test_val = spend_test_input or 0.0

        # ── 2. Fetch control group data ────────────────────────────────────────
        ctrl_date_from = control_date_from or test_date_from
        ctrl_date_to   = control_date_to   or test_date_to

        if kpi_control is None or exposed_control is None:
            ctrl_geo_clause = ""
            if methodology == "geo_holdout" and control_regions:
                region_list = ", ".join(
                    f"'{r.replace(chr(39), chr(39)*2)}'" for r in control_regions
                )
                ctrl_geo_clause = f"AND geo_country_code IN ({region_list})"
            elif methodology == "conversion_lift" and test_regions:
                # Same geos as test, different date range
                region_list = ", ".join(
                    f"'{r.replace(chr(39), chr(39)*2)}'" for r in test_regions
                )
                ctrl_geo_clause = f"AND geo_country_code IN ({region_list})"

            ctrl_sql = f"""
            SELECT
                SUM(CAST(spend AS FLOAT64))       AS total_spend,
                SUM(impressions)                   AS total_impressions,
                SUM(platform_conversions)          AS total_conversions,
                COUNT(DISTINCT date)               AS n_days
            FROM {bq.table_ref('platform_daily_spend')}
            WHERE date BETWEEN '{ctrl_date_from}' AND '{ctrl_date_to}'
              AND platform = '{safe_channel}'
              {ctrl_geo_clause}
            """
            try:
                ctrl_rows = bq.run_query(ctrl_sql)
            except Exception as exc:
                return {"error": f"Failed to fetch control group data from BQ: {exc}"}

            if not ctrl_rows or ctrl_rows[0].get("total_impressions") is None:
                return {
                    "error": "No data found for control group.",
                    "hint": (
                        "geo_holdout: verify control_regions are populated in platform_daily_spend. "
                        "conversion_lift: verify control_date_from / control_date_to."
                    ),
                }

            cr = ctrl_rows[0]
            kpi_control     = float(cr.get("total_conversions")  or 0.0)
            exposed_control = float(cr.get("total_impressions")  or 0.0)
            if spend_control_input is None:
                spend_control_input = float(cr.get("total_spend") or 0.0)

        spend_control_val = spend_control_input or 0.0

        # ── 3. Guard against insufficient data ─────────────────────────────────
        if exposed_test < 100 or exposed_control < 100:
            return {
                "error": "Insufficient exposure data for reliable inference.",
                "exposed_test":     exposed_test,
                "exposed_control":  exposed_control,
                "minimum_required": 100,
                "hint": (
                    "Extend the date range, widen the geographic split, or reduce "
                    "min_weekly_impressions in meridian_data_loader to include more geos."
                ),
            }

        # ── 4. Conversion rates and z-test ────────────────────────────────────
        cvr_test    = kpi_test    / exposed_test
        cvr_control = kpi_control / exposed_control

        # Pooled CVR for z-test (two-proportion)
        cvr_pooled = (kpi_test + kpi_control) / (exposed_test + exposed_control)

        # Two-proportion z-statistic: one-tailed H1: cvr_test > cvr_control
        if 0 < cvr_pooled < 1:
            se_pooled = math.sqrt(
                cvr_pooled * (1.0 - cvr_pooled) * (1.0 / exposed_test + 1.0 / exposed_control)
            )
        else:
            se_pooled = 1e-9

        z_score = (cvr_test - cvr_control) / max(se_pooled, 1e-12)

        # One-tailed p-value: P(Z > z | H0)  using erfc (no scipy)
        p_value = 0.5 * math.erfc(z_score / math.sqrt(2))
        is_significant = p_value < (1.0 - confidence_level)

        # ── 5. Lift % with CI ─────────────────────────────────────────────────
        se_diff = math.sqrt(
            cvr_test    * (1.0 - cvr_test)    / exposed_test +
            cvr_control * (1.0 - cvr_control) / exposed_control
        )
        lift_pct      = (cvr_test - cvr_control) / max(cvr_control, 1e-9)
        lift_lower_90 = (cvr_test - cvr_control - z_alpha * se_diff) / max(cvr_control, 1e-9)
        lift_upper_90 = (cvr_test - cvr_control + z_alpha * se_diff) / max(cvr_control, 1e-9)

        # ── 6. iROAS estimation ────────────────────────────────────────────────
        incremental_cvr         = cvr_test - cvr_control
        incremental_conversions = incremental_cvr * exposed_test

        # Incremental spend: test spend minus the counterfactual (control spend rate × test exposure)
        # Avoids double-counting baseline spend that would have occurred even without the campaign.
        if exposed_control > 0 and spend_control_val > 0:
            cpm_control       = spend_control_val / exposed_control  # spend per impression
            counterfactual_sp = cpm_control * exposed_test
            incremental_spend = max(spend_test_val - counterfactual_sp, spend_test_val * 0.01)
        else:
            # geo_holdout with no control spend: treat all test spend as incremental
            incremental_spend = max(spend_test_val, 1e-6)

        iroas_est   = (incremental_conversions * avg_conversion_value) / incremental_spend
        iroas_se    = (se_diff * exposed_test * avg_conversion_value) / incremental_spend
        iroas_lower = max(0.0, iroas_est - z_alpha * iroas_se)
        iroas_upper = max(iroas_est, iroas_est + z_alpha * iroas_se)

        # ── 7. Log-normal Bayesian posterior update ────────────────────────────
        # Prior: log(ROI) ~ N(mu₀, σ₀²) — weak observational default
        # Likelihood: log(iROAS_est) with standard error se_log
        # Posterior: conjugate Gaussian — precision-weighted average
        log_iroas_est = math.log(max(iroas_est, 1e-6))

        # SE of log(iROAS) via delta method: se_log ≈ iroas_se / iroas_est
        if iroas_est > 1e-6 and iroas_se > 0:
            se_log = iroas_se / iroas_est
        elif iroas_upper > iroas_lower > 0:
            # Fallback: derive se_log from CI width
            se_log = (math.log(max(iroas_upper, 1e-6)) - math.log(max(iroas_lower, 1e-6))) / (2 * z_alpha)
        else:
            se_log = 0.5

        se_log = max(se_log, 0.02)  # floor prevents degenerate posterior collapse

        tau_prior   = 1.0 / (_DEFAULT_PRIOR_SIGMA ** 2)
        tau_obs     = 1.0 / (se_log ** 2)
        tau_post    = tau_prior + tau_obs
        roi_prior_mu    = (tau_prior * _DEFAULT_PRIOR_MU + tau_obs * log_iroas_est) / tau_post
        roi_prior_sigma = math.sqrt(1.0 / tau_post)

        # E[ROI] under posterior (log-normal mean formula)
        posterior_mean_roi = math.exp(roi_prior_mu + roi_prior_sigma ** 2 / 2.0)

        # ── 8. Write experiment record (MERGE — idempotent) ───────────────────
        safe_exp_id   = experiment_id.replace("'", "''")
        safe_notes    = (notes or "").replace("'", "''")
        test_json     = json.dumps(test_regions or [])
        ctrl_json     = json.dumps(control_regions or [])

        exp_merge_sql = f"""
        MERGE {bq.table_ref('incrementality_experiments')} AS target
        USING (SELECT '{safe_exp_id}' AS experiment_id) AS source
        ON target.experiment_id = source.experiment_id
        WHEN MATCHED THEN UPDATE SET
            status     = 'completed',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            experiment_id, channel, methodology,
            test_group_ids, control_group_ids,
            test_date_from, test_date_to,
            control_date_from, control_date_to,
            kpi, status, created_by, notes,
            created_at, updated_at
        ) VALUES (
            '{safe_exp_id}', '{safe_channel}', '{methodology}',
            JSON '{test_json}', JSON '{ctrl_json}',
            DATE '{test_date_from}', DATE '{test_date_to}',
            DATE '{ctrl_date_from}', DATE '{ctrl_date_to}',
            'conversions', 'completed', 'analyst_agent', '{safe_notes}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """
        try:
            bq.run_dml(exp_merge_sql)
        except Exception as exc:
            log.warning("analyst.incrementality.exp_merge_failed", error=str(exc))

        # ── 9. Write lift result row ──────────────────────────────────────────
        result_id    = bq.new_uuid()
        result_row   = {
            "result_id":               result_id,
            "experiment_id":           experiment_id,
            "channel":                 channel,
            "methodology":             methodology,
            "measurement_date":        test_date_to,
            "test_date_from":          test_date_from,
            "test_date_to":            test_date_to,
            "control_date_from":       ctrl_date_from,
            "control_date_to":         ctrl_date_to,
            "measurement_window_days": test_days,
            "kpi":                     "conversions",
            "kpi_test":                round(kpi_test, 4),
            "kpi_control":             round(kpi_control, 4),
            "exposed_test":            round(exposed_test, 0),
            "exposed_control":         round(exposed_control, 0),
            # NUMERIC fields stored as strings to preserve precision
            "spend_test":              str(round(spend_test_val, 4)),
            "spend_control":           str(round(spend_control_val, 4)),
            "avg_conversion_value":    str(round(avg_conversion_value, 4)),
            "cvr_test":                round(cvr_test, 8),
            "cvr_control":             round(cvr_control, 8),
            "lift_pct":                round(lift_pct, 6),
            "lift_pct_lower_90":       round(lift_lower_90, 6),
            "lift_pct_upper_90":       round(lift_upper_90, 6),
            "incremental_conversions": round(incremental_conversions, 4),
            "incremental_spend":       str(round(incremental_spend, 4)),
            "iroas_mean":              round(iroas_est, 6),
            "iroas_std":               round(iroas_se, 6),
            "iroas_lower_90":          round(iroas_lower, 6),
            "iroas_upper_90":          round(iroas_upper, 6),
            "z_score":                 round(z_score, 4),
            "p_value":                 round(p_value, 6),
            "confidence_level":        confidence_level,
            "is_significant":          is_significant,
            # Meridian prior parameters (log-normal)
            "roi_prior_mu":            round(roi_prior_mu, 6),
            "roi_prior_sigma":         round(roi_prior_sigma, 6),
            "is_active":               mark_active,
            "notes":                   notes,
            "created_by":              "analyst_agent",
            "created_at":              now_str,
        }

        errors = bq.insert_rows("incrementality_lift_results", [result_row])
        if errors:
            log.error("analyst.incrementality.write_failed", errors=errors)
            return {"error": "BQ streaming insert failed", "bq_errors": str(errors)}

        log.info(
            "analyst.incrementality.complete",
            result_id=result_id,
            experiment_id=experiment_id,
            channel=channel,
            iroas_mean=round(iroas_est, 4),
            lift_pct_pct=f"{round(lift_pct * 100, 1)}%",
            p_value=round(p_value, 4),
            is_significant=is_significant,
            roi_prior_mu=round(roi_prior_mu, 4),
            roi_prior_sigma=round(roi_prior_sigma, 4),
            mark_active=mark_active,
        )

        return {
            "result_id":          result_id,
            "experiment_id":      experiment_id,
            "channel":            channel,
            "methodology":        methodology,
            "measurement_date":   test_date_to,
            # ── Lift estimates (human-readable) ────────────────────────────────
            "lift_pct":           f"{round(lift_pct * 100, 2)}%",
            "lift_ci_90":         f"[{round(lift_lower_90 * 100, 1)}%, {round(lift_upper_90 * 100, 1)}%]",
            # ── iROAS estimates ────────────────────────────────────────────────
            "iroas_mean":         round(iroas_est, 4),
            "iroas_ci_90":        f"[{round(iroas_lower, 3)}, {round(iroas_upper, 3)}]",
            "incremental_convs":  round(incremental_conversions, 1),
            "incremental_spend":  round(incremental_spend, 2),
            # ── Statistical significance ──────────────────────────────────────
            "p_value":            round(p_value, 4),
            "z_score":            round(z_score, 4),
            "is_significant":     is_significant,
            "confidence_level":   confidence_level,
            # ── Meridian calibration priors (Task 27 hook) ─────────────────────
            "roi_prior_mu":       round(roi_prior_mu, 4),    # log-normal location
            "roi_prior_sigma":    round(roi_prior_sigma, 4), # log-normal scale (tighter = stronger)
            "posterior_mean_roi": round(posterior_mean_roi, 3),  # E[ROI] = exp(mu + σ²/2)
            "is_active":          mark_active,
            "meridian_note": (
                f"Priors written: mu={round(roi_prior_mu, 4)}, "
                f"sigma={round(roi_prior_sigma, 4)} "
                f"(vs. default sigma=0.9). "
                f"Posterior E[ROI] ≈ {round(posterior_mean_roi, 2)}x. "
                "These parameters auto-inject into the next run_mmm_model call "
                "via v_incrementality_roi_priors — no manual roi_priors dict needed."
                if mark_active and is_significant else
                "Result stored but NOT activated for Meridian calibration "
                "(is_significant=False or mark_active=False). "
                "Re-run with mark_active=True after verifying data quality."
            ),
            "bq_tables_written": [
                "incrementality_experiments",
                "incrementality_lift_results",
            ],
        }

    def _tool_analyze_marketing_intervention(
        self,
        target_metric:            str,
        pre_period_from:          str,
        pre_period_to:            str,
        post_period_from:         str,
        post_period_to:           str,
        target_channel:           str | None = None,
        target_geo:               str | None = None,
        control_channels:         list[str] | None = None,
        intervention_description: str | None = None,
        n_draws:                  int = 200,
        analyst_notes:            str | None = None,
    ) -> dict:
        """
        Bayesian Structural Time Series causal impact analysis (Task 24).

        Fetches daily metric data from platform_daily_spend, constructs a CausalInputData
        object, delegates to causal_analyst_engine.run_causal_analysis() for BSTS inference,
        and returns both the structured result dict and a Markdown summary table.

        Returns JSON-ready dict with:
            markdown_summary  — formatted Markdown table for agent response
            cumulative_effect, relative_effect_pct — headline numbers
            posterior_tail_prob — Bayesian p-value analog
            series — full daily counterfactual + effect series
            bq_write — confirmation of BigQuery streaming insert
        """
        try:
            import pandas as pd
            from tools.causal_analyst_engine import CausalInputData, run_causal_analysis
        except ImportError as exc:
            return {
                "status": "dependency_missing",
                "error": str(exc),
                "fix": "pip install 'paid-media-agent[causal]'",
                "note": (
                    "The BSTS causal impact engine requires tensorflow-probability[jax] and jax[cpu]. "
                    "These are not in the core install. Run: pip install 'paid-media-agent[causal]'"
                ),
            }

        # ── 1. Validate dates & build range ────────────────────────────────────
        safe_channel = (target_channel or "").replace("'", "''")
        safe_geo     = (target_geo     or "").replace("'", "''")

        # Column mapping for target_metric → platform_daily_spend SQL expression
        _METRIC_SQL = {
            "conversions": "SUM(platform_conversions)",
            "impressions": "SUM(impressions)",
            "spend":       "SUM(CAST(spend AS FLOAT64))",
            "clicks":      "SUM(clicks)",
        }
        if target_metric not in _METRIC_SQL:
            return {
                "error": f"Unknown target_metric: {target_metric!r}. "
                         f"Must be one of: {list(_METRIC_SQL.keys())}",
            }

        metric_agg  = _METRIC_SQL[target_metric]
        channel_filter = f"AND platform = '{safe_channel}'" if safe_channel else ""
        geo_filter     = f"AND geo_country_code = '{safe_geo}'" if safe_geo else ""

        full_from = pre_period_from
        full_to   = post_period_to

        # ── 2. Fetch target metric series ──────────────────────────────────────
        target_sql = f"""
        SELECT date, {metric_agg} AS value
        FROM {bq.table_ref('platform_daily_spend')}
        WHERE date BETWEEN '{full_from}' AND '{full_to}'
          {channel_filter}
          {geo_filter}
        GROUP BY date
        ORDER BY date
        """
        try:
            target_rows = bq.run_query(target_sql)
        except Exception as exc:
            return {
                "error": f"Failed to fetch target metric from BQ: {exc}",
                "hint": "Verify target_channel matches the platform field in platform_daily_spend.",
            }

        if not target_rows:
            return {
                "error": "No data found for the specified target_metric, channel, and date range.",
                "target_metric": target_metric,
                "target_channel": target_channel,
                "period": f"{full_from} → {full_to}",
                "hint": "Check that platform_daily_spend is populated for these dates and channel.",
            }

        # Align to full date range (fill missing days with 0)
        all_dates = pd.date_range(full_from, full_to, freq="D")
        target_by_date = {str(r["date"])[:10]: float(r["value"] or 0.0) for r in target_rows}
        aligned_values = [target_by_date.get(str(d.date()), 0.0) for d in all_dates]
        aligned_dates  = [str(d.date()) for d in all_dates]

        pre_mask  = [d <= pre_period_to   for d in aligned_dates]
        post_mask = [d >= post_period_from for d in aligned_dates]

        import numpy as np
        arr = np.array(aligned_values, dtype=float)
        y_pre  = arr[[i for i, m in enumerate(pre_mask)  if m]]
        y_post = arr[[i for i, m in enumerate(post_mask) if m]]
        dates_pre  = [d for d, m in zip(aligned_dates, pre_mask)  if m]
        dates_post = [d for d, m in zip(aligned_dates, post_mask) if m]

        if len(y_pre) == 0 or len(y_post) == 0:
            return {
                "error": "Pre or post period yielded zero data points after date alignment.",
                "n_pre": len(y_pre),
                "n_post": len(y_post),
                "hint": "Check pre_period_to < post_period_from (non-overlapping windows).",
            }

        # ── 3. Fetch control series (optional) ────────────────────────────────
        control_pre:  np.ndarray | None = None
        control_post: np.ndarray | None = None
        control_names: list[str] = []

        if control_channels:
            ctrl_channels_safe = [c.replace("'", "''") for c in control_channels]
            ctrl_list = ", ".join(f"'{c}'" for c in ctrl_channels_safe)

            ctrl_sql = f"""
            SELECT date, platform, {metric_agg} AS value
            FROM {bq.table_ref('platform_daily_spend')}
            WHERE date BETWEEN '{full_from}' AND '{full_to}'
              AND platform IN ({ctrl_list})
              {geo_filter}
            GROUP BY date, platform
            ORDER BY date, platform
            """
            try:
                ctrl_rows = bq.run_query(ctrl_sql)
            except Exception as exc:
                log.warning("analyst.causal.control_fetch_failed", error=str(exc))
                ctrl_rows = []

            if ctrl_rows:
                # Pivot to [n_dates, K] matrix aligned to same date range
                ctrl_df = pd.DataFrame(ctrl_rows)
                ctrl_df["date"] = ctrl_df["date"].astype(str).str[:10]
                ctrl_df["value"] = ctrl_df["value"].fillna(0.0).astype(float)
                pivoted = ctrl_df.pivot_table(
                    index="date", columns="platform", values="value", aggfunc="sum"
                ).reindex(aligned_dates, fill_value=0.0)

                control_names = list(pivoted.columns)
                ctrl_arr = pivoted.to_numpy(dtype=float)

                control_pre  = ctrl_arr[[i for i, m in enumerate(pre_mask)  if m], :]
                control_post = ctrl_arr[[i for i, m in enumerate(post_mask) if m], :]

        # ── 4. Build CausalInputData and run analysis ─────────────────────────
        data = CausalInputData(
            y_pre=y_pre,
            y_post=y_post,
            control_pre=control_pre,
            control_post=control_post,
            dates_pre=dates_pre,
            dates_post=dates_post,
            control_names=control_names,
            target_metric=target_metric,
            target_channel=target_channel,
            target_geo=target_geo,
        )

        try:
            pipeline_result = run_causal_analysis(
                data=data,
                n_draws=n_draws,
                n_chains=4,
                n_warmup=max(50, n_draws // 2),
                write_to_bq=True,
                intervention_description=intervention_description,
                analyst_notes=analyst_notes,
            )
        except ValueError as exc:
            return {
                "status": "insufficient_data",
                "error": str(exc),
                "n_pre": len(y_pre),
                "n_post": len(y_post),
                "fix": (
                    "Extend pre_period_from to include more baseline days. "
                    f"Need ≥ {len(y_post) * 4} pre-period observations for {len(y_post)} post days."
                ),
            }
        except ImportError as exc:
            return {
                "status": "dependency_missing",
                "error": str(exc),
                "fix": "pip install 'paid-media-agent[causal]'",
            }
        except Exception as exc:
            log.error("analyst.causal.pipeline_failed", error=str(exc))
            return {
                "status": "failed",
                "error": str(exc),
                "note": "Check Cloud Logging for the full traceback.",
            }

        log.info(
            "analyst.causal.complete",
            run_id=pipeline_result["run_id"],
            channel=target_channel,
            metric=target_metric,
            cumulative_effect=pipeline_result["cumulative_effect"],
            relative_pct=pipeline_result["relative_effect_pct"],
            posterior_tail_prob=pipeline_result["posterior_tail_prob"],
            is_significant=pipeline_result["is_significant"],
        )

        return {
            "status":               "completed",
            "run_id":               pipeline_result["run_id"],
            "is_significant":       pipeline_result["is_significant"],
            "cumulative_effect":    pipeline_result["cumulative_effect"],
            "relative_effect_pct":  pipeline_result["relative_effect_pct"],
            "posterior_tail_prob":  pipeline_result["posterior_tail_prob"],
            "statistical_certainty_pct": round((1.0 - pipeline_result["posterior_tail_prob"]) * 100, 1),
            "r_hat_max":            pipeline_result["r_hat_max"],
            "elapsed_seconds":      pipeline_result["elapsed_seconds"],
            "model_components":     pipeline_result["model_components"],
            "bq_tables_written":    ["causal_impact_runs", "causal_impact_metrics"],
            # Full series for downstream programmatic use
            "series":               pipeline_result["series"],
            # Markdown summary — surface this directly in agent response
            "markdown_summary":     pipeline_result["markdown_summary"],
        }

    def _tool_complete_attribution_run(
        self,
        run_id: str,
        status: str,
        paths_modeled: int | None = None,
        conversions_attributed: int | None = None,
        identity_match_rate: float | None = None,
        avg_path_length: float | None = None,
        error_message: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        set_clauses = [
            f"status = '{status}'",
            f"completed_at = TIMESTAMP '{now}'",
        ]
        if paths_modeled is not None:
            set_clauses.append(f"paths_modeled = {paths_modeled}")
        if conversions_attributed is not None:
            set_clauses.append(f"conversions_attributed = {conversions_attributed}")
        if identity_match_rate is not None:
            set_clauses.append(f"identity_match_rate = {identity_match_rate}")
        if avg_path_length is not None:
            set_clauses.append(f"avg_path_length = {avg_path_length}")
        if error_message:
            set_clauses.append(f"error_message = '{error_message.replace(chr(39), chr(39)*2)}'")

        bq.run_dml(f"""
            UPDATE {bq.table_ref('attribution_runs')}
            SET {', '.join(set_clauses)}
            WHERE run_id = '{run_id}'
        """)

        log.info("analyst.run_completed", run_id=run_id, status=status)
        return {"run_id": run_id, "status": status, "completed_at": now}

    def _tool_fetch_market_momentum_signals(
        self,
        keywords: list[str],
        subreddits: list[str] | None = None,
        lookback_days: int = 30,
        geo_code: str = "US",
        sources: list[str] | None = None,
    ) -> dict:
        """
        Ingest Google Trends + Reddit signals for a keyword set.

        Delegates to tools/social_listening_client.run_social_listening() which:
          1. Fetches Google Trends interest-over-time (current + prior windows)
          2. Fetches Reddit posts via PRAW across target subreddits
          3. Normalises and scores sentiment on all mention text
          4. Writes rows to social_trend_signals + social_mentions_staging
          5. Computes MoM velocity per keyword

        Returns a dual payload:
          signal_summary  — per-keyword velocity data for backend consumption
          markdown_summary — formatted Markdown table for user presentation
        """
        from tools.social_listening_client import run_social_listening

        result = run_social_listening(
            keywords=keywords,
            subreddits=subreddits,
            lookback_days=lookback_days,
            geo_code=geo_code,
            sources=sources,
        )

        signal_summary: dict = result.get("signal_summary", {})

        markdown_summary = _build_momentum_markdown(
            keywords=keywords,
            signal_summary=signal_summary,
            run_id=result["run_id"],
            signals_written=result["signals_written"],
            mentions_written=result["mentions_written"],
            lookback_days=lookback_days,
            geo_code=geo_code,
            sources=sources or ["google_trends", "reddit"],
            errors=result.get("errors", []),
        )

        log.info(
            "analyst.market_momentum.complete",
            run_id=result["run_id"],
            keywords=keywords,
            signals_written=result["signals_written"],
            mentions_written=result["mentions_written"],
            status=result["status"],
        )

        return {
            "ok":              result["status"] in ("completed", "partial"),
            "run_id":          result["run_id"],
            "status":          result["status"],
            "signals_written": result["signals_written"],
            "mentions_written": result["mentions_written"],
            "keywords":        keywords,
            "geo_code":        geo_code,
            "lookback_days":   lookback_days,
            "signal_summary":  signal_summary,
            "markdown_summary": markdown_summary,
            "errors":          result.get("errors", []),
        }

    def _tool_ingest_and_analyze_market_signals(
        self,
        source_urls: list[str],
        competitor_name: str,
        category: str | None = None,
    ) -> dict:
        """
        Execute a full market signals extraction and inference run.

        Delegates to tools/market_signals_client.MarketSignalsClient.run_extraction():
          1. Fetch + normalize each source URL
          2. Write to market_signals_staging
          3. Claude inference via resolved prompt (private or public fallback)
          4. Write to competitor_messaging_vectors
          5. Log run to market_signals_runs

        Returns a dual payload:
          markdown_summary           — competitor comparison matrix with counter-positioning guide
          competitor_context_for_copy — pre-formatted string for Task 26 copy prompt injection
          (plus all backend result fields: run_id, signal_count, vector, prompt_source, errors)
        """
        from tools.market_signals_client import (
            MarketSignalsClient,
            get_competitor_context,
            format_competitor_context_for_copy,
        )

        client = MarketSignalsClient()
        result = client.run_extraction(
            source_urls=source_urls,
            competitor_name=competitor_name,
            category=category,
        )

        # ── Build Markdown summary ─────────────────────────────────────────
        vector = result.get("vector") or {}
        markdown_summary = _build_market_signals_markdown(
            competitor_name=competitor_name,
            category=category,
            result=result,
            vector=vector,
        )

        # ── Task 26 optimization hook ──────────────────────────────────────
        # Fetch context from BQ (includes this run + any prior runs for this competitor)
        # and format as a copy-prompt-ready string for generate_creative_campaign_brief().
        recent_vectors = get_competitor_context(
            competitor_name=competitor_name,
            category=category,
            limit=3,
        )
        competitor_context_for_copy = format_competitor_context_for_copy(recent_vectors)

        log.info(
            "analyst.market_signals.complete",
            run_id=result.get("run_id"),
            competitor=competitor_name,
            signal_count=result.get("signal_count", 0),
            prompt_source=result.get("prompt_source"),
            ok=result.get("ok"),
        )

        return {
            **result,
            "competitor_name":           competitor_name,
            "category":                  category,
            "markdown_summary":          markdown_summary,
            "competitor_context_for_copy": competitor_context_for_copy,
        }

    def _tool_audit_data_attribution_cleanliness(
        self,
        lookback_days: int = 90,
        attribution_window_days: int = 7,
        lead_source_offline_patterns: list[str] | None = None,
    ) -> dict:
        """
        Run all three forensic attribution integrity tests and return a dual payload.

        Delegates to tools/attribution_verifier.AttributionVerifier.run_audit().
        Anomaly rows are streamed to data_attribution_anomalies in BigQuery.
        Correction multipliers become available via v_attribution_correction_weights.

        Returns:
          run_id, anomaly_count, cleanliness_score (0–100),
          total_pipeline_at_risk, anomalies_by_type, anomalies_by_platform,
          anomalies_by_lead_source, test_results,
          markdown_summary (audit brief),
          mmm_note (instructions for applying correction weights to next MMM run).
        """
        from tools.attribution_verifier import AttributionVerifier

        verifier = AttributionVerifier()
        result = verifier.run_audit(
            lookback_days=int(lookback_days),
            attribution_window_days=int(attribution_window_days),
            lead_source_offline_patterns=lead_source_offline_patterns,
        )

        markdown_summary = _build_audit_markdown(result)

        log.info(
            "analyst.attribution_audit.complete",
            run_id=result["run_id"],
            anomaly_count=result["anomaly_count"],
            cleanliness_score=result["cleanliness_score"],
            pipeline_at_risk=result["total_pipeline_at_risk"],
        )

        mmm_note = (
            "Attribution correction weights are now available in v_attribution_correction_weights. "
            "On the next run_mmm_model call, the meridian_data_loader will read these weights "
            "and apply per-channel/geo correction multipliers to the KPI tensor when "
            "apply_attribution_correction=True is passed."
            if result["anomaly_count"] > 0
            else "No anomalies detected. Attribution data is clean — no MMM corrections needed."
        )

        return {
            **result,
            "markdown_summary": markdown_summary,
            "mmm_note":         mmm_note,
            "bq_views_updated": ["v_attribution_correction_weights"],
        }


# ── Market Signals Markdown builder (module-level, pure formatting) ────────────

def _build_market_signals_markdown(
    competitor_name: str,
    category: str | None,
    result: dict,
    vector: dict,
) -> str:
    """
    Build a structured Markdown comparison matrix for competitor analysis.

    Sections:
      1. Run summary (source count, prompt source, status)
      2. Competitor Messaging Profile (core fields)
      3. Keyword Frequency Analysis (top 15 detected terms)
      4. Messaging Pillars breakdown
      5. Counter-Positioning Action Guide (3 concrete hooks)
    """
    from datetime import date
    import json as _json

    today        = date.today().isoformat()
    run_id       = result.get("run_id", "—")
    ok           = result.get("ok", False)
    signal_count = result.get("signal_count", 0)
    prompt_src   = result.get("prompt_source", "public_fallback")
    errors       = result.get("errors", [])
    cat_label    = f" — {category}" if category else ""
    status_badge = "✅ Complete" if ok else "⚠️ Partial" if signal_count > 0 else "❌ Failed"
    prompt_badge = "🔒 Private heuristics" if prompt_src == "private" else "📋 Generic fallback"

    # ── Parse JSON fields safely ───────────────────────────────────────────
    def _parse_json(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (list, dict)):
            return raw
        try:
            return _json.loads(raw)
        except Exception:
            return default

    keywords  = _parse_json(vector.get("primary_keywords_detected"), [])
    pillars   = _parse_json(vector.get("messaging_pillars_json"), [])
    ctas      = _parse_json(vector.get("cta_patterns_json"), [])
    hooks     = _parse_json(vector.get("counter_positioning_hooks_json"), [])
    themes    = _parse_json(vector.get("key_themes_json"), [])

    lines: list[str] = [
        f"## 🕵️ Market Signals Report: {competitor_name}{cat_label}",
        "",
        f"| | |",
        f"|-|-|",
        f"| **Status** | {status_badge} |",
        f"| **Run ID** | `{run_id}` |",
        f"| **Analysis date** | {today} |",
        f"| **Sources ingested** | {signal_count} URL(s) |",
        f"| **Evaluation mode** | {prompt_badge} |",
    ]
    if errors:
        lines.append(f"| **Fetch errors** | {len(errors)} URL(s) failed — see errors list |")
    lines += ["", "---", ""]

    if not vector:
        lines += [
            "> ❌ No competitor messaging vector was produced.",
            "> Check that the source URLs returned valid content and that the "
            "Claude API key is configured.",
        ]
        return "\n".join(lines)

    # ── Section 1: Competitor Messaging Profile ────────────────────────────
    core_prop  = vector.get("core_value_prop") or "—"
    angle      = vector.get("observed_positioning_angle") or "—"
    audience   = vector.get("inferred_target_audience") or "—"
    tone       = vector.get("sentiment_tone") or "—"

    lines += [
        "## 📋 Competitor Messaging Profile",
        "",
        "| Dimension | Detected Value |",
        "|-----------|---------------|",
        f"| **Core Value Proposition** | {core_prop} |",
        f"| **Positioning Angle** | `{angle}` |",
        f"| **Inferred Target Audience** | {audience} |",
        f"| **Messaging Tone** | {tone} |",
    ]

    if ctas:
        cta_str = " · ".join(f'"{c}"' for c in ctas[:5])
        lines.append(f"| **CTA Patterns** | {cta_str} |")

    lines += ["", "---", ""]

    # ── Section 2: Keyword Frequency Analysis ─────────────────────────────
    lines += [
        "## 🔑 Primary Keywords Detected",
        "",
        "| Rank | Keyword | Signal Strength |",
        "|------|---------|----------------|",
    ]
    for i, kw in enumerate(keywords[:15], 1):
        kw_str = kw if isinstance(kw, str) else kw.get("keyword", str(kw))
        bar    = "█" * max(1, 15 - i) + "░" * (i - 1)
        lines.append(f"| {i} | `{kw_str}` | {bar} |")
    lines += ["", "---", ""]

    # ── Section 3: Messaging Pillars ──────────────────────────────────────
    if pillars:
        lines += ["## 🏛️ Messaging Pillars", ""]
        for p in pillars[:3]:
            pillar_name   = p.get("pillar", "—") if isinstance(p, dict) else str(p)
            supporting    = p.get("supporting_claims", []) if isinstance(p, dict) else []
            lines.append(f"### {pillar_name}")
            for claim in supporting[:3]:
                lines.append(f"- {claim}")
            lines.append("")
        lines += ["---", ""]

    # ── Section 4: Key Narrative Themes ───────────────────────────────────
    if themes:
        lines += [
            "## 🧵 Recurring Narrative Themes",
            "",
            "| Theme | Frequency | Example Phrase |",
            "|-------|-----------|---------------|",
        ]
        for t in themes[:5]:
            if isinstance(t, dict):
                theme_name = t.get("theme", "—")
                freq       = t.get("frequency", "—")
                example    = t.get("example_phrase", "—")[:80]
            else:
                theme_name, freq, example = str(t), "—", "—"
            lines.append(f"| {theme_name} | {freq} | *{example}* |")
        lines += ["", "---", ""]

    # ── Section 5: Counter-Positioning Action Guide ────────────────────────
    lines += [
        "## ⚔️ Counter-Positioning Action Guide",
        "",
        "*Concrete angles to differentiate your ad campaigns against this competitor's "
        "observed messaging.*",
        "",
    ]
    if hooks:
        for i, h in enumerate(hooks[:3], 1):
            if isinstance(h, dict):
                angle_h     = h.get("angle", "—")
                hook_text   = h.get("hook", "—")
                rationale   = h.get("rationale", "—")
            else:
                angle_h, hook_text, rationale = str(h), "—", "—"
            lines += [
                f"### {i}. {angle_h}",
                f"> **Ad Hook:** *\"{hook_text}\"*",
                f"> **Rationale:** {rationale}",
                "",
            ]
    else:
        lines += [
            "> No counter-positioning hooks were generated. "
            "This may indicate content was too thin or generic for strategic extraction.",
            "",
        ]

    lines += [
        "---",
        "",
        f"*Analysis produced by the Paid Media Analyst Agent — Market Signals Engine (Task 36).*",
        f"*Evaluation mode: {prompt_badge}. "
        f"Raw signals in `market_signals_staging` · Vector in `competitor_messaging_vectors`.*",
        f"*To use this as context in ad copy generation, pass the "
        "`competitor_context_for_copy` field to `generate_creative_campaign_brief`.*",
    ]

    return "\n".join(lines)


# ── Momentum Markdown builder (module-level, pure formatting) ─────────────────

_TREND_ICONS = {
    "rising":  "🟢 Rising",
    "falling": "🔴 Falling",
    "stable":  "🟡 Stable",
    "new":     "🆕 New",
    "unknown": "⬜ Unknown",
}


def _build_momentum_markdown(
    keywords: list[str],
    signal_summary: dict[str, dict],
    run_id: str,
    signals_written: int,
    mentions_written: int,
    lookback_days: int,
    geo_code: str,
    sources: list[str],
    errors: list[str],
) -> str:
    """
    Build the user-facing Markdown table summarising market momentum signals.

    Shows: keyword, trend direction, current avg interest score, MoM velocity %,
    and a quick interpretation note.
    """
    from datetime import date
    today = date.today().isoformat()

    # Sort keywords by velocity — rising first, then stable, then falling
    def _sort_key(kw: str) -> tuple:
        data = signal_summary.get(kw, {})
        direction = data.get("trend_direction", "unknown")
        velocity  = data.get("velocity_pct") or 0.0
        order_map = {"rising": 0, "new": 1, "stable": 2, "falling": 3, "unknown": 4}
        return (order_map.get(direction, 4), -abs(velocity))

    sorted_kws = sorted(keywords, key=_sort_key)

    lines: list[str] = [
        f"## 📊 Market Momentum Signals — {geo_code} | {today}",
        "",
        f"**Keywords tracked:** {len(keywords)}  |  "
        f"**Lookback:** {lookback_days} days  |  "
        f"**Sources:** {', '.join(sources)}",
        f"**Run ID:** `{run_id}`  |  "
        f"**Signals written:** {signals_written}  |  "
        f"**Mentions indexed:** {mentions_written}",
        "",
        "---",
        "",
        "### Signal Velocity Summary",
        "",
        "| Keyword | Trend | Current Score | MoM Velocity | Prior Score |",
        "|---------|-------|--------------|-------------|------------|",
    ]

    for kw in sorted_kws:
        data        = signal_summary.get(kw, {})
        direction   = data.get("trend_direction", "unknown")
        current_avg = data.get("current_avg")
        prior_avg   = data.get("prior_avg")
        velocity    = data.get("velocity_pct")

        trend_label   = _TREND_ICONS.get(direction, "⬜ Unknown")
        current_str   = f"{current_avg:.1f}" if current_avg is not None else "—"
        prior_str     = f"{prior_avg:.1f}" if prior_avg is not None else "—"
        velocity_str  = f"{velocity:+.1f}%" if velocity is not None else "—"

        lines.append(
            f"| {kw} | {trend_label} | {current_str} | {velocity_str} | {prior_str} |"
        )

    # Highlight top movers
    rising_kws = [
        kw for kw in sorted_kws
        if signal_summary.get(kw, {}).get("trend_direction") == "rising"
    ]
    falling_kws = [
        kw for kw in sorted_kws
        if signal_summary.get(kw, {}).get("trend_direction") == "falling"
    ]
    new_kws = [
        kw for kw in sorted_kws
        if signal_summary.get(kw, {}).get("trend_direction") == "new"
    ]

    lines += ["", "---", "", "### Key Observations", ""]

    if rising_kws:
        top = rising_kws[0]
        v   = signal_summary[top].get("velocity_pct")
        v_s = f"{v:+.1f}%" if v is not None else "n/a"
        lines.append(
            f"🟢 **Strongest rising term:** `{top}` ({v_s} MoM) — "
            "consider increasing keyword coverage and creative alignment."
        )

    if new_kws:
        lines.append(
            f"🆕 **New keywords (no prior baseline):** "
            + ", ".join(f"`{k}`" for k in new_kws)
            + " — first observation window. Monitor for sustained momentum."
        )

    if falling_kws:
        top_fall = falling_kws[0]
        v_fall   = signal_summary[top_fall].get("velocity_pct")
        v_fall_s = f"{v_fall:.1f}%" if v_fall is not None else "n/a"
        lines.append(
            f"🔴 **Declining term:** `{top_fall}` ({v_fall_s} MoM) — "
            "audit creative relevance and consider copy refresh."
        )

    if not rising_kws and not falling_kws and not new_kws:
        lines.append("All tracked terms show stable search momentum this period.")

    if errors:
        lines += [
            "",
            f"> ⚠️ **Partial data:** {len(errors)} source(s) returned errors.",
        ]
        for e in errors:
            lines.append(f"> - {e}")

    lines += [
        "",
        "---",
        "",
        "*Signal scores are Google Trends relative interest (0–100 within the keyword set).*",
        "*MoM velocity = (current window avg − prior window avg) / prior avg × 100.*",
        "*Reddit mention counts are normalised to 0–100 relative to the highest-volume keyword.*",
    ]

    return "\n".join(lines)


# ── Attribution Audit Markdown builder (module-level, pure formatting) ────────


def _build_audit_markdown(result: dict) -> str:
    """
    Build a structured Markdown audit brief for audit_data_attribution_cleanliness.

    Sections:
      1. Audit Run Summary (run_id, period, tests status)
      2. Data Cleanliness Score (0–100 visual gauge with grade)
      3. Anomaly Detection Matrix (by type and platform)
      4. Attribution Drift — Source Breakdown (which CRM source labels are affected)
      5. Recommendation Block (per-workflow corrective actions)
    """
    from datetime import date

    today          = date.today().isoformat()
    run_id         = result.get("run_id", "—")
    anomaly_count  = result.get("anomaly_count", 0)
    score          = float(result.get("cleanliness_score", 100.0))
    pipeline_risk  = float(result.get("total_pipeline_at_risk", 0.0))
    lookback_days  = result.get("lookback_days", 90)
    test_results   = result.get("test_results", {})
    by_type        = result.get("anomalies_by_type", {})
    by_platform    = result.get("anomalies_by_platform", {})
    by_source      = result.get("anomalies_by_lead_source", {})

    # ── Score interpretation ────────────────────────────────────────────────
    if score >= 90:
        grade, tier, badge = "A", "Clean", "✅"
    elif score >= 75:
        grade, tier, badge = "B", "Degraded", "🟡"
    elif score >= 60:
        grade, tier, badge = "C", "Contaminated", "🟠"
    else:
        grade, tier, badge = "D", "Critical", "🔴"

    filled  = int(score / 10)
    empty   = 10 - filled
    gauge   = "█" * filled + "░" * empty

    # ── Test status badges ──────────────────────────────────────────────────
    def _test_badge(test_key: str) -> str:
        tr = test_results.get(test_key, {})
        if not tr.get("ok", False):
            return "⚠️ Skipped"
        n = tr.get("anomalies_detected", 0)
        return f"✅ Clean (0 found)" if n == 0 else f"🔴 {n} anomalies"

    orphan_badge    = _test_badge("orphaned_token")
    diverge_badge   = _test_badge("timestamp_divergence")
    phantom_badge   = _test_badge("phantom_conversion")

    lines: list[str] = [
        f"## 🔬 Attribution Data Audit — {today}",
        "",
        f"| | |",
        f"|-|-|",
        f"| **Run ID** | `{run_id}` |",
        f"| **Lookback window** | {lookback_days} days |",
        f"| **Total anomalies detected** | {anomaly_count} |",
        f"| **Estimated pipeline at risk** | ${pipeline_risk:,.0f} |",
        "",
        "---",
        "",
    ]

    # ── Section 2: Cleanliness Score ───────────────────────────────────────
    lines += [
        "## 📊 Data Cleanliness Score",
        "",
        f"**{badge} {score:.0f}/100 — Grade {grade} ({tier})**",
        "",
        f"`{gauge}` {score:.0f}%",
        "",
    ]

    if score >= 90:
        lines.append(
            "> ✅ Attribution data is clean. No significant tracking integrity issues detected. "
            "No MMM correction adjustments are required for this period."
        )
    elif score >= 75:
        lines.append(
            "> 🟡 Minor attribution drift detected. Some CRM leads have been mislabelled "
            "away from their paid source. Review the source breakdown below and correct "
            "the highest-volume overwrite workflows."
        )
    elif score >= 60:
        lines.append(
            "> 🟠 Significant attribution contamination detected. Paid channel credit is "
            "being systematically shifted to offline sources. MMM models built on this "
            "data will under-estimate paid media ROI. Apply correction weights before "
            "the next Meridian run."
        )
    else:
        lines.append(
            "> 🔴 Critical attribution integrity failure. Phantom conversion events and/or "
            "pervasive CRM overwrites are corrupting the measurement stack. Escalate "
            "immediately to the CRM admin and GTM/pixel implementation team."
        )

    lines += ["", "---", ""]

    # ── Section 3: Anomaly Detection Matrix ────────────────────────────────
    lines += [
        "## 🧪 Forensic Test Results",
        "",
        "| Test | Status | Description |",
        "|------|--------|-------------|",
        f"| Orphaned Token Test | {orphan_badge} | Paid click token + offline CRM LeadSource |",
        f"| Timestamp Divergence | {diverge_badge} | LeadSource reassigned after click captured |",
        f"| Phantom Conversion Test | {phantom_badge} | Platform pixels > CRM conversions (gap > window) |",
        "",
        "---",
        "",
    ]

    if anomaly_count > 0:
        # ── Anomaly breakdown by type ─────────────────────────────────────
        lines += [
            "## 📋 Anomaly Breakdown",
            "",
            "**By Anomaly Type:**",
            "",
            "| Anomaly Type | Count | Severity | Impact |",
            "|-------------|-------|----------|--------|",
        ]
        _type_meta = {
            "orphaned_token":       ("Orphaned Token",       "Medium 🟡", "Wrong channel credited in CRM"),
            "timestamp_divergence": ("Timestamp Divergence",  "High 🟠",   "Proven CRM overwrite event"),
            "phantom_conversion":   ("Phantom Conversion",    "Critical 🔴", "Platform over-reporting conversions"),
        }
        for atype, count in sorted(by_type.items(), key=lambda x: -x[1]):
            label, sev, impact = _type_meta.get(atype, (atype, "Unknown", "—"))
            lines.append(f"| {label} | {count} | {sev} | {impact} |")

        # ── Anomaly breakdown by platform ─────────────────────────────────
        if by_platform:
            lines += [
                "",
                "**By Flagged Platform:**",
                "",
                "| Platform | Anomaly Count |",
                "|----------|--------------|",
            ]
            for platform, count in sorted(by_platform.items(), key=lambda x: -x[1]):
                lines.append(f"| {platform} | {count} |")

        lines += ["", "---", ""]

        # ── Section 4: Attribution Drift — Source Breakdown ───────────────
        lines += [
            "## 🗂️ Attribution Drift — CRM Source Breakdown",
            "",
            "*These CRM LeadSource labels are receiving credit that forensic evidence "
            "attributes to paid media channels. Each row represents a CRM workflow "
            "or data integration that is introducing attribution drift.*",
            "",
            "| CRM LeadSource (Receiving Unearned Credit) | Anomaly Count |",
            "|--------------------------------------------|--------------|",
        ]
        top_sources = sorted(by_source.items(), key=lambda x: -x[1])[:15]
        for source, count in top_sources:
            display = source.replace("_", " ").title() if source != "no_crm_source" else "— (Phantom, no CRM lead)"
            lines.append(f"| {display} | {count} |")

        lines += ["", "---", ""]

    # ── Section 5: Recommendation Block ────────────────────────────────────
    lines += [
        "## 🔧 Corrective Action Recommendations",
        "",
    ]

    orphan_n  = by_type.get("orphaned_token", 0)
    diverge_n = by_type.get("timestamp_divergence", 0)
    phantom_n = by_type.get("phantom_conversion", 0)

    if orphan_n > 0:
        lines += [
            "### 1. Orphaned Token Overwrites — CRM LeadSource Integration",
            "",
            f"> **{orphan_n} leads** were sourced by a paid click but reassigned to an offline label.",
            ">",
            "> **Root cause:** Manual rep edits or CRM workflow rules are overwriting `LeadSource` "
            "after the lead enters the system, discarding the original ad attribution.",
            ">",
            "> **Fix:** (a) Audit CRM workflow rules and triggers that modify `LeadSource` — "
            "implement a 'first touch wins' protection rule to prevent overwrite after initial "
            "paid attribution is set. (b) Add a secondary field `Paid_Lead_Source__c` that is "
            "write-protected once a click token is detected. (c) Review your Salesforce / HubSpot "
            "LeadSource picklist to ensure paid channels appear as valid options.",
            "",
        ]

    if diverge_n > 0:
        lines += [
            "### 2. Timestamp Divergence — Explicit CRM Source Overwrites",
            "",
            f"> **{diverge_n} leads** had their LeadSource changed to an offline label "
            "AFTER the paid click was captured.",
            ">",
            "> **Root cause:** SDR or marketing ops teams are manually re-sourcing leads after "
            "outreach contact — overwriting the original paid attribution timestamp.",
            ">",
            "> **Fix:** (a) Implement CRM field-level security to lock `LeadSource` after initial "
            "creation when a click token is present. (b) Introduce an `Original_Lead_Source__c` "
            "field mirroring the creation-time `LeadSource` value — make it read-only. "
            "(c) Run a de-duplication reconciliation to restore original attribution on "
            "high-value overwritten leads in the current pipeline.",
            "",
        ]

    if phantom_n > 0:
        lines += [
            "### 3. Phantom Conversions — Pixel / Tag Implementation",
            "",
            f"> **{phantom_n} day-platform combinations** show platform-reported conversions "
            "significantly exceeding CRM-matched conversions.",
            ">",
            "> **Root cause:** Conversion pixel misconfiguration — common causes include "
            "double-firing on form submissions, view-through conversion counting without "
            "deduplication, or test environment pixel events reaching production.",
            ">",
            "> **Fix:** (a) Audit GTM triggers for the conversion events flagged above — "
            "check for duplicate triggers on the same form submission. (b) Review each "
            "platform's conversion deduplication settings (order ID / transaction ID). "
            "(c) Add environment filters in GTM to exclude dev/staging domains from "
            "production conversion tags. (d) Consider switching to server-side conversion "
            "API (CAPI/ECA) for the affected platforms to get deduplication at the "
            "ingestion layer.",
            "",
        ]

    if anomaly_count == 0:
        lines += [
            "> ✅ No corrective actions required. Attribution data integrity is clean "
            f"for the {lookback_days}-day lookback window.",
            "",
        ]

    lines += [
        "---",
        "",
        "**MMM Calibration:**",
        (
            f"Correction weights for {len(by_platform)} platform(s) are now available in "
            "`v_attribution_correction_weights`. Pass `apply_attribution_correction=True` "
            "to the next `run_mmm_model` call to apply per-channel/geo/week multipliers "
            "before tensor packaging."
            if anomaly_count > 0
            else "No correction adjustments needed — all channels are clean."
        ),
        "",
        "---",
        "",
        f"*Attribution Forensic Verification Engine — Task 37 | Run ID: `{run_id}`*",
        f"*Anomalies written to `data_attribution_anomalies` · Correction view: `v_attribution_correction_weights`*",
    ]

    return "\n".join(lines)
