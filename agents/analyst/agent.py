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
