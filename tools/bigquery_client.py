# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
BigQuery client — thin wrapper used by all three agents.

Table names are defined here as the single source of truth so they map
exactly to the paid-media-schema DDL (bigquery/*.sql).
"""
import uuid
from google.cloud import bigquery
from config import settings

_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=settings.gcp_project_id)
    return _client


# ── Table name registry ────────────────────────────────────────────────────────
# Maps logical names to fully-qualified BigQuery table references.
# All names must match paid-media-schema/bigquery/*.sql exactly.

_TABLES: dict[str, str] = {
    # ── Identity layer (01_identity.sql) ──────────────────────────────────────
    "identity_signals":          "identity_signals",
    "identity_entities":         "identity_entities",
    "identity_entity_signals":   "identity_entity_signals",
    "identity_stitching_log":    "identity_stitching_log",

    # ── Touchpoint layer (02_touchpoints.sql) ─────────────────────────────────
    "sessions":                  "sessions",
    "touchpoint_events":         "touchpoint_events",
    "conversion_events":         "conversion_events",

    # ── Platform layer (03_platform.sql) ──────────────────────────────────────
    "platform_campaigns":        "platform_campaigns",
    "platform_ad_groups":        "platform_ad_groups",
    "platform_ads":              "platform_ads",
    "platform_daily_spend":      "platform_daily_spend",
    "platform_daily_spend_adg":  "platform_daily_spend_ad_group",

    # ── Attribution layer (04_attribution.sql) ────────────────────────────────
    "attribution_paths":         "attribution_paths",
    "attribution_runs":          "attribution_runs",
    "attribution_results":       "attribution_results",
    "attribution_channel_summary": "attribution_channel_summary",

    # ── Agent output layer (05_agent_outputs.sql) ─────────────────────────────
    "watchdog_alerts":           "watchdog_alerts",
    "watchdog_capture_rate_log": "watchdog_capture_rate_log",
    "analyst_insights":          "analyst_insights",
    "operator_action_log":       "operator_action_log",
    "operator_pending_approvals": "operator_pending_approvals",

    # ── Reporting layer — tables (06_reporting.sql) ────────────────────────────
    "platform_keywords":              "platform_keywords",
    "platform_daily_spend_ad":        "platform_daily_spend_ad",
    "platform_daily_spend_keyword":   "platform_daily_spend_keyword",

    # ── Reporting layer — views (06_reporting.sql) ─────────────────────────────
    # These are BigQuery views, not base tables. They are read-only.
    # Use table_ref() to query them just like base tables.
    "v_campaign_performance":    "v_campaign_performance",
    "v_pacing_status":           "v_pacing_status",
    "v_roas_comparison":         "v_roas_comparison",
    "v_channel_efficiency":      "v_channel_efficiency",
    "v_ad_performance":          "v_ad_performance",
    "v_keyword_performance":     "v_keyword_performance",
    "v_daily_performance":       "v_daily_performance",

    # ── Account-based analytics layer — tables (07_account_analytics.sql) ───────
    "company_profiles":          "company_profiles",
    "ip_resolution_cache":       "ip_resolution_cache",
    "company_sessions":          "company_sessions",
    "company_engagement":        "company_engagement",
    "target_account_activity":   "target_account_activity",

    # ── Account-based analytics layer — views (07_account_analytics.sql) ────────
    "v_target_account_funnel":   "v_target_account_funnel",
    "v_dark_funnel_coverage":    "v_dark_funnel_coverage",

    # ── MMM layer — tables (08_mmm.sql) ──────────────────────────────────────
    # Written by tools/meridian_analyst_engine.py after each model run.
    "mmm_runs":                  "mmm_runs",
    "mmm_channel_contributions": "mmm_channel_contributions",

    # ── Incrementality layer — tables (09_incrementality.sql) ─────────────────
    # Written by agents/analyst/agent.py _tool_run_incrementality_analysis().
    "incrementality_experiments":  "incrementality_experiments",
    "incrementality_lift_results": "incrementality_lift_results",

    # ── Incrementality layer — view (09_incrementality.sql) ──────────────────
    # Latest significant lift result per channel, formatted for Meridian prior injection.
    # Read by tools/meridian_analyst_engine._get_roi_priors_from_bq().
    "v_incrementality_roi_priors": "v_incrementality_roi_priors",

    # ── Causal impact layer — tables (10_causal_impact.sql) ──────────────────
    # Written by tools/causal_analyst_engine.py after each BSTS analysis run.
    "causal_impact_runs":    "causal_impact_runs",
    "causal_impact_metrics": "causal_impact_metrics",

    # ── Reddit Ads layer — tables (13_reddit_ads.sql) ────────────────────────
    # Written by tools/reddit_ads_client.py run_daily_extraction().
    "reddit_ads_runs":           "reddit_ads_runs",
    "reddit_daily_spend":        "reddit_daily_spend",
    "reddit_spatial_performance": "reddit_spatial_performance",

    # ── Social listening layer — tables (12_social_listening.sql) ─────────────
    # Written by tools/social_listening_client.py run_social_listening().
    "social_listening_runs":    "social_listening_runs",
    "social_trend_signals":     "social_trend_signals",
    "social_mentions_staging":  "social_mentions_staging",

    # ── Audience mutation layer — table + view (14_audience_mutation.sql) ─────
    # Written by tools/audience_mutation_engine.py AudienceMutationEngine.
    "audience_mutation_logs":    "audience_mutation_logs",
    # Read by audience_mutation_engine._extract_seed_cohort().
    "v_lookalike_mutation_seed": "v_lookalike_mutation_seed",

    # ── Market signals layer — tables (15_market_signals.sql) ─────────────────
    # Written by tools/market_signals_client.py MarketSignalsClient.
    "market_signals_runs":           "market_signals_runs",
    "market_signals_staging":        "market_signals_staging",
    "competitor_messaging_vectors":  "competitor_messaging_vectors",

    # ── Unified reporting layer — views (17_unified_reporting.sql) ───────────────
    # Blend and normalize metrics across all 4 active channels + CRM pipeline.
    "v_unified_daily_spend":          "v_unified_daily_spend",
    "v_unified_spatial_performance":  "v_unified_spatial_performance",
    "v_reporting_campaign_roi":       "v_reporting_campaign_roi",
    "v_reporting_monthly_pacing":     "v_reporting_monthly_pacing",

    # ── Attribution forensics layer — table + view (16_attribution_forensics.sql) ─
    # Written by tools/attribution_verifier.py AttributionVerifier.run_audit().
    "data_attribution_anomalies":        "data_attribution_anomalies",
    # Aggregated correction multipliers — read by meridian_data_loader.
    "v_attribution_correction_weights":  "v_attribution_correction_weights",

    # ── Source / staging tables (org-defined, outside schema DDL) ─────────────
    # These are the raw source tables that the agents read from.
    # Names are configurable via settings but default to sensible values.
    "sgtm_request_logs":         "sgtm_request_logs",
    "crm_leads_staging":         "crm_leads_staging",
    "crm_opportunities_staging": "crm_opportunities_staging",
}


def table_ref(logical_name: str) -> str:
    """
    Return a fully-qualified BigQuery table reference for use in SQL.
    Raises KeyError if the logical name isn't registered.
    """
    table = _TABLES.get(logical_name)
    if table is None:
        raise KeyError(
            f"Unknown table: '{logical_name}'. "
            f"Register it in tools/bigquery_client.py _TABLES. "
            f"Available: {sorted(_TABLES.keys())}"
        )
    return f"`{settings.gcp_project_id}.{settings.gcp_dataset_id}.{table}`"


def new_uuid() -> str:
    """Generate a UUID suitable for use as a primary key in BigQuery."""
    return str(uuid.uuid4())


# ── Query helpers ──────────────────────────────────────────────────────────────

def run_query(sql: str, params: dict | None = None) -> list[dict]:
    client = get_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, _infer_type(v), v)
            for k, v in (params or {}).items()
        ]
    )
    job = client.query(sql, job_config=job_config)
    return [dict(row) for row in job.result()]


def run_dml(sql: str) -> int:
    """Execute DML (INSERT/UPDATE/MERGE/DELETE). Returns rows affected."""
    client = get_client()
    job = client.query(sql)
    job.result()
    return job.num_dml_affected_rows or 0


def insert_rows(table_logical_name: str, rows: list[dict]) -> list[dict]:
    """
    Streaming insert — use for single-row writes (alert logs, run records).
    Returns any insertion errors.
    """
    client = get_client()
    ref = table_ref(table_logical_name).strip("`")  # streaming API uses dotted form
    errors = client.insert_rows_json(ref, rows)
    return errors


def _infer_type(v: object) -> str:
    if isinstance(v, bool):
        return "BOOL"
    if isinstance(v, int):
        return "INT64"
    if isinstance(v, float):
        return "FLOAT64"
    return "STRING"
