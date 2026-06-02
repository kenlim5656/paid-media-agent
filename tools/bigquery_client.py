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
