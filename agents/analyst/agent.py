"""
The Analyst — Attribution Modeling Agent.
Runs daily. Stitches accounts and updates MTA weightings in BigQuery.
"""
from agents.base import BaseAgent
from tools import bigquery_client as bq


SYSTEM = """You are the Analyst, an attribution modeling agent for a B2B paid media pipeline.

Your job on each daily run:
1. Run account stitching: map GA4 client_ids to Salesforce Account IDs using email-domain and IP routing signals.
2. Generate and execute the multi-touch attribution (MTA) weighting query.
3. Write the results back to the attribution_results table in BigQuery.
4. Return a summary of model outputs: top channels by pipeline influence, anomalies, and data freshness.

You have access to BigQuery. Write efficient SQL. Prefer incremental updates (WHERE date = CURRENT_DATE - 1) over full scans.
Current attribution model: Full-Path (first touch 30%, last touch 30%, middle touches share 40%).
When data volume exceeds 1,000 converted opportunities, note that Shapley/Markov models are ready to activate."""


class AnalystAgent(BaseAgent):
    name = "analyst"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "run_bigquery_query",
            "description": "Execute a SELECT query in BigQuery. Returns rows as a list of dicts.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Valid BigQuery SQL"},
                    "params": {
                        "type": "object",
                        "description": "Optional named query parameters",
                        "additionalProperties": True,
                    },
                },
                "required": ["sql"],
            },
        },
        {
            "name": "run_bigquery_dml",
            "description": "Execute an INSERT, UPDATE, MERGE, or CREATE TABLE AS SELECT statement. Returns rows affected.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"}
                },
                "required": ["sql"],
            },
        },
        {
            "name": "stitch_accounts",
            "description": (
                "Run the account-stitching job: match GA4 client_ids to Salesforce Account IDs "
                "using email-domain and IP routing signals. Writes to account_identity_map table. "
                "Returns match stats."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_days": {"type": "integer", "default": 1}
                },
                "required": [],
            },
        },
        {
            "name": "run_mta_model",
            "description": (
                "Generate and execute the Full-Path MTA weighting query for the given date range. "
                "Updates the attribution_results table. Returns top 10 channels by pipeline credit."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["start_date", "end_date"],
            },
        },
    ]

    def _tool_run_bigquery_query(self, sql: str, params: dict | None = None) -> dict:
        rows = bq.run_query(sql, params)
        return {"rows": rows[:200], "total_rows": len(rows)}  # cap to avoid huge context

    def _tool_run_bigquery_dml(self, sql: str) -> dict:
        affected = bq.run_dml(sql)
        return {"rows_affected": affected}

    def _tool_stitch_accounts(self, lookback_days: int = 1) -> dict:
        sql = f"""
        MERGE {bq.table_ref('account_identity_map')} AS target
        USING (
            SELECT
                g.client_id,
                s.account_id,
                SPLIT(s.email, '@')[SAFE_OFFSET(1)] AS email_domain,
                CURRENT_TIMESTAMP() AS stitched_at
            FROM {bq.table_ref('ga4_sessions')} g
            JOIN {bq.table_ref('salesforce_leads')} s
              ON (
                SPLIT(s.email, '@')[SAFE_OFFSET(1)] = g.page_hostname
                OR s.ip_address = g.ip_address
              )
            WHERE DATE(g.event_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
              AND s.account_id IS NOT NULL
        ) AS source
        ON target.client_id = source.client_id
        WHEN MATCHED THEN
            UPDATE SET account_id = source.account_id, stitched_at = source.stitched_at
        WHEN NOT MATCHED THEN
            INSERT (client_id, account_id, email_domain, stitched_at)
            VALUES (source.client_id, source.account_id, source.email_domain, source.stitched_at)
        """
        affected = bq.run_dml(sql)
        return {"rows_stitched": affected, "lookback_days": lookback_days}

    def _tool_run_mta_model(self, start_date: str, end_date: str) -> dict:
        # Full-Path: first=30%, last=30%, middle touches split 40%
        sql = f"""
        CREATE OR REPLACE TABLE {bq.table_ref('attribution_results')} AS
        WITH touchpoints AS (
            SELECT
                aim.account_id,
                t.client_id,
                t.campaign_id,
                t.channel,
                t.touchpoint_timestamp,
                t.opportunity_id,
                ROW_NUMBER() OVER (PARTITION BY t.client_id, t.opportunity_id ORDER BY t.touchpoint_timestamp ASC)  AS touch_seq,
                COUNT(*) OVER (PARTITION BY t.client_id, t.opportunity_id) AS total_touches
            FROM {bq.table_ref('touchpoint_events')} t
            JOIN {bq.table_ref('account_identity_map')} aim USING (client_id)
            WHERE DATE(t.touchpoint_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        ),
        weighted AS (
            SELECT
                *,
                CASE
                    WHEN total_touches = 1 THEN 1.0
                    WHEN touch_seq = 1 THEN 0.30
                    WHEN touch_seq = total_touches THEN 0.30
                    ELSE 0.40 / NULLIF(total_touches - 2, 0)
                END AS mta_weight
            FROM touchpoints
        )
        SELECT
            channel,
            campaign_id,
            COUNT(DISTINCT opportunity_id) AS influenced_opps,
            ROUND(SUM(mta_weight), 2) AS weighted_credit,
            '{start_date}' AS period_start,
            '{end_date}' AS period_end,
            CURRENT_TIMESTAMP() AS updated_at
        FROM weighted
        GROUP BY 1, 2
        ORDER BY weighted_credit DESC
        """
        bq.run_dml(sql)
        top = bq.run_query(
            f"SELECT channel, campaign_id, weighted_credit FROM {bq.table_ref('attribution_results')} ORDER BY weighted_credit DESC LIMIT 10"
        )
        return {"top_channels": top, "period": f"{start_date} → {end_date}"}
