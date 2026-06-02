# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
The Watchdog — Data Governance Agent.
Runs hourly. Monitors signal capture rates and CRM field health across
all configured ad platforms. Writes structured alerts and capture-rate
history to BigQuery (paid-media-schema agent output tables) so the
paid-media-mcp can surface them in interactive skill sessions.
"""
import httpx
import structlog
from datetime import datetime, timezone

from agents.base import BaseAgent
from config import settings
from tools import bigquery_client as bq
from tools import salesforce_client

log = structlog.get_logger()

# Namespace IDs for the signals we monitor (from identity_namespaces.json).
# Extend this list as the org adds new platform integrations.
MONITORED_NAMESPACES = [
    {"namespace_id": "platform_click_id.google.gclid",       "column": "gclid",        "platform": "google_ads"},
    {"namespace_id": "platform_click_id.google.dclid",       "column": "dclid",        "platform": "dv360"},
    {"namespace_id": "platform_click_id.meta.fbclid",        "column": "fbclid",       "platform": "meta"},
    {"namespace_id": "platform_click_id.linkedin.li_fat_id", "column": "li_fat_id",    "platform": "linkedin"},
    {"namespace_id": "platform_click_id.tiktok.ttclid",      "column": "ttclid",       "platform": "tiktok"},
    {"namespace_id": "analytics_cookie.google.ga4_client_id","column": "ga4_client_id","platform": "ga4"},
]

SYSTEM = """You are the Watchdog, a data governance agent for a paid media attribution system.

Your job is to monitor data quality across all ad platform → analytics → CRM → BigQuery pipelines.
You are platform-agnostic: monitor ALL configured signal namespaces, not just Google signals.

On each hourly run:
1. Call `audit_signal_capture_rates` — checks all monitored click ID and analytics cookie capture rates from the sessions table.
2. Call `audit_crm_null_fields` — checks for null media identifier fields on recent CRM lead records.
3. For any metric outside threshold, call `write_alert` — this persists to BigQuery AND sends a webhook notification.
4. Call `log_capture_rates` — write the capture rate time series to BigQuery regardless of status.
5. End with a terse status summary: GREEN (all ok) / YELLOW (warnings) / RED (critical breach).

Be diagnostic, not chatty. Name the specific namespace_id and platform for every finding."""


class WatchdogAgent(BaseAgent):
    name = "watchdog"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "audit_signal_capture_rates",
            "description": (
                "Query the sessions table in BigQuery to measure what percentage of sessions "
                "have each monitored click ID or analytics cookie present. "
                "Returns per-namespace capture rates for the past N hours."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "hours_back": {"type": "integer", "default": 1, "description": "Hours of history to sample"}
                },
                "required": [],
            },
        },
        {
            "name": "audit_crm_null_fields",
            "description": (
                "Check the CRM leads staging table for records missing media identifier fields. "
                "Detects spikes in null gclid, fbclid, li_fat_id, ga4_client_id, utm_source. "
                "Returns null count, total, and percentage for the past N hours."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "since_hours": {"type": "integer", "default": 1}
                },
                "required": [],
            },
        },
        {
            "name": "write_alert",
            "description": (
                "Persist a data quality alert to BigQuery (watchdog_alerts table) "
                "and send a webhook notification. Call this when any metric breaches its threshold."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "alert_type":        {"type": "string", "enum": [
                        "signal_capture_drop", "null_crm_fields", "conversion_gap",
                        "spend_anomaly", "identity_match_decline", "data_freshness",
                        "capi_match_rate", "dedup_failure",
                    ]},
                    "severity":          {"type": "string", "enum": ["info", "warning", "critical"]},
                    "namespace_id":      {"type": "string", "description": "The signal namespace affected, e.g. platform_click_id.google.gclid"},
                    "affected_platform": {"type": "string"},
                    "metric_name":       {"type": "string"},
                    "metric_value":      {"type": "number"},
                    "threshold_value":   {"type": "number"},
                    "description":       {"type": "string"},
                    "probable_cause":    {"type": "string"},
                    "recommended_action":{"type": "string"},
                },
                "required": ["alert_type", "severity", "description"],
            },
        },
        {
            "name": "log_capture_rates",
            "description": (
                "Write the capture rate measurements to BigQuery watchdog_capture_rate_log "
                "for trend tracking. Call this at the end of every run, even when no alerts fire."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "measurements": {
                        "type": "array",
                        "description": "List of {namespace_id, platform, total_events, events_with_signal, capture_rate_pct} dicts",
                        "items": {"type": "object", "additionalProperties": True},
                    },
                    "hours_back": {"type": "integer", "default": 1},
                },
                "required": ["measurements"],
            },
        },
    ]

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_audit_signal_capture_rates(self, hours_back: int = 1) -> dict:
        """
        Query sessions table for each monitored namespace.
        Sessions table has denormalized click ID columns — fast lookup without joins.
        """
        results = []
        floor = settings.gclid_capture_floor_pct

        for ns in MONITORED_NAMESPACES:
            col = ns["column"]
            try:
                rows = bq.run_query(f"""
                    SELECT
                        COUNTIF({col} IS NOT NULL AND {col} != '') AS with_signal,
                        COUNT(*)                                    AS total
                    FROM {bq.table_ref('sessions')}
                    WHERE session_start_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
                """)
                row = rows[0] if rows else {"with_signal": 0, "total": 0}
                total = int(row.get("total", 0))
                with_signal = int(row.get("with_signal", 0))
                rate = round((with_signal / max(total, 1)) * 100, 2)
            except Exception as exc:
                log.warning("watchdog.capture_rate_error", namespace=ns["namespace_id"], error=str(exc))
                total, with_signal, rate = 0, 0, 0.0

            results.append({
                "namespace_id":      ns["namespace_id"],
                "platform":          ns["platform"],
                "total_events":      total,
                "events_with_signal": with_signal,
                "capture_rate_pct":  rate,
                "threshold_pct":     floor,
                "ok":                rate >= floor or total == 0,
            })

        breaches = [r for r in results if not r["ok"] and r["total_events"] > 0]
        return {
            "hours_sampled": hours_back,
            "namespaces_checked": len(results),
            "breaches": len(breaches),
            "results": results,
        }

    def _tool_audit_crm_null_fields(self, since_hours: int = 1) -> dict:
        """
        Check the CRM leads staging table for missing media fields.
        Falls back to Salesforce API if the staging table isn't populated.
        """
        spike_threshold = settings.null_field_spike_pct

        # Try BigQuery staging table first (faster, doesn't hit SF API limits)
        try:
            rows = bq.run_query(f"""
                SELECT
                    COUNTIF(gclid IS NULL AND fbclid IS NULL AND li_fat_id IS NULL
                            AND ttclid IS NULL AND ga_client_id IS NULL) AS null_count,
                    COUNTIF(utm_source IS NULL OR utm_source = '')        AS null_utm,
                    COUNT(*)                                               AS total
                FROM {bq.table_ref('crm_leads_staging')}
                WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {since_hours} HOUR)
            """)
            row = rows[0] if rows else {"null_count": 0, "null_utm": 0, "total": 0}
            null_count = int(row.get("null_count", 0))
            null_utm   = int(row.get("null_utm", 0))
            total      = int(row.get("total", 0))
            source     = "bigquery_staging"
        except Exception:
            # Fall back to live Salesforce query
            try:
                missing = salesforce_client.get_leads_missing_media_fields(since_hours)
                from tools.salesforce_client import query as sf_query
                total_result = sf_query(f"SELECT COUNT() FROM Lead WHERE CreatedDate = LAST_N_HOURS:{since_hours}")
                total      = total_result[0].get("expr0", 1) if total_result else 1
                null_count = len(missing)
                null_utm   = 0
                source     = "salesforce_api"
            except Exception as exc:
                return {"error": str(exc), "source": "none"}

        null_pct = round((null_count / max(total, 1)) * 100, 2)
        return {
            "source":             source,
            "hours_sampled":      since_hours,
            "total_leads":        total,
            "null_media_ids":     null_count,
            "null_utm":           null_utm,
            "null_pct":           null_pct,
            "threshold_pct":      spike_threshold,
            "breach":             null_pct > spike_threshold and total > 0,
        }

    def _tool_write_alert(
        self,
        alert_type: str,
        severity: str,
        description: str,
        namespace_id: str | None = None,
        affected_platform: str | None = None,
        metric_name: str | None = None,
        metric_value: float | None = None,
        threshold_value: float | None = None,
        probable_cause: str | None = None,
        recommended_action: str | None = None,
    ) -> dict:
        alert_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()

        row = {
            "alert_id":           alert_id,
            "alert_type":         alert_type,
            "severity":           severity,
            "status":             "open",
            "affected_namespace": namespace_id,
            "affected_platform":  affected_platform,
            "metric_name":        metric_name,
            "metric_value":       metric_value,
            "threshold_value":    threshold_value,
            "baseline_value":     None,
            "variance_pct":       None,
            "description":        description,
            "probable_cause":     probable_cause,
            "recommended_action": recommended_action,
            "detected_at":        now,
            "resolved_at":        None,
            "alert_sent":         False,
            "run_id":             None,
            "context":            None,
        }

        # Write to BigQuery
        bq_errors = bq.insert_rows("watchdog_alerts", [row])
        bq_ok = len(bq_errors) == 0

        # Send webhook notification
        webhook_sent = False
        if settings.alert_webhook_url:
            emoji = ":red_circle:" if severity == "critical" else ":warning:"
            payload = {
                "text": (
                    f"{emoji} *Attribution Watchdog [{severity.upper()}]*\n"
                    f"*Type:* {alert_type}\n"
                    f"*Metric:* {metric_name or 'N/A'} = {metric_value:.1f}% "
                    f"(threshold: {threshold_value:.1f}%)\n"
                    f"*Description:* {description}\n"
                    f"*Probable cause:* {probable_cause or 'Unknown'}"
                    + (f"\n*Recommended action:* {recommended_action}" if recommended_action else "")
                )
            }
            try:
                resp = httpx.post(settings.alert_webhook_url, json=payload, timeout=10)
                webhook_sent = resp.status_code < 300
            except Exception as exc:
                log.warning("watchdog.webhook_failed", error=str(exc))

        log.info("watchdog.alert_written", alert_id=alert_id, severity=severity, type=alert_type, bq_ok=bq_ok)
        return {
            "alert_id":     alert_id,
            "written_to_bq": bq_ok,
            "webhook_sent": webhook_sent,
            "bq_errors":    bq_errors,
        }

    def _tool_log_capture_rates(
        self,
        measurements: list[dict],
        hours_back: int = 1,
    ) -> dict:
        floor = settings.gclid_capture_floor_pct
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for m in measurements:
            rate = float(m.get("capture_rate_pct", 0))
            rows.append({
                "log_id":                  bq.new_uuid(),
                "logged_at":               now,
                "measurement_window_hours": hours_back,
                "namespace_id":            m.get("namespace_id", "unknown"),
                "platform":                m.get("platform"),
                "total_events":            int(m.get("total_events", 0)),
                "events_with_signal":      int(m.get("events_with_signal", 0)),
                "capture_rate_pct":        rate,
                "baseline_capture_rate_pct": None,   # Analyst agent fills this via rolling avg
                "variance_from_baseline_pct": None,
                "is_anomaly":              rate < floor and int(m.get("total_events", 0)) > 0,
                "run_id":                  None,
            })

        errors = bq.insert_rows("watchdog_capture_rate_log", rows)
        return {"rows_logged": len(rows), "errors": len(errors)}
