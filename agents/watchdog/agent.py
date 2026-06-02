"""
The Watchdog — Data Governance Agent.
Runs hourly. Audits GTM capture rates and Salesforce null-field spikes.
"""
import httpx
from agents.base import BaseAgent
from config import settings
from tools import gtm_client, salesforce_client


SYSTEM = """You are the Watchdog, a data governance agent for a B2B paid media attribution system.

Your job is to silently monitor data quality across the CM360 → GA4 → Salesforce → BigQuery pipeline.
On each run:
1. Check gclid and client_id capture rates from GTM logs.
2. Check the rate of null media fields on recent Salesforce Leads.
3. If any metric is outside acceptable thresholds, call `send_alert` with a clear diagnosis.
4. End with a brief status summary: GREEN / YELLOW / RED.

You are not a chatbot. You run on a cron schedule. Be terse and diagnostic."""


class WatchdogAgent(BaseAgent):
    name = "watchdog"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "audit_gtm_capture_rates",
            "description": "Check the gclid and client_id capture rates in GTM server-side logs for the past N hours.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "hours_back": {"type": "integer", "default": 1, "description": "Hours of history to analyze"}
                },
                "required": [],
            },
        },
        {
            "name": "audit_salesforce_null_fields",
            "description": "Fetch recent Salesforce Leads missing gclid or ga_client_id. Returns count and percentage.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "since_hours": {"type": "integer", "default": 1}
                },
                "required": [],
            },
        },
        {
            "name": "send_alert",
            "description": "Send an alert to the configured webhook when a threshold breach is detected.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "severity": {"type": "string", "enum": ["warning", "critical"]},
                    "metric": {"type": "string", "description": "Which metric breached"},
                    "current_value": {"type": "number"},
                    "threshold": {"type": "number"},
                    "diagnosis": {"type": "string", "description": "Plain-English explanation of the probable cause"},
                },
                "required": ["severity", "metric", "current_value", "threshold", "diagnosis"],
            },
        },
    ]

    def _tool_audit_gtm_capture_rates(self, hours_back: int = 1) -> dict:
        gclid = gtm_client.get_gclid_capture_rate(hours_back)
        client_id = gtm_client.get_client_id_capture_rate(hours_back)
        floor = settings.gclid_capture_floor_pct
        return {
            "gclid_capture_rate_pct": gclid["capture_rate_pct"],
            "client_id_capture_rate_pct": client_id["capture_rate_pct"],
            "threshold_pct": floor,
            "gclid_ok": gclid["capture_rate_pct"] >= floor,
            "client_id_ok": client_id["capture_rate_pct"] >= floor,
            "total_requests_sampled": gclid["total"],
        }

    def _tool_audit_salesforce_null_fields(self, since_hours: int = 1) -> dict:
        missing = salesforce_client.get_leads_missing_media_fields(since_hours)
        from tools.salesforce_client import query
        total_sql = f"SELECT COUNT() FROM Lead WHERE CreatedDate = LAST_N_HOURS:{since_hours}"
        total_rows = query(total_sql)
        total = total_rows[0].get("expr0", 1) if total_rows else 1
        null_count = len(missing)
        null_pct = (null_count / max(total, 1)) * 100
        spike = settings.null_field_spike_pct
        return {
            "null_media_field_count": null_count,
            "total_leads": total,
            "null_pct": round(null_pct, 2),
            "threshold_pct": spike,
            "breach": null_pct > spike,
            "sample_missing_ids": [r["Id"] for r in missing[:5]],
        }

    def _tool_send_alert(
        self,
        severity: str,
        metric: str,
        current_value: float,
        threshold: float,
        diagnosis: str,
    ) -> dict:
        if not settings.alert_webhook_url:
            return {"sent": False, "reason": "ALERT_WEBHOOK_URL not configured"}
        payload = {
            "text": (
                f":{'red' if severity == 'critical' else 'warning'}_circle: "
                f"*Attribution Watchdog [{severity.upper()}]*\n"
                f"*Metric:* {metric}\n"
                f"*Value:* {current_value:.1f}% (threshold: {threshold:.1f}%)\n"
                f"*Diagnosis:* {diagnosis}"
            )
        }
        resp = httpx.post(settings.alert_webhook_url, json=payload, timeout=10)
        return {"sent": True, "status_code": resp.status_code}
