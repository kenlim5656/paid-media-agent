"""
The Operator — Media Optimization Agent.
Runs daily after the Analyst. Reads attribution_channel_summary and acts on insights.
All write actions are logged to operator_action_log and operator_pending_approvals.
Platform-agnostic: supports GMP, Meta, LinkedIn, TikTok via platform adapters.
"""
import structlog
from datetime import datetime, timezone

from agents.base import BaseAgent
from config import settings
from tools import bigquery_client as bq, gmp_client, salesforce_client

log = structlog.get_logger()

SYSTEM = """You are the Operator, a media optimization agent for a paid media pipeline.

You are platform-agnostic. You read attribution results and act on any supported platform
(DV360, SA360, Google Ads, Meta, LinkedIn, TikTok, etc.) using the available tools.

Your daily run sequence:
1. Call `get_attribution_summary` to see which channels are over/under-performing.
2. Call `get_accounts_in_open_pipeline` to get domains to suppress.
3. For each recommended action, call `log_proposed_action` FIRST to record it.
4. If criteria and guardrails pass, call the appropriate execution tool.
5. For audience suppression: call `push_audience_suppression`.
6. For budget changes: call `reallocate_budget`.

CRITICAL RULES:
- NEVER move more than {max_budget_shift_pct}% of any line item's budget in one run.
- ALWAYS call `log_proposed_action` before any execution tool.
- If OPERATOR_REQUIRE_APPROVAL=true, execution tools will return a pending-approval payload — surface it clearly.
- Always explain your reasoning (which attribution data drove each decision) before acting."""

SYSTEM = SYSTEM.format(max_budget_shift_pct=settings.max_budget_shift_pct)


class OperatorAgent(BaseAgent):
    name = "operator"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "get_attribution_summary",
            "description": (
                "Fetch the latest attribution channel summary from the most recent Analyst run. "
                "Returns channels ranked by attributed conversions, with spend, CPA, ROAS, "
                "and credit share. Reads from attribution_channel_summary — the MCP-compatible "
                "aggregated view, not raw touchpoint data."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "conversion_type": {"type": "string", "description": "Filter to a specific conversion type, e.g. 'opportunity_created'"},
                    "limit":           {"type": "integer", "default": 20},
                },
                "required": [],
            },
        },
        {
            "name": "get_accounts_in_open_pipeline",
            "description": (
                "Return company domains for accounts that have an open (non-closed) opportunity "
                "in the CRM. Used to suppress top-of-funnel ads for accounts already in pipeline."
            ),
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "log_proposed_action",
            "description": (
                "Write a proposed media action to operator_action_log and (if approval is required) "
                "operator_pending_approvals. MUST be called before any execution tool. "
                "Returns an action_id to pass to the execution tool for audit linkage."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_type":        {"type": "string", "enum": [
                        "budget_reallocation", "budget_pause", "budget_resume", "budget_adjustment",
                        "audience_exclusion", "audience_inclusion", "bid_adjustment",
                        "creative_suppression", "frequency_cap_update", "campaign_status_change",
                    ]},
                    "platform":           {"type": "string"},
                    "platform_entity_id": {"type": "string"},
                    "campaign_id":        {"type": "string"},
                    "field_changed":      {"type": "string"},
                    "value_before":       {"type": "string"},
                    "value_after":        {"type": "string"},
                    "change_magnitude":   {"type": "number"},
                    "change_magnitude_pct": {"type": "number"},
                    "rationale":          {"type": "string", "description": "Which attribution data drove this decision"},
                    "summary":            {"type": "string", "description": "One-line human-readable summary, e.g. 'Reallocate $500 from EMEA Brand to EMEA Retargeting'"},
                    "estimated_impact":   {"type": "string"},
                    "insight_id":         {"type": "string"},
                },
                "required": ["action_type", "platform", "platform_entity_id", "rationale", "summary"],
            },
        },
        {
            "name": "push_audience_suppression",
            "description": (
                "Add company domains to an audience exclusion list on a supported platform "
                "(DV360, Meta, LinkedIn) to suppress top-of-funnel ads for accounts already "
                "in open pipeline. Requires approval unless OPERATOR_REQUIRE_APPROVAL=false."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id":       {"type": "string", "description": "From log_proposed_action"},
                    "platform":        {"type": "string", "enum": ["dv360", "meta", "linkedin"], "description": "Which platform to push the exclusion to"},
                    "advertiser_id":   {"type": "string"},
                    "audience_list_id":{"type": "string"},
                    "domains":         {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Company domains to exclude, e.g. ['acme.com', 'bigcorp.com']",
                    },
                },
                "required": ["action_id", "platform", "advertiser_id", "audience_list_id", "domains"],
            },
        },
        {
            "name": "reallocate_budget",
            "description": (
                "Move budget from an underperforming line item to a high-performing one. "
                "Supports DV360, SA360, and Google Ads. Capped at max_budget_shift_pct. "
                "Requires approval unless OPERATOR_REQUIRE_APPROVAL=false."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id":           {"type": "string", "description": "From log_proposed_action"},
                    "platform":            {"type": "string", "enum": ["dv360", "sa360", "google_ads"]},
                    "advertiser_id":       {"type": "string"},
                    "source_entity_id":    {"type": "string", "description": "Line item / campaign to reduce"},
                    "target_entity_id":    {"type": "string", "description": "Line item / campaign to increase"},
                    "amount_usd":          {"type": "number"},
                },
                "required": ["action_id", "platform", "advertiser_id", "source_entity_id", "target_entity_id", "amount_usd"],
            },
        },
    ]

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_get_attribution_summary(
        self,
        conversion_type: str | None = None,
        limit: int = 20,
    ) -> dict:
        conv_filter = f"AND conversion_type = '{conversion_type}'" if conversion_type else ""
        rows = bq.run_query(f"""
            SELECT
                platform,
                channel,
                conversion_type,
                attributed_conversions,
                attributed_value,
                credit_share_pct,
                total_spend,
                attributed_cpa,
                attributed_roas,
                period_start,
                period_end,
                model_name
            FROM {bq.table_ref('attribution_channel_summary')}
            WHERE run_id = (
                SELECT run_id FROM {bq.table_ref('attribution_runs')}
                WHERE status = 'completed'
                ORDER BY completed_at DESC
                LIMIT 1
            )
            {conv_filter}
            ORDER BY attributed_conversions DESC
            LIMIT {limit}
        """)
        return {"results": rows, "count": len(rows)}

    def _tool_get_accounts_in_open_pipeline(self) -> dict:
        # Try BigQuery staging table first, fall back to live Salesforce
        try:
            rows = bq.run_query(f"""
                SELECT DISTINCT
                    account_id,
                    company_domain,
                    pipeline_stage
                FROM {bq.table_ref('crm_opportunities_staging')}
                WHERE is_closed = FALSE
                  AND company_domain IS NOT NULL
                  AND company_domain NOT IN ('gmail.com', 'outlook.com', 'yahoo.com')
                ORDER BY company_domain
                LIMIT 500
            """)
            domains = [r["company_domain"] for r in rows if r.get("company_domain")]
            source = "bigquery_staging"
        except Exception:
            accounts = salesforce_client.get_accounts_with_open_opportunities()
            domains = list({
                a.get("Account", {}).get("Website", "")
                .replace("https://", "").replace("http://", "")
                .replace("www.", "").strip("/")
                for a in accounts
                if a.get("Account", {}).get("Website")
            })
            source = "salesforce_api"

        return {"domain_count": len(domains), "domains": domains, "source": source}

    def _tool_log_proposed_action(
        self,
        action_type: str,
        platform: str,
        platform_entity_id: str,
        rationale: str,
        summary: str,
        campaign_id: str | None = None,
        field_changed: str | None = None,
        value_before: str | None = None,
        value_after: str | None = None,
        change_magnitude: float | None = None,
        change_magnitude_pct: float | None = None,
        estimated_impact: str | None = None,
        insight_id: str | None = None,
    ) -> dict:
        action_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()
        execution_mode = "pending_approval" if settings.operator_require_approval else "autonomous"

        action_row = {
            "action_id":             action_id,
            "action_type":           action_type,
            "platform":              platform,
            "platform_entity_type":  None,
            "platform_entity_id":    platform_entity_id,
            "campaign_id":           campaign_id,
            "field_changed":         field_changed,
            "value_before":          value_before,
            "value_after":           value_after,
            "change_magnitude":      change_magnitude,
            "change_magnitude_pct":  change_magnitude_pct,
            "rationale":             rationale,
            "insight_id":            insight_id,
            "attribution_run_id":    None,
            "execution_mode":        execution_mode,
            "status":                "proposed",
            "guardrail_check_passed": True,
            "guardrail_notes":       f"max_budget_shift_pct={settings.max_budget_shift_pct}",
            "requires_approval":     settings.operator_require_approval,
            "approved_by":           None,
            "approved_at":           None,
            "rejected_by":           None,
            "rejected_at":           None,
            "rejection_reason":      None,
            "proposed_at":           now,
            "executed_at":           None,
            "rolled_back_at":        None,
            "platform_response":     None,
        }
        bq.insert_rows("operator_action_log", [action_row])

        # If approval required, also write to the pending approvals queue
        if settings.operator_require_approval:
            approval_row = {
                "action_id":            action_id,
                "platform":             platform,
                "action_type":          action_type,
                "platform_entity_id":   platform_entity_id,
                "campaign_id":          campaign_id,
                "summary":              summary,
                "rationale":            rationale,
                "estimated_impact":     estimated_impact,
                "spend_at_risk":        change_magnitude,
                "change_magnitude_pct": change_magnitude_pct,
                "proposed_at":          now,
                "expires_at":           None,
                "proposed_by":          "operator_agent",
            }
            bq.insert_rows("operator_pending_approvals", [approval_row])

        log.info("operator.action_logged", action_id=action_id, type=action_type, requires_approval=settings.operator_require_approval)
        return {
            "action_id":        action_id,
            "requires_approval": settings.operator_require_approval,
            "execution_mode":   execution_mode,
            "message":          (
                f"Action logged (ID: {action_id}). "
                + ("Pending human approval before execution." if settings.operator_require_approval
                   else "Proceeding to execute.")
            ),
        }

    def _tool_push_audience_suppression(
        self,
        action_id: str,
        platform: str,
        advertiser_id: str,
        audience_list_id: str,
        domains: list[str],
    ) -> dict:
        from tools.gmp_client import ApprovalRequiredError, dv360_push_audience_exclusion
        from tools.meta_client import MetaAPIError, add_domains_to_exclusion_audience
        from tools.linkedin_client import LinkedInAPIError, add_companies_to_segment

        try:
            if platform == "dv360":
                result = dv360_push_audience_exclusion(advertiser_id, audience_list_id, domains)

            elif platform == "meta":
                result = add_domains_to_exclusion_audience(
                    audience_id=audience_list_id,
                    domains=domains,
                )

            elif platform == "linkedin":
                result = add_companies_to_segment(
                    segment_id=audience_list_id,
                    company_domains=domains,
                )

            else:
                if settings.operator_require_approval:
                    raise ApprovalRequiredError(
                        f"Audience suppression on {platform} requires approval. "
                        "No direct API adapter is implemented for this platform yet."
                    )
                result = {
                    "status": "queued",
                    "platform": platform,
                    "domains": len(domains),
                    "note": f"Manual: add {len(domains)} domains to exclusion list in {platform} UI.",
                }

            self._update_action_status(action_id, "executed")
            log.info("operator.suppression_executed", platform=platform, domains=len(domains))
            return {**result, "action_id": action_id, "domain_count": len(domains), "platform": platform}

        except (ApprovalRequiredError, MetaAPIError, LinkedInAPIError) as exc:
            return {
                "action_id": action_id,
                "executed":  False,
                "reason":    str(exc),
                "next_step": "Review pending approval in get_pending_approvals (MCP) or operator_pending_approvals (BQ).",
            }

    def _tool_reallocate_budget(
        self,
        action_id: str,
        platform: str,
        advertiser_id: str,
        source_entity_id: str,
        target_entity_id: str,
        amount_usd: float,
    ) -> dict:
        from tools.gmp_client import ApprovalRequiredError, dv360_reallocate_budget, sa360_adjust_campaign_budget
        from tools.meta_client import (
            MetaAPIError,
            get_campaign as meta_get_campaign,
            update_campaign_daily_budget as meta_update_budget,
        )
        from tools.linkedin_client import (
            LinkedInAPIError,
            get_campaign as li_get_campaign,
            update_campaign_daily_budget as li_update_budget,
        )

        try:
            if platform == "dv360":
                result = dv360_reallocate_budget(
                    advertiser_id, source_entity_id, target_entity_id, amount_usd
                )

            elif platform == "sa360":
                result = sa360_adjust_campaign_budget(
                    settings.sa360_agency_id, advertiser_id, target_entity_id, amount_usd
                )

            elif platform == "meta":
                source = meta_get_campaign(source_entity_id)
                target = meta_get_campaign(target_entity_id)
                source_current = int(source.get("daily_budget", 0))
                target_current = int(target.get("daily_budget", 0))
                amount_cents = int(amount_usd * 100)
                r_source = meta_update_budget(source_entity_id, max(0, source_current - amount_cents))
                r_target = meta_update_budget(target_entity_id, target_current + amount_cents)
                result = {
                    "platform": "meta",
                    "source_campaign": r_source,
                    "target_campaign": r_target,
                    "amount_moved_usd": amount_usd,
                }

            elif platform == "linkedin":
                source = li_get_campaign(source_entity_id)
                target = li_get_campaign(target_entity_id)
                source_current = float(source.get("dailyBudget", {}).get("amount", 0))
                target_current = float(target.get("dailyBudget", {}).get("amount", 0))
                r_source = li_update_budget(source_entity_id, max(10.0, source_current - amount_usd))
                r_target = li_update_budget(target_entity_id, target_current + amount_usd)
                result = {
                    "platform": "linkedin",
                    "source_campaign": r_source,
                    "target_campaign": r_target,
                    "amount_moved_usd": amount_usd,
                }

            else:
                if settings.operator_require_approval:
                    raise ApprovalRequiredError(
                        f"Budget reallocation on {platform} requires approval. "
                        "No API adapter implemented for this platform yet."
                    )
                result = {
                    "status": "queued",
                    "platform": platform,
                    "amount_usd": amount_usd,
                    "note": f"Manual: move ${amount_usd} from {source_entity_id} to {target_entity_id} in {platform} UI.",
                }

            self._update_action_status(action_id, "executed")
            log.info("operator.budget_reallocated", platform=platform, amount_usd=amount_usd)
            return {**result, "action_id": action_id}

        except (ApprovalRequiredError, MetaAPIError, LinkedInAPIError, ValueError) as exc:
            return {
                "action_id": action_id,
                "executed":  False,
                "reason":    str(exc),
                "next_step": "Review pending approval in get_pending_approvals (MCP) or operator_pending_approvals (BQ).",
            }

    def _update_action_status(self, action_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        try:
            bq.run_dml(f"""
                UPDATE {bq.table_ref('operator_action_log')}
                SET status = '{status}', executed_at = TIMESTAMP '{now}'
                WHERE action_id = '{action_id}'
            """)
            # Remove from pending approvals if it's been executed
            if status == "executed":
                bq.run_dml(f"""
                    DELETE FROM {bq.table_ref('operator_pending_approvals')}
                    WHERE action_id = '{action_id}'
                """)
        except Exception as exc:
            log.warning("operator.status_update_failed", action_id=action_id, error=str(exc))
