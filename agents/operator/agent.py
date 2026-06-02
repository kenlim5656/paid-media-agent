# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
The Operator — Media Optimization Agent.
Runs daily after the Analyst. Reads attribution_channel_summary and acts on insights.
All write actions are logged to operator_action_log and operator_pending_approvals.
Platform-agnostic: supports GMP, Meta, LinkedIn, Google Ads, TikTok via platform adapters.
"""
import json
import textwrap
import structlog
from datetime import date, datetime, timezone

import anthropic

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
                "(DV360, Meta, LinkedIn, Google Ads, TikTok) to suppress top-of-funnel ads "
                "for accounts already in open pipeline. "
                "Google Ads: uses Customer Match — pass audience_list_id as the user_list "
                "resource_name (e.g. 'customers/123/userLists/456'). "
                "TikTok: uses Custom Audience — pass audience_list_id as the TikTok audience_id. "
                "Requires approval unless OPERATOR_REQUIRE_APPROVAL=false."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id":       {"type": "string", "description": "From log_proposed_action"},
                    "platform":        {
                        "type": "string",
                        "enum": ["dv360", "meta", "linkedin", "google_ads", "tiktok"],
                        "description": "Which platform to push the exclusion to",
                    },
                    "advertiser_id":    {"type": "string", "description": "Platform advertiser / customer ID"},
                    "audience_list_id": {
                        "type": "string",
                        "description": (
                            "Audience list ID. "
                            "DV360/Meta/LinkedIn: list/segment ID. "
                            "Google Ads: user_list resource_name (customers/{id}/userLists/{id}). "
                            "TikTok: custom audience_id (numeric string)."
                        ),
                    },
                    "domains": {
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
                "Move budget from an underperforming campaign to a high-performing one. "
                "Supports DV360, SA360, Meta, LinkedIn, Google Ads, and TikTok. "
                "Capped at max_budget_shift_pct per run. "
                "Google Ads: source_entity_id and target_entity_id are campaign IDs (numeric). "
                "TikTok: source_entity_id and target_entity_id are TikTok campaign IDs. "
                "Requires approval unless OPERATOR_REQUIRE_APPROVAL=false."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id":        {"type": "string", "description": "From log_proposed_action"},
                    "platform":         {
                        "type": "string",
                        "enum": ["dv360", "sa360", "meta", "linkedin", "google_ads", "tiktok"],
                    },
                    "advertiser_id":    {
                        "type": "string",
                        "description": "Platform advertiser ID. Google Ads: customer ID (digits only). TikTok: advertiser_id.",
                    },
                    "source_entity_id": {"type": "string", "description": "Campaign / line item to reduce"},
                    "target_entity_id": {"type": "string", "description": "Campaign / line item to increase"},
                    "amount_usd":       {"type": "number"},
                },
                "required": ["action_id", "platform", "advertiser_id", "source_entity_id", "target_entity_id", "amount_usd"],
            },
        },
        {
            "name": "generate_creative_campaign_brief",
            "description": (
                "Generate a full creative campaign package for one or more channels. "
                "Queries historical top-performing ad copy (by CVR/CTR) as few-shot context, "
                "then produces: (1) platform-ready text copy variants across PAS, AIDA, and "
                "Benefit-Driven frameworks, with hard character-limit validation enforced per "
                "platform; (2) production-grade visual creative briefs for each recommended "
                "asset format, including visual concept, aesthetic guidance, on-screen copy "
                "timeline, and AI image/video generation prompt. "
                "Returns a dual payload: structured JSON (text_copy_variants + "
                "visual_creative_briefs) for pipeline storage, and a formatted Markdown "
                "deployment package for the design team."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "channels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Target channel(s) to write copy for. "
                            "Supported: 'meta', 'linkedin', 'google_ads', 'tiktok', 'display'. "
                            "Example: ['meta', 'linkedin']"
                        ),
                    },
                    "value_proposition": {
                        "type": "string",
                        "description": (
                            "The core product value proposition — what the product does and "
                            "the primary outcome it delivers. Be specific: include the product "
                            "name, key capability, and measurable outcome if known. "
                            "Example: '[Product] auto-routes intent signals from your website "
                            "to Salesforce in real time, cutting MQL response time by 60%.'"
                        ),
                    },
                    "target_persona": {
                        "type": "string",
                        "description": (
                            "Target audience description. Include job title, company type/size, "
                            "and the primary pain point this campaign addresses. "
                            "Example: 'VP of Demand Generation at B2B SaaS companies (100-1000 "
                            "employees) who struggles with misaligned MQL-to-SQL handoffs.'"
                        ),
                    },
                    "copy_framework": {
                        "type": "string",
                        "enum": ["PAS", "AIDA", "benefit_driven", "all"],
                        "default": "all",
                        "description": (
                            "Which copywriting framework(s) to generate variants for. "
                            "'all' returns one variant per framework per channel."
                        ),
                    },
                    "lookback_days": {
                        "type": "integer",
                        "default": 90,
                        "description": "Days to look back when fetching historical top-performers for few-shot context.",
                    },
                    "rank_by": {
                        "type": "string",
                        "enum": ["cvr", "ctr", "roas", "attributed_cpa"],
                        "default": "cvr",
                        "description": "Metric to rank historical top-performers by.",
                    },
                    "campaign_objective": {
                        "type": "string",
                        "description": (
                            "Optional. Campaign goal context for brief alignment: "
                            "'lead_gen', 'demo_request', 'trial_signup', 'awareness', "
                            "'retargeting', 'upsell'. Defaults to 'lead_gen'."
                        ),
                    },
                    "brand_notes": {
                        "type": "string",
                        "description": (
                            "Optional. Any brand voice, tone constraints, or product-specific "
                            "language to apply (e.g. 'avoid using the word integration', "
                            "'our brand voice is authoritative but approachable')."
                        ),
                    },
                },
                "required": ["channels", "value_proposition", "target_persona"],
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
        from tools.google_ads_client import (
            GoogleAdsAPIError, GoogleAdsSetupError, push_domain_suppression as gads_push_suppression,
        )
        from tools.tiktok_ads_client import (
            TikTokAdsError, TikTokSetupError,
            push_domain_suppression as tiktok_push_suppression,
        )

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

            elif platform == "google_ads":
                # Google Ads Customer Match — audience_list_id is the user_list resource_name.
                # Domain-to-email mapping via CRM is preferred for higher match rates.
                # If no emails are pre-loaded, push_domain_suppression returns a manual fallback.
                result = gads_push_suppression(
                    customer_id=advertiser_id,
                    user_list_resource_name=audience_list_id,
                    domains=domains,
                    crm_emails_by_domain=None,  # TODO: wire CRM lookup in Task 22/24
                )

            elif platform == "tiktok":
                # TikTok Custom Audience — audience_list_id is the TikTok audience_id (numeric string).
                # Domain-to-email mapping via CRM is preferred (TikTok doesn't support raw domains).
                # If no CRM data, push_domain_suppression returns a manual fallback with instructions.
                result = tiktok_push_suppression(
                    advertiser_id=advertiser_id,
                    audience_id=audience_list_id,
                    domains=domains,
                    crm_emails_by_domain=None,  # TODO: wire CRM lookup in Task 22/24
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

        except (ApprovalRequiredError, MetaAPIError, LinkedInAPIError,
                GoogleAdsAPIError, GoogleAdsSetupError,
                TikTokAdsError, TikTokSetupError) as exc:
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
        from tools.google_ads_client import (
            GoogleAdsAPIError, GoogleAdsSetupError, GoogleAdsBudgetGuardrailError,
            reallocate_campaign_budget as gads_reallocate,
        )
        from tools.tiktok_ads_client import (
            TikTokAdsError, TikTokSetupError, TikTokBudgetGuardrailError,
            reallocate_campaign_budget as tiktok_reallocate,
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

            elif platform == "google_ads":
                # advertiser_id = Google Ads customer ID (digits only, no dashes)
                # source/target entity IDs = Google Ads campaign IDs (numeric strings)
                result = gads_reallocate(
                    customer_id=advertiser_id,
                    source_campaign_id=source_entity_id,
                    target_campaign_id=target_entity_id,
                    amount_usd=amount_usd,
                )

            elif platform == "tiktok":
                # advertiser_id = TikTok advertiser_id (numeric string)
                # source/target entity IDs = TikTok campaign IDs (numeric strings)
                result = tiktok_reallocate(
                    advertiser_id=advertiser_id,
                    source_campaign_id=source_entity_id,
                    target_campaign_id=target_entity_id,
                    amount_usd=amount_usd,
                )

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

        except (ApprovalRequiredError, MetaAPIError, LinkedInAPIError,
                GoogleAdsAPIError, GoogleAdsSetupError, GoogleAdsBudgetGuardrailError,
                TikTokAdsError, TikTokSetupError, TikTokBudgetGuardrailError,
                ValueError) as exc:
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

    # ── Creative Brief Tool ───────────────────────────────────────────────────

    def _tool_generate_creative_campaign_brief(
        self,
        channels: list[str],
        value_proposition: str,
        target_persona: str,
        copy_framework: str = "all",
        lookback_days: int = 90,
        rank_by: str = "cvr",
        campaign_objective: str | None = None,
        brand_notes: str | None = None,
    ) -> dict:
        """
        Full creative engine: queries historical performance data as few-shot context,
        then calls Claude to generate platform-validated copy variants and production
        visual creative briefs.

        Returns a dual payload:
          text_copy_variants    — list of per-channel, per-framework copy dicts
          visual_creative_briefs — list of asset production brief dicts
          markdown_summary       — formatted deployment package for the design team
          few_shot_count         — number of historical ads used as context
          top_format_by_cvr      — highest-CVR asset format from historical data
        """
        from tools.creative_insights_client import (
            get_top_performing_ads,
            get_asset_type_performance_correlation,
            format_few_shot_context,
            format_asset_correlation_context,
        )

        objective = campaign_objective or "lead_gen"

        # ── 1. Fetch historical context ─────────────────────────────────────
        top_ads: list[dict] = []
        asset_correlations: list[dict] = []
        try:
            top_ads = get_top_performing_ads(
                channels=channels,
                lookback_days=lookback_days,
                rank_by=rank_by,
                limit=20,
            )
            asset_correlations = get_asset_type_performance_correlation(
                channels=channels,
                lookback_days=lookback_days,
            )
            log.info(
                "operator.creative_brief.context_loaded",
                top_ad_count=len(top_ads),
                asset_format_count=len(asset_correlations),
            )
        except Exception as exc:
            log.warning("operator.creative_brief.context_fetch_failed", error=str(exc))

        few_shot_text  = format_few_shot_context(top_ads, max_examples=5)
        asset_ctx_text = format_asset_correlation_context(asset_correlations)
        top_format     = asset_correlations[0].get("creative_format", "unknown") if asset_correlations else "unknown"
        top_format_cvr = asset_correlations[0].get("avg_cvr", 0.0) if asset_correlations else 0.0

        # ── 2. Determine frameworks to generate ─────────────────────────────
        if copy_framework == "all":
            frameworks_requested = ["PAS", "AIDA", "benefit_driven"]
        else:
            frameworks_requested = [copy_framework]

        # ── 3. Build the generation prompt ──────────────────────────────────
        channels_str   = ", ".join(channels)
        framework_list = ", ".join(frameworks_requested)

        _PLATFORM_CONSTRAINTS = {
            "meta": {
                "primary_text_max": 125,
                "headline_max": 27,
                "description_max": 27,
                "fields": ["primary_text", "headline", "description"],
                "note": "Primary text > 125 chars is truncated in feed. Headline appears in link preview tile.",
            },
            "linkedin": {
                "primary_text_max": 150,
                "headline_max": 70,
                "description_max": 100,
                "fields": ["primary_text", "headline", "description"],
                "note": "Primary text > 150 chars requires 'see more' click. CTA must land before char 150.",
            },
            "google_ads": {
                "headline_max": 30,
                "description_max": 90,
                "fields": ["headlines", "descriptions"],
                "note": "RSA: provide 3 headline variants and 2 description variants. Each headline ≤ 30 chars. Each description ≤ 90 chars.",
            },
            "tiktok": {
                "ad_text_max": 100,
                "fields": ["ad_text"],
                "note": "Ad text appears as caption overlay. Keep scannable — one idea. ≤ 100 chars.",
            },
            "display": {
                "headline_max": 25,
                "description_max": 90,
                "fields": ["headline", "description"],
                "note": "Standard display: headline ≤ 25 chars, description ≤ 90 chars.",
            },
        }

        constraints_block = "\n".join(
            f"  {ch}: {json.dumps(_PLATFORM_CONSTRAINTS.get(ch, {'note': 'standard platform limits'}), indent=2)}"
            for ch in channels
        )

        brand_block = f"\nBrand voice / constraints:\n{brand_notes}\n" if brand_notes else ""

        generation_prompt = textwrap.dedent(f"""
            You are a senior direct-response copywriter and creative director.

            Generate a complete creative campaign package. Return ONLY a valid JSON object
            matching the schema below. No markdown, no explanation outside the JSON.

            === Campaign Brief ===
            Product / Value Proposition:
            {value_proposition}

            Target Persona:
            {target_persona}

            Campaign Objective: {objective}
            Target Channels: {channels_str}
            Frameworks requested: {framework_list}
            {brand_block}
            === Platform Constraints ===
            {constraints_block}

            === Historical Performance Context (few-shot) ===
            {few_shot_text}

            {asset_ctx_text}

            === Output JSON Schema ===
            {{
              "text_copy_variants": [
                {{
                  "framework": "PAS" | "AIDA" | "benefit_driven",
                  "channel": "<channel name>",
                  "primary_text": "<string — used for meta/linkedin/tiktok>",
                  "headline": "<string>",
                  "description": "<string>",
                  "headlines": ["<string>", ...],      // google_ads only — 3 variants
                  "descriptions": ["<string>", ...],   // google_ads only — 2 variants
                  "ad_text": "<string>",               // tiktok only
                  "char_validation": {{
                    "primary_text": <int>,
                    "headline": <int>,
                    "description": <int>,
                    "passes": true | false,
                    "violations": ["<field>: <N> chars exceeds limit of <M>"]
                  }}
                }}
                // one object per framework × channel combination
              ],
              "visual_creative_briefs": [
                {{
                  "brief_id": <int>,
                  "asset_type": "<e.g. 9:16 Vertical Video>",
                  "placement_targets": ["<platform/placement>", ...],
                  "aspect_ratio": "<e.g. 9:16>",
                  "duration_seconds": <int | null>,
                  "recommended_format": "video" | "image" | "carousel",
                  "visual_concept": "<psychological hook + visual metaphor in 2-3 sentences>",
                  "aesthetic_guidance": {{
                    "color_palette": "<primary, secondary, accent with intent>",
                    "lighting": "<hard directional | soft diffuse | high-contrast dramatic>",
                    "composition": "<minimalist | collage | screen-capture | lifestyle>",
                    "tone_markers": ["<adjective>", "<adjective>", "<adjective>"]
                  }},
                  "on_screen_copy": [
                    {{
                      "timestamp_or_zone": "<0:00-0:02 or Top 20%>",
                      "layer": "<Hook | Body | CTA | etc.>",
                      "copy": "<exact text>",
                      "style": "<Bold | Regular | Brand color BG>"
                    }}
                  ],
                  "ai_image_prompt": "<detailed generation prompt for Midjourney/DALL-E/Firefly>"
                }}
                // 2-3 briefs covering the highest-CVR formats from the performance data
              ]
            }}

            Rules:
            - Every copy variant MUST pass char_validation for its channel constraints.
            - Rewrite any field that would exceed the limit — never truncate mid-word.
            - Each framework variant must be structurally distinct — different hook, different proof, different CTA.
            - Headlines for Google RSA must all be ≤ 30 characters.
            - TikTok ad_text must be ≤ 100 characters.
            - LinkedIn primary_text must deliver the key message within the first 150 characters.
            - Do not plagiarise the few-shot examples — use them as structural pattern reference only.
            - visual_creative_briefs must be grounded in the best-performing format(s) from the asset correlation data.
            - The ai_image_prompt must be production-grade (camera angle, lighting, composition, style flags, --ar).
        """).strip()

        # ── 4. Claude sub-inference call ────────────────────────────────────
        generation_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        try:
            gen_response = generation_client.messages.create(
                model=settings.claude_model,
                max_tokens=8192,
                system=(
                    "You are a creative campaign generator. "
                    "Output ONLY a valid JSON object. No preamble, no markdown fences, "
                    "no explanation. Start your response with '{' and end with '}'."
                ),
                messages=[{"role": "user", "content": generation_prompt}],
            )
            raw_json = gen_response.content[0].text.strip()
            # Strip accidental markdown fences if the model adds them
            if raw_json.startswith("```"):
                raw_json = raw_json.split("\n", 1)[1]
                raw_json = raw_json.rsplit("```", 1)[0].strip()
            structured = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            log.error("operator.creative_brief.json_parse_failed", error=str(exc))
            return {
                "ok": False,
                "error": f"JSON parse failed: {exc}",
                "raw_response": raw_json[:500] if "raw_json" in dir() else "(no response)",
            }
        except Exception as exc:
            log.error("operator.creative_brief.generation_failed", error=str(exc))
            return {"ok": False, "error": str(exc)}

        text_variants = structured.get("text_copy_variants", [])
        visual_briefs = structured.get("visual_creative_briefs", [])

        # ── 5. Build Markdown deployment package ────────────────────────────
        markdown_summary = _build_creative_brief_markdown(
            channels=channels,
            value_proposition=value_proposition,
            target_persona=target_persona,
            objective=objective,
            text_variants=text_variants,
            visual_briefs=visual_briefs,
            few_shot_count=len(top_ads),
            lookback_days=lookback_days,
            rank_by=rank_by,
            top_format=top_format,
            top_format_cvr=top_format_cvr,
        )

        log.info(
            "operator.creative_brief.generated",
            channels=channels,
            frameworks=frameworks_requested,
            copy_variants=len(text_variants),
            visual_briefs=len(visual_briefs),
            few_shot_count=len(top_ads),
        )

        return {
            "ok": True,
            "text_copy_variants":   text_variants,
            "visual_creative_briefs": visual_briefs,
            "markdown_summary":     markdown_summary,
            "few_shot_count":       len(top_ads),
            "top_format_by_cvr":    top_format,
            "top_format_cvr":       top_format_cvr,
            "channels":             channels,
            "frameworks_generated": frameworks_requested,
        }


# ── Markdown builder (module-level, pure formatting) ─────────────────────────

_FRAMEWORK_LABELS = {
    "PAS":           "PAS — Problem · Agitate · Solution",
    "AIDA":          "AIDA — Attention · Interest · Desire · Action",
    "benefit_driven": "Benefit-Driven Direct Hook",
}

_CHANNEL_FIELDS: dict[str, list[str]] = {
    "meta":       ["primary_text", "headline", "description"],
    "linkedin":   ["primary_text", "headline", "description"],
    "google_ads": ["headlines", "descriptions"],
    "tiktok":     ["ad_text"],
    "display":    ["headline", "description"],
}

_CHAR_LIMITS: dict[str, dict[str, int]] = {
    "meta":       {"primary_text": 125, "headline": 27, "description": 27},
    "linkedin":   {"primary_text": 150, "headline": 70, "description": 100},
    "google_ads": {"headline": 30, "description": 90},
    "tiktok":     {"ad_text": 100},
    "display":    {"headline": 25, "description": 90},
}


def _char_status(text: str, limit: int) -> str:
    """Return '✅' if within limit, '⚠️ OVER' if not."""
    n = len(text or "")
    return f"{n}/{limit} ✅" if n <= limit else f"{n}/{limit} ⚠️ OVER"


def _build_copy_matrix_section(text_variants: list[dict], channels: list[str]) -> str:
    """Render the copy matrix section of the Markdown brief."""
    lines: list[str] = ["## 📝 Copy Matrix\n"]

    # Group variants by framework, then by channel
    from collections import defaultdict
    by_framework: dict[str, dict[str, dict]] = defaultdict(dict)
    for v in text_variants:
        fw = v.get("framework", "unknown")
        ch = v.get("channel", "unknown")
        by_framework[fw][ch] = v

    framework_order = ["PAS", "AIDA", "benefit_driven"]
    for fw in framework_order:
        if fw not in by_framework:
            continue
        fw_label = _FRAMEWORK_LABELS.get(fw, fw)
        lines.append(f"### Framework: {fw_label}\n")

        for ch in channels:
            v = by_framework[fw].get(ch)
            if not v:
                continue
            ch_display = ch.replace("_", " ").title()
            lines.append(f"#### {ch_display}\n")
            limits = _CHAR_LIMITS.get(ch, {})

            if ch == "google_ads":
                headlines = v.get("headlines") or []
                descs     = v.get("descriptions") or []
                lines.append("**Headlines** (≤ 30 chars each):\n")
                for i, hl in enumerate(headlines, 1):
                    status = _char_status(hl, 30)
                    lines.append(f"{i}. \"{hl}\" — {status}")
                lines.append("\n**Descriptions** (≤ 90 chars each):\n")
                for i, d in enumerate(descs, 1):
                    status = _char_status(d, 90)
                    lines.append(f"{i}. \"{d}\" — {status}")
            elif ch == "tiktok":
                ad_text = v.get("ad_text", "")
                status  = _char_status(ad_text, limits.get("ad_text", 100))
                lines += [
                    "| Field | Copy | Length |",
                    "|-------|------|--------|",
                    f"| Ad Text | {ad_text} | {status} |",
                ]
            else:
                # meta, linkedin, display
                rows = []
                for field in _CHANNEL_FIELDS.get(ch, []):
                    val    = v.get(field, "")
                    lim    = limits.get(field, 9999)
                    status = _char_status(val, lim)
                    label  = field.replace("_", " ").title()
                    rows.append(f"| {label} | {val} | {status} |")
                lines += [
                    "| Field | Copy | Length |",
                    "|-------|------|--------|",
                ] + rows

            violations = (v.get("char_validation") or {}).get("violations", [])
            if violations:
                lines.append(f"\n> ⚠️ **Violations:** {'; '.join(violations)}")
            lines.append("")  # blank line between channels

        lines.append("---\n")

    return "\n".join(lines)


def _build_visual_briefs_section(visual_briefs: list[dict]) -> str:
    """Render the visual creative briefs section as blockquotes."""
    if not visual_briefs:
        return "## 🎨 Visual Creative Briefs\n\n*(No briefs generated.)*\n"

    lines: list[str] = ["## 🎨 Visual Creative Briefs\n"]

    for brief in visual_briefs:
        bid    = brief.get("brief_id", "?")
        atype  = brief.get("asset_type", "Unknown")
        ratio  = brief.get("aspect_ratio", "")
        dur    = brief.get("duration_seconds")
        places = ", ".join(brief.get("placement_targets") or [])
        dur_str = f" · {dur}s" if dur else ""

        lines.append(f"> ### Brief {bid}: {atype} — {places}")
        lines.append(f"> **Format:** {ratio}{dur_str} · {brief.get('recommended_format', 'TBD')}")
        lines.append(">")

        concept = brief.get("visual_concept", "")
        if concept:
            lines.append(f"> **Visual Concept:**")
            lines.append(f"> {concept}")
            lines.append(">")

        aesthetic = brief.get("aesthetic_guidance") or {}
        if aesthetic:
            lines.append("> **Aesthetic Guidance:**")
            for k, v in aesthetic.items():
                label = k.replace("_", " ").title()
                if isinstance(v, list):
                    v = " · ".join(v)
                lines.append(f"> - **{label}:** {v}")
            lines.append(">")

        osc = brief.get("on_screen_copy") or []
        if osc:
            lines.append("> **On-Screen Copy:**")
            lines.append(">")
            lines.append("> | Timestamp / Zone | Layer | Copy | Style |")
            lines.append("> |-------------------|-------|------|-------|")
            for row in osc:
                ts    = row.get("timestamp_or_zone", "")
                layer = row.get("layer", "")
                copy  = row.get("copy", "")
                style = row.get("style", "")
                lines.append(f"> | {ts} | {layer} | {copy} | {style} |")
            lines.append(">")

        ai_prompt = brief.get("ai_image_prompt", "")
        if ai_prompt:
            lines.append("> **AI Generation Prompt:**")
            lines.append("> ```")
            # Wrap long prompts for readability inside the blockquote
            for chunk in textwrap.wrap(ai_prompt, width=90):
                lines.append(f"> {chunk}")
            lines.append("> ```")

        lines.append("")  # blank line between briefs

    return "\n".join(lines)


def _build_creative_brief_markdown(
    channels: list[str],
    value_proposition: str,
    target_persona: str,
    objective: str,
    text_variants: list[dict],
    visual_briefs: list[dict],
    few_shot_count: int,
    lookback_days: int,
    rank_by: str,
    top_format: str,
    top_format_cvr: float,
) -> str:
    """
    Assemble the full Markdown deployment package from structured data.
    Deterministic — does not call Claude. Pure string formatting.
    """
    channels_display = " · ".join(ch.replace("_", " ").title() for ch in channels)
    today = date.today().isoformat()

    header = textwrap.dedent(f"""\
        ## 🎯 Creative Campaign Brief — {channels_display}

        | | |
        |-|-|
        | **Target Persona** | {target_persona} |
        | **Objective** | {objective.replace("_", " ").title()} |
        | **Value Proposition** | {value_proposition[:100]}{"..." if len(value_proposition) > 100 else ""} |
        | **Generated** | {today} |
        | **Few-shot context** | {few_shot_count} historical top-performers ({lookback_days}-day lookback, ranked by {rank_by}) |
        | **Top format by CVR** | {top_format} ({top_format_cvr * 100:.2f}% CVR) |

        ---
    """)

    copy_section   = _build_copy_matrix_section(text_variants, channels)
    visual_section = _build_visual_briefs_section(visual_briefs)

    footer = textwrap.dedent(f"""\
        ---

        *Brief generated by the Paid Media Agent creative engine.*
        *Platform constraints enforced: Google RSA ≤ 30/90 chars · LinkedIn ≤ 150 chars (primary) · Meta ≤ 125/27 chars · TikTok ≤ 100 chars.*
        *Visual briefs follow the Multi-Asset Creative Brief Framework (see `agents/operator/skills/copy_assistant.md`).*
    """)

    return f"{header}\n{copy_section}\n{visual_section}\n{footer}"
