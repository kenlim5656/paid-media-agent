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
import hashlib
import json
import textwrap
from datetime import date, datetime, timezone

import anthropic
import structlog

from agents.base import BaseAgent
from config import settings
from tools import bigquery_client as bq
from tools import salesforce_client

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
                        "enum": ["dv360", "meta", "linkedin", "google_ads", "tiktok", "reddit_ads"],
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
                        "enum": ["dv360", "sa360", "meta", "linkedin", "google_ads", "tiktok", "reddit_ads"],
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
        {
            "name": "sync_evolving_lookalike_seeds",
            "description": (
                "Detect ICP drift in closed-won CRM revenue and retrain lookalike seed "
                "audiences across Meta, Google Ads, TikTok, and Reddit Ads in a single run. "
                "\n\n"
                "How it works:\n"
                "  1. Reads v_lookalike_mutation_seed — a rolling 60-day cohort of closed-won "
                "accounts in the top-25% ARR tier, enriched with firmographic over-index scores "
                "(which industries / company sizes / regions are disproportionately driving "
                "revenue vs top-of-funnel lead mix).\n"
                "  2. SHA-256 hashes all seed emails (raw emails are never stored).\n"
                "  3. Pushes the hashed seed to each configured platform audience synchronously.\n"
                "  4. Logs each push to audience_mutation_logs in BigQuery.\n"
                "  5. Returns a dual payload: internal result dict + Markdown analysis table "
                "showing firmographic shifts and per-platform upload confirmation.\n"
                "\n"
                "Platform audience IDs:\n"
                "  meta:       Custom Audience ID (numeric string)\n"
                "  google_ads: UserList resource_name (customers/{id}/userLists/{id})\n"
                "  tiktok:     DMP Custom Audience ID (numeric string)\n"
                "  reddit_ads: Audience ID from POST /api/v3/audiences\n"
                "\n"
                "Requires OPERATOR_REQUIRE_APPROVAL approval gate before upload (each platform "
                "client enforces this independently via _require_approval())."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "platform_configs": {
                        "type": "array",
                        "description": (
                            "List of platform audience targets to hydrate in this run. "
                            "Each entry must specify: platform, advertiser_id, audience_id. "
                            "You may target all four platforms or a subset."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "platform": {
                                    "type": "string",
                                    "enum": ["meta", "google_ads", "tiktok", "reddit_ads"],
                                    "description": "Ad platform to push the seed to.",
                                },
                                "advertiser_id": {
                                    "type": "string",
                                    "description": (
                                        "Platform-specific advertiser/account ID. "
                                        "Meta: leave blank (uses settings.meta_ad_account_id). "
                                        "Google Ads: customer_id (digits only, no dashes). "
                                        "TikTok: advertiser_id (numeric string). "
                                        "Reddit Ads: ad account ID (t2_xxx or a2_xxx)."
                                    ),
                                },
                                "audience_id": {
                                    "type": "string",
                                    "description": (
                                        "Platform audience / list ID to hydrate with seed emails. "
                                        "Meta/TikTok/Reddit: numeric audience ID. "
                                        "Google Ads: user_list resource_name "
                                        "(e.g. 'customers/123/userLists/456')."
                                    ),
                                },
                            },
                            "required": ["platform", "audience_id"],
                        },
                        "minItems": 1,
                    },
                    "seed_limit": {
                        "type": "integer",
                        "default": 10000,
                        "description": (
                            "Maximum unique emails to push per platform audience. "
                            "Default: 10,000 (safe for initial runs). "
                            "Increase for mature pipelines with larger seed pools. "
                            "Set to 0 or omit to use the full cohort (no cap)."
                        ),
                    },
                    "run_label": {
                        "type": "string",
                        "description": (
                            "Optional human-readable label for this mutation run, "
                            "e.g. 'Q2 ICP refresh — post-series-B cohort'. "
                            "Stored in the Markdown summary header."
                        ),
                    },
                },
                "required": ["platform_configs"],
            },
        },
        {
            "name": "execute_system_budget_reallocation",
            "description": (
                "Ingest a task27.v1 MMM optimization package (from run_marketing_mix_optimization) "
                "and execute the channel budget adjustments across all configured ad platforms "
                "(Google Ads, TikTok, Meta, LinkedIn, Reddit Ads) in a single orchestrated run.\n\n"
                "Pre-flight guardrail checks enforced before any mutation:\n"
                "  • operator_approval_required flag must be present and True in the package.\n"
                "  • No individual recommended_shift_pct may exceed the package's max_shift_pct_policy (±10.0%).\n"
                "  • new_target_budget_usd must be ≥ platform minimum floor "
                "($5.00 Google Ads, $20.00 TikTok).\n"
                "  • All channels in the recommendations array must be present in channel_campaign_map.\n\n"
                "If any pre-flight check fails, NO mutations are applied and the full error list is returned.\n\n"
                "Execution: mutations are applied sequentially (one channel at a time). Each result is "
                "logged to operator_action_log. Returns a Markdown confirmation table tracking "
                "transaction state (executed / failed / unsupported) per channel, plus a "
                "structured results array for downstream audit."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id": {
                        "type": "string",
                        "description": (
                            "The action_id returned by log_proposed_action. "
                            "Links this execution to the audit trail in operator_action_log "
                            "and operator_pending_approvals."
                        ),
                    },
                    "execution_package": {
                        "type": "object",
                        "description": (
                            "The operator_execution_package dict returned by "
                            "run_marketing_mix_optimization. Must contain:\n"
                            "  schema_version: 'task27.v1'\n"
                            "  operator_approval_required: true\n"
                            "  max_shift_pct_policy: 10.0\n"
                            "  mmm_run_id: string\n"
                            "  recommendations: list of channel directives"
                        ),
                    },
                    "channel_campaign_map": {
                        "type": "object",
                        "description": (
                            "Maps each channel name in the recommendations array to its "
                            "platform-specific entity IDs for the API mutation call. "
                            "Only channels with a map entry will be executed.\n\n"
                            "Per-channel format:\n"
                            "  google_ads:  { customer_id: '123456789', campaign_id: '987654321' }\n"
                            "  tiktok:      { advertiser_id: '111', campaign_id: '222' }\n"
                            "  meta:        { campaign_id: '333444555' }\n"
                            "  linkedin:    { advertiser_id: '456', campaign_id: '789' }\n"
                            "  reddit_ads:  { account_id: 'a2_xxx', campaign_id: 'yyy' }"
                        ),
                    },
                },
                "required": ["action_id", "execution_package", "channel_campaign_map"],
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
        from tools.google_ads_client import (
            GoogleAdsAPIError,
            GoogleAdsSetupError,
        )
        from tools.google_ads_client import (
            push_domain_suppression as gads_push_suppression,
        )
        from tools.linkedin_client import LinkedInAPIError, add_companies_to_segment
        from tools.meta_client import MetaAPIError, add_domains_to_exclusion_audience
        from tools.tiktok_ads_client import (
            TikTokAdsError,
            TikTokSetupError,
        )
        from tools.tiktok_ads_client import (
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
                    crm_emails_by_domain=None,  # CRM auto-fetch wired in Task 22
                )

            elif platform == "reddit_ads":
                from tools.reddit_ads_client import (
                    push_domain_suppression as reddit_push_suppression,
                )
                # Reddit Ads Custom Audience — audience_list_id is the Reddit audience ID.
                # CRM email hash upload is the only supported suppression method
                # (Reddit does not support direct domain targeting).
                result = reddit_push_suppression(
                    account_id=advertiser_id,
                    audience_id=audience_list_id,
                    domains=domains,
                    crm_emails_by_domain=None,  # CRM auto-fetch wired in Task 22
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

            action_log_updated = self._update_action_status(action_id, "executed")
            log.info("operator.suppression_executed", platform=platform, domains=len(domains))
            return {
                **result,
                "action_id": action_id,
                "domain_count": len(domains),
                "platform": platform,
                "action_log_updated": action_log_updated,
            }

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
        from tools.google_ads_client import (
            GoogleAdsAPIError,
            GoogleAdsBudgetGuardrailError,
            GoogleAdsSetupError,
        )
        from tools.google_ads_client import (
            reallocate_campaign_budget as gads_reallocate,
        )
        from tools.linkedin_client import (
            LinkedInAPIError,
        )
        from tools.linkedin_client import (
            get_campaign as li_get_campaign,
        )
        from tools.linkedin_client import (
            update_campaign_daily_budget as li_update_budget,
        )
        from tools.meta_client import (
            MetaAPIError,
        )
        from tools.meta_client import (
            get_campaign as meta_get_campaign,
        )
        from tools.meta_client import (
            update_campaign_daily_budget as meta_update_budget,
        )
        from tools.tiktok_ads_client import (
            TikTokAdsError,
            TikTokBudgetGuardrailError,
            TikTokSetupError,
        )
        from tools.tiktok_ads_client import (
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

            elif platform == "reddit_ads":
                from tools.reddit_ads_client import (
                    reallocate_campaign_budget as reddit_reallocate,
                )
                # advertiser_id = Reddit ad account ID (t2_xxx / a2_xxx — validated in client)
                # source/target entity IDs = Reddit campaign IDs
                result = reddit_reallocate(
                    account_id=advertiser_id,
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

            action_log_updated = self._update_action_status(action_id, "executed")
            log.info("operator.budget_reallocated", platform=platform, amount_usd=amount_usd)
            return {**result, "action_id": action_id, "action_log_updated": action_log_updated}

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

    def _find_executed_package(self, package_hash: str) -> dict | None:
        """
        Return the most recent executed/partial record for this task27 package
        hash, or None. Raises on query failure — the caller fails closed.
        """
        rows = bq.run_query(
            f"""
            SELECT action_id, status, CAST(executed_at AS STRING) AS executed_at
            FROM {bq.table_ref('operator_action_log')}
            WHERE action_type = 'budget_reallocation'
              AND status IN ('executed', 'partial')
              AND guardrail_notes = @notes
            ORDER BY executed_at DESC
            LIMIT 1
            """,
            params={"notes": f"package_hash={package_hash}"},
        )
        return rows[0] if rows else None

    def _record_package_execution(
        self,
        action_id: str,
        package_hash: str,
        terminal_status: str,
        mmm_run_id: str,
        executed_count: int,
        failed_count: int,
    ) -> bool:
        """
        Write the idempotency record as a NEW streamed row (rows in BigQuery's
        streaming buffer cannot be UPDATEd, so amending the original action row
        is not reliable within ~90 minutes of its insert).
        """
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "action_id":             bq.new_uuid(),
            "action_type":           "budget_reallocation",
            "platform":              "multi",
            "platform_entity_type":  None,
            "platform_entity_id":    mmm_run_id,
            "campaign_id":           None,
            "field_changed":         "daily_budget",
            "value_before":          None,
            "value_after":           f"executed={executed_count},failed={failed_count}",
            "change_magnitude":      None,
            "change_magnitude_pct":  None,
            "rationale":             f"task27 package execution record (parent action {action_id})",
            "insight_id":            None,
            "attribution_run_id":    None,
            "execution_mode":        "autonomous",
            "status":                terminal_status,
            "guardrail_check_passed": True,
            "guardrail_notes":       f"package_hash={package_hash}",
            "requires_approval":     False,
            "approved_by":           None,
            "approved_at":           None,
            "rejected_by":           None,
            "rejected_at":           None,
            "rejection_reason":      None,
            "proposed_at":           now,
            "executed_at":           now,
            "rolled_back_at":        None,
            "platform_response":     None,
        }
        try:
            errors = bq.insert_rows("operator_action_log", [row])
            if errors:
                log.error(
                    "operator.idempotency_record_failed",
                    action_id=action_id,
                    package_hash=package_hash,
                    errors=str(errors)[:500],
                )
                return False
            return True
        except Exception as exc:
            log.error(
                "operator.idempotency_record_failed",
                action_id=action_id,
                package_hash=package_hash,
                error=str(exc),
            )
            return False

    def _update_action_status(self, action_id: str, status: str) -> bool:
        """
        Set the terminal status on operator_action_log. Returns False (and logs
        an error) if the audit write fails — callers must surface that to the
        result payload so a platform mutation never silently lacks its audit
        record.
        """
        now = datetime.now(timezone.utc).isoformat()
        try:
            bq.run_dml(
                f"""
                UPDATE {bq.table_ref('operator_action_log')}
                SET status = @status, executed_at = TIMESTAMP(@now)
                WHERE action_id = @action_id
                """,
                params={"status": status, "now": now, "action_id": action_id},
            )
            # Remove from pending approvals once the action reaches a terminal state
            if status in ("executed", "partial"):
                bq.run_dml(
                    f"""
                    DELETE FROM {bq.table_ref('operator_pending_approvals')}
                    WHERE action_id = @action_id
                    """,
                    params={"action_id": action_id},
                )
            return True
        except Exception as exc:
            log.error(
                "operator.status_update_failed",
                action_id=action_id,
                status=status,
                error=str(exc),
            )
            return False

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
            format_asset_correlation_context,
            format_few_shot_context,
            get_asset_type_performance_correlation,
            get_top_performing_ads,
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

    # ── Lookalike Audience Mutation Tool ──────────────────────────────────────

    def _tool_sync_evolving_lookalike_seeds(
        self,
        platform_configs: list[dict],
        seed_limit: int = 10_000,
        run_label: str | None = None,
    ) -> dict:
        """
        Execute a full lookalike seed mutation cycle:
          • Read v_lookalike_mutation_seed (rolling 60-day closed-won cohort)
          • SHA-256 hash all seed emails (raw addresses discarded immediately)
          • Push hashed seed to each configured platform audience
          • Log results to audience_mutation_logs in BigQuery
          • Return internal result dict + Markdown deployment summary

        Returns a dual payload:
          backend_result     — structured dict with counts and per-platform status
          markdown_summary   — formatted Markdown table for human review
        """
        from tools.audience_mutation_engine import AudienceMutationEngine

        # Normalise seed_limit: 0 → None (no cap)
        effective_limit = int(seed_limit) if seed_limit and seed_limit > 0 else None

        engine = AudienceMutationEngine()
        result = engine.run_mutation(
            platform_configs=platform_configs,
            seed_limit=effective_limit,
        )

        label = run_label or f"ICP mutation run {date.today().isoformat()}"

        markdown_summary = _build_mutation_markdown(
            run_label=label,
            result=result,
        )

        log.info(
            "operator.audience_mutation.complete",
            run_id=result.get("run_id"),
            seed_count=result.get("seed_count", 0),
            platforms_pushed=result.get("platforms_pushed", 0),
            ok=result.get("ok"),
        )

        return {
            **result,
            "run_label":      label,
            "markdown_summary": markdown_summary,
        }

    # ── MMM Budget Reallocation Execution Tool ────────────────────────────────

    def _tool_execute_system_budget_reallocation(
        self,
        action_id: str,
        execution_package: dict,
        channel_campaign_map: dict,
    ) -> dict:
        """
        Execute a task27.v1 MMM optimization package across all configured platforms.

        Orchestration sequence:
          1. Schema version + approval flag validation.
          2. Pre-flight guardrail sweep over all recommendations —
             abort with full error list if any check fails (zero mutations applied).
          3. Sequential channel execution loop — Google Ads, TikTok, Meta,
             LinkedIn, Reddit Ads dispatched to dedicated operator mutation clients.
          4. BQ action log update per executed channel.
          5. Markdown confirmation log returned alongside structured results.
        """
        from tools.gmp_client import ApprovalRequiredError
        from tools.google_ads_operator import (
            GOOGLE_ADS_MIN_DAILY_BUDGET_USD,
            GoogleAdsAPIError,
            GoogleAdsBudgetGuardrailError,
            GoogleAdsSetupError,
            modify_google_campaign_budget,
        )
        from tools.linkedin_client import (
            LinkedInAPIError,
        )
        from tools.linkedin_client import (
            update_campaign_daily_budget as li_update_budget,
        )
        from tools.meta_client import (
            MetaAPIError,
        )
        from tools.meta_client import (
            update_campaign_daily_budget as meta_update_budget,
        )
        from tools.reddit_ads_client import (
            RedditAdsBudgetGuardrailError,
            RedditAdsError,
            RedditAdsSetupError,
            modify_reddit_campaign_budget,
        )
        from tools.tiktok_ads_operator import (
            TIKTOK_MIN_DAILY_BUDGET_USD,
            TikTokAdsError,
            TikTokBudgetGuardrailError,
            TikTokSetupError,
            modify_tiktok_campaign_budget,
        )

        # ── 0. Schema version guard ───────────────────────────────────────────
        schema_version = execution_package.get("schema_version", "")
        if not schema_version.startswith("task27"):
            return {
                "action_id": action_id,
                "executed":  False,
                "reason": (
                    f"Unrecognized schema_version '{schema_version}'. "
                    "Expected 'task27.v1'. Pass the operator_execution_package "
                    "returned directly by run_marketing_mix_optimization."
                ),
            }

        # ── 1. Approval flag guard ────────────────────────────────────────────
        # The flag must be True — this is a non-negotiable constraint.
        # A package with operator_approval_required=False would bypass the human
        # review requirement that is central to the Operator's safety model.
        pkg_approval_flag = execution_package.get("operator_approval_required", None)
        if pkg_approval_flag is not True:
            return {
                "action_id": action_id,
                "executed":  False,
                "reason": (
                    f"execution_package.operator_approval_required is {pkg_approval_flag!r}. "
                    "This field must be True for all task27.v1 packages. "
                    "Do not modify the package before passing it to this tool."
                ),
            }

        recommendations: list[dict] = execution_package.get("recommendations", [])
        if not recommendations:
            return {
                "action_id": action_id,
                "executed":  False,
                "reason": "No recommendations found in execution_package.recommendations.",
            }

        max_shift_policy: float = float(
            execution_package.get("max_shift_pct_policy", 10.0)
        )
        mmm_run_id: str = execution_package.get("mmm_run_id", "unknown")

        # ── 2. Pre-flight guardrail sweep (all-or-nothing) ────────────────────
        preflight_errors: list[str] = []

        for rec in recommendations:
            ch          = rec.get("channel", "unknown")
            shift_pct   = float(rec.get("recommended_shift_pct", 0) or 0)
            new_target  = float(rec.get("new_target_budget_usd", 0) or 0)
            direction   = rec.get("direction", "")

            # ▸ Shift ceiling
            if abs(shift_pct) > max_shift_policy:
                preflight_errors.append(
                    f"[{ch}] recommended_shift_pct {shift_pct:.1f}% exceeds "
                    f"policy ceiling {max_shift_policy:.1f}%."
                )

            # ▸ Platform floor — Google Ads ($5.00)
            if ch == "google_ads" and new_target < GOOGLE_ADS_MIN_DAILY_BUDGET_USD:
                preflight_errors.append(
                    f"[google_ads] new_target_budget_usd ${new_target:.2f} is below "
                    f"the ${GOOGLE_ADS_MIN_DAILY_BUDGET_USD:.2f} minimum floor."
                )

            # ▸ Platform floor — TikTok ($20.00)
            if ch == "tiktok" and new_target < TIKTOK_MIN_DAILY_BUDGET_USD:
                preflight_errors.append(
                    f"[tiktok] new_target_budget_usd ${new_target:.2f} is below "
                    f"the ${TIKTOK_MIN_DAILY_BUDGET_USD:.2f} minimum floor."
                )

            # ▸ Campaign map presence
            if ch not in channel_campaign_map:
                preflight_errors.append(
                    f"[{ch}] not found in channel_campaign_map. "
                    f"Add an entry with the platform-specific entity IDs for this channel."
                )

        if preflight_errors:
            log.warning(
                "operator.budget_reallocation.preflight_failed",
                action_id=action_id,
                mmm_run_id=mmm_run_id,
                error_count=len(preflight_errors),
                errors=preflight_errors,
            )
            return {
                "action_id":        action_id,
                "executed":         False,
                "preflight_failed": True,
                "error_count":      len(preflight_errors),
                "errors":           preflight_errors,
                "reason": (
                    f"{len(preflight_errors)} pre-flight check(s) failed. "
                    "Zero mutations applied — resolve all errors before retrying."
                ),
            }

        # ── 2.5 Idempotency guard ─────────────────────────────────────────────
        # A scheduler retry or double cron fire must not apply the same task27
        # package twice. Every execution that mutates at least one channel
        # writes a record keyed by the package's SHA-256; replays return the
        # prior outcome instead of mutating again. Runs after pre-flight (so a
        # malformed package reports its errors without a BQ round-trip) and
        # fails CLOSED: if the lookup itself errors, we refuse to move money
        # blind.
        package_hash = _hash_execution_package(execution_package)
        try:
            prior = self._find_executed_package(package_hash)
        except Exception as exc:
            return {
                "action_id": action_id,
                "executed":  False,
                "reason": (
                    f"Idempotency check against operator_action_log failed ({exc}). "
                    "Refusing to execute budget mutations without replay protection — retry "
                    "once BigQuery is reachable."
                ),
            }
        if prior:
            log.warning(
                "operator.budget_reallocation.replay_blocked",
                action_id=action_id,
                package_hash=package_hash,
                prior_action_id=prior.get("action_id"),
                prior_status=prior.get("status"),
            )
            return {
                "action_id":        action_id,
                "executed":         False,
                "replay_blocked":   True,
                "package_hash":     package_hash,
                "prior_action_id":  prior.get("action_id"),
                "prior_status":     prior.get("status"),
                "prior_executed_at": prior.get("executed_at"),
                "reason": (
                    "This exact task27 package was already executed "
                    f"(action {prior.get('action_id')}, status '{prior.get('status')}'). "
                    "Re-applying it could compound budget shifts. Generate a fresh "
                    "optimization package if a new reallocation is intended."
                ),
            }

        # ── 3. Sequential execution loop ──────────────────────────────────────
        results: list[dict] = []

        for rec in recommendations:
            ch          = rec.get("channel", "unknown")
            direction   = rec.get("direction", "")
            shift_pct   = float(rec.get("recommended_shift_pct", 0) or 0)
            shift_usd   = float(rec.get("recommended_shift_usd", 0) or 0)
            new_target  = float(rec.get("new_target_budget_usd", 0) or 0)
            adj_roi     = rec.get("adj_roi_mean")
            conf_tier   = rec.get("confidence_tier", "")
            bsts_align  = rec.get("bsts_alignment", "")

            platform_ids: dict = channel_campaign_map.get(ch, {})

            tx: dict = {
                "channel":                ch,
                "direction":              direction,
                "recommended_shift_pct":  shift_pct,
                "recommended_shift_usd":  shift_usd,
                "new_target_budget_usd":  new_target,
                "adj_roi_mean":           adj_roi,
                "confidence_tier":        conf_tier,
                "bsts_alignment":         bsts_align,
            }

            try:
                if ch == "google_ads":
                    r = modify_google_campaign_budget(
                        customer_id=platform_ids["customer_id"],
                        campaign_id=platform_ids["campaign_id"],
                        new_budget_usd=new_target,
                    )
                    tx.update({"status": "executed", "platform_response": r})

                elif ch == "tiktok":
                    r = modify_tiktok_campaign_budget(
                        advertiser_id=platform_ids["advertiser_id"],
                        campaign_id=platform_ids["campaign_id"],
                        new_budget_usd=new_target,
                    )
                    tx.update({"status": "executed", "platform_response": r})

                elif ch == "meta":
                    # Meta budget API uses cents (int × 100)
                    new_cents = int(round(new_target * 100))
                    r = meta_update_budget(
                        campaign_id=platform_ids["campaign_id"],
                        new_daily_budget_cents=new_cents,
                    )
                    tx.update({"status": "executed", "platform_response": r})

                elif ch == "linkedin":
                    r = li_update_budget(
                        campaign_id=platform_ids["campaign_id"],
                        new_daily_budget_usd=new_target,
                    )
                    tx.update({"status": "executed", "platform_response": r})

                elif ch == "reddit_ads":
                    r = modify_reddit_campaign_budget(
                        account_id=platform_ids["account_id"],
                        campaign_id=platform_ids["campaign_id"],
                        new_budget_usd=new_target,
                    )
                    tx.update({"status": "executed", "platform_response": r})

                else:
                    tx.update({
                        "status": "unsupported",
                        "note":   f"No budget adapter configured for channel '{ch}'.",
                    })

            except (
                ApprovalRequiredError,
                GoogleAdsAPIError, GoogleAdsSetupError, GoogleAdsBudgetGuardrailError,
                TikTokAdsError, TikTokSetupError, TikTokBudgetGuardrailError,
                RedditAdsError, RedditAdsSetupError, RedditAdsBudgetGuardrailError,
                MetaAPIError, LinkedInAPIError,
                KeyError, ValueError,
            ) as exc:
                tx.update({"status": "failed", "error": str(exc)})
                log.error(
                    "operator.budget_reallocation.channel_failed",
                    channel=ch,
                    action_id=action_id,
                    error=str(exc),
                )

            results.append(tx)

        # ── 4. Summary + single terminal status ──────────────────────────────
        executed_count  = sum(1 for r in results if r["status"] == "executed")
        failed_count    = sum(1 for r in results if r["status"] == "failed")
        skipped_count   = sum(1 for r in results if r["status"] == "unsupported")

        # Pre-flight is all-or-nothing, but the execution loop is not: a
        # channel can fail after earlier channels already applied. Record ONE
        # honest terminal status for the whole action instead of stamping
        # "executed" per channel — "partial" means some budgets moved and some
        # did not, and the per-channel detail is in `results`.
        if executed_count and not failed_count:
            terminal_status = "executed"
        elif executed_count and failed_count:
            terminal_status = "partial"
        else:
            terminal_status = "failed"
        action_log_updated = self._update_action_status(action_id, terminal_status)

        # Record the package hash whenever anything mutated, so a replay of the
        # same package is blocked. Fully-failed runs are NOT recorded — they
        # applied nothing and stay retryable.
        idempotency_recorded = True
        if executed_count:
            idempotency_recorded = self._record_package_execution(
                action_id=action_id,
                package_hash=package_hash,
                terminal_status=terminal_status,
                mmm_run_id=mmm_run_id,
                executed_count=executed_count,
                failed_count=failed_count,
            )

        markdown_summary = _build_budget_reallocation_markdown(
            mmm_run_id=mmm_run_id,
            action_id=action_id,
            results=results,
            executed_count=executed_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
        )

        log.info(
            "operator.budget_reallocation.complete",
            action_id=action_id,
            mmm_run_id=mmm_run_id,
            total=len(results),
            executed=executed_count,
            failed=failed_count,
            skipped=skipped_count,
            terminal_status=terminal_status,
            action_log_updated=action_log_updated,
        )

        return {
            "action_id":              action_id,
            "mmm_run_id":             mmm_run_id,
            "schema_version":         schema_version,
            "package_hash":           package_hash,
            "terminal_status":        terminal_status,
            "action_log_updated":     action_log_updated,
            "idempotency_recorded":   idempotency_recorded,
            "total_recommendations":  len(results),
            "executed":               executed_count,
            "failed":                 failed_count,
            "skipped":                skipped_count,
            "results":                results,
            "markdown_summary":       markdown_summary,
        }


def _hash_execution_package(execution_package: dict) -> str:
    """Stable SHA-256 of a task27 package — the idempotency key for execution."""
    canonical = json.dumps(
        execution_package, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ── Markdown builders (module-level, pure formatting) ────────────────────────

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
            lines.append("> **Visual Concept:**")
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

    footer = textwrap.dedent("""\
        ---

        *Brief generated by the Paid Media Agent creative engine.*
        *Platform constraints enforced: Google RSA ≤ 30/90 chars · LinkedIn ≤ 150 chars (primary) · Meta ≤ 125/27 chars · TikTok ≤ 100 chars.*
        *Visual briefs follow the Multi-Asset Creative Brief Framework (see `agents/operator/skills/copy_assistant.md`).*
    """)

    return f"{header}\n{copy_section}\n{visual_section}\n{footer}"


# ── Audience mutation Markdown builder ────────────────────────────────────────

_PLATFORM_DISPLAY_NAMES = {
    "meta":       "Meta Ads",
    "google_ads": "Google Ads",
    "tiktok":     "TikTok Ads",
    "reddit_ads": "Reddit Ads",
}


def _build_mutation_markdown(run_label: str, result: dict) -> str:
    """
    Build a structured Markdown analysis report for a lookalike seed mutation run.

    Deterministic — no inference calls. Pure formatting from the result dict
    returned by AudienceMutationEngine.run_mutation().

    Sections:
      1. Run summary (seed count, unique domains, ARR threshold)
      2. Firmographic Over-Index table (top 3 traits per dimension)
      3. Platform upload confirmation table (✅ / ❌)
      4. ICP drift narrative (dominant shift headline)
    """
    today = date.today().isoformat()
    ok    = result.get("ok", False)
    run_id     = result.get("run_id", "—")
    seed_count = result.get("seed_count", 0)
    domains    = result.get("unique_domains", 0)
    arr_p75    = result.get("arr_p75_threshold")
    arr_str    = f"${arr_p75:,.0f}" if arr_p75 is not None else "n/a"
    platforms_pushed = result.get("platforms_pushed", 0)
    platforms_total  = result.get("platforms_total", 0)
    firmographic     = result.get("firmographic_summary", {})
    dominant_shift   = firmographic.get("dominant_shift", "—")
    platform_results = result.get("platform_results", {})

    overall_badge = "✅ Mutation complete" if ok else "❌ Mutation failed or partial"

    lines: list[str] = [
        "## 🔄 Lookalike Seed Mutation Report",
        f"**{run_label}**",
        "",
        "| | |",
        "|-|-|",
        f"| **Status** | {overall_badge} ({platforms_pushed}/{platforms_total} platforms) |",
        f"| **Run ID** | `{run_id}` |",
        f"| **Generated** | {today} |",
        f"| **Seed emails pushed** | {seed_count:,} unique SHA-256 hashed contacts |",
        f"| **Unique seed domains** | {domains:,} closed-won accounts (60-day rolling window) |",
        f"| **ARR gating threshold (p75)** | {arr_str} |",
        f"| **Dominant ICP shift** | **{dominant_shift}** |",
        "",
        "---",
        "",
    ]

    # ── Section 1: Firmographic Over-Index Table ─────────────────────────────
    lines.append("## 📊 Firmographic Over-Index Analysis")
    lines.append("")
    lines.append(
        "*Over-index = how much more (or less) a trait appears in closed-won accounts "
        "vs top-of-funnel lead mix. +30% means 1.3× over-represented in revenue cohort.*"
    )
    lines.append("")

    top_industries = firmographic.get("top_industries", [])
    top_emp        = firmographic.get("top_employee_ranges", [])
    top_regions    = firmographic.get("top_regions", [])

    # Build a unified 3-column table: rank | trait | over-index %
    def _fmt_pct(v: float) -> str:
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.1f}%"

    def _over_index_badge(v: float) -> str:
        if v >= 25:
            return "🟢 Strong"
        if v >= 10:
            return "🟡 Moderate"
        if v >= 0:
            return "⚪ Neutral"
        return "🔴 Under-index"

    lines += [
        "### By Industry",
        "",
        "| Rank | Industry | Over-Index vs Lead Mix | Signal |",
        "|------|----------|----------------------|--------|",
    ]
    if top_industries:
        for i, item in enumerate(top_industries, 1):
            pct    = item.get("over_index_pct", 0.0)
            badge  = _over_index_badge(pct)
            lines.append(f"| {i} | {item['trait']} | {_fmt_pct(pct)} | {badge} |")
    else:
        lines.append("| — | No firmographic data available | — | — |")
    lines.append("")

    lines += [
        "### By Company Size",
        "",
        "| Rank | Employee Range | Over-Index vs Lead Mix | Signal |",
        "|------|---------------|----------------------|--------|",
    ]
    if top_emp:
        for i, item in enumerate(top_emp, 1):
            pct   = item.get("over_index_pct", 0.0)
            badge = _over_index_badge(pct)
            lines.append(f"| {i} | {item['trait']} | {_fmt_pct(pct)} | {badge} |")
    else:
        lines.append("| — | No firmographic data available | — | — |")
    lines.append("")

    lines += [
        "### By Region",
        "",
        "| Rank | Region | Over-Index vs Lead Mix | Signal |",
        "|------|--------|----------------------|--------|",
    ]
    if top_regions:
        for i, item in enumerate(top_regions, 1):
            pct   = item.get("over_index_pct", 0.0)
            badge = _over_index_badge(pct)
            lines.append(f"| {i} | {item['trait']} | {_fmt_pct(pct)} | {badge} |")
    else:
        lines.append("| — | No firmographic data available | — | — |")
    lines.append("")

    lines.append("---")
    lines.append("")

    # ── Section 2: Platform Upload Confirmation Table ─────────────────────────
    lines += [
        "## 🚀 Platform Seed Upload Confirmation",
        "",
        "| Platform | Audience ID | Emails Pushed | Status |",
        "|----------|-------------|--------------|--------|",
    ]

    for key, pr in platform_results.items():
        platform   = pr.get("platform", key.split(":")[0])
        aud_id     = pr.get("audience_id", "—")
        emails     = pr.get("emails_pushed", 0)
        status     = pr.get("status", "unknown")
        error      = pr.get("error", "")
        plat_label = _PLATFORM_DISPLAY_NAMES.get(platform, platform.replace("_", " ").title())

        if status == "ok":
            status_cell = f"✅ Uploaded ({emails:,} emails)"
        elif status == "unsupported":
            status_cell = "⚠️ Unsupported platform"
        elif status == "error":
            short_err   = error[:60] + "…" if len(error) > 60 else error
            status_cell = f"❌ Failed — {short_err}"
        else:
            status_cell = f"⚠️ {status}"

        lines.append(f"| {plat_label} | `{aud_id}` | {emails:,} | {status_cell} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Section 3: ICP Drift Narrative ────────────────────────────────────────
    lines += [
        "## 💡 ICP Drift Summary",
        "",
        f"> **Dominant signal:** {dominant_shift}",
        ">",
        "> The seed cohort above reflects accounts that closed in the last 60 days "
        "and ranked in the top-25% ARR tier. The firmographic over-index scores "
        "identify which buyer profiles are disproportionately converting to revenue "
        "relative to current top-of-funnel lead mix.",
        ">",
        "> **Recommended action:** Review the over-indexing industries and size tiers "
        "with your demand gen team. If the mutation signal is materially different "
        "from your current ICP definition, consider updating:",
        ">   1. Paid media audience targeting segments",
        ">   2. ICP score thresholds in `company_profiles.icp_score`",
        ">   3. Content and creative messaging for over-indexing verticals",
        "",
        f"*Seed mutation executed by the Paid Media Operator Agent. "
        f"Raw contact data was SHA-256 hashed and discarded — "
        f"zero PII persisted. Logged to `audience_mutation_logs` (run_id: `{run_id}`).*",
    ]

    return "\n".join(lines)


# ── Budget Reallocation Execution Markdown builder ────────────────────────────

_DIRECTION_ICON: dict[str, str] = {
    "increase": "⬆️",
    "decrease": "⬇️",
}

_STATUS_ICON: dict[str, str] = {
    "executed":    "✅",
    "failed":      "❌",
    "unsupported": "⚠️",
}

_CONFIDENCE_ICON: dict[str, str] = {
    "high":   "🟢",
    "medium": "🟡",
    "low":    "🔴",
    "":       "—",
}

_BSTS_ICON: dict[str, str] = {
    "aligned":         "✅",
    "divergent":       "⚠️",
    "uncorroborated":  "—",
    "":                "—",
}


def _build_budget_reallocation_markdown(
    mmm_run_id: str,
    action_id: str,
    results: list[dict],
    executed_count: int,
    failed_count: int,
    skipped_count: int,
) -> str:
    """
    Render a Markdown execution confirmation log for a task27.v1 budget reallocation run.

    Three sections:
      1. Header — run metadata and overall status badge.
      2. Transaction table — per-channel row with direction, amounts, and status.
      3. Intelligence summary — confidence tier and BSTS alignment per channel.
      4. Failure detail block (only if failed_count > 0).
      5. Sequencing reminder footer.
    """
    total = len(results)
    clean = failed_count == 0 and skipped_count == 0

    overall_badge = (
        "✅ Clean run — all channels executed"
        if clean
        else f"{'⚠️' if failed_count > 0 else '📋'} "
             f"{executed_count}/{total} executed"
             + (f" · {failed_count} failed" if failed_count else "")
             + (f" · {skipped_count} unsupported" if skipped_count else "")
    )

    lines: list[str] = [
        "## 🤖 Budget Reallocation Execution Log",
        "",
        "| Field | Value |",
        "|-------|-------|",
        f"| MMM Run ID | `{mmm_run_id}` |",
        f"| Action ID | `{action_id}` |",
        f"| Channels processed | {total} |",
        f"| Overall status | {overall_badge} |",
        "",
        "---",
        "",
        "### Transaction Summary",
        "",
        "| Channel | Direction | Shift % | Shift USD | New Budget | Status |",
        "|---------|-----------|--------:|----------:|-----------:|--------|",
    ]

    for r in results:
        ch      = r.get("channel", "?")
        dirn    = r.get("direction", "")
        s_pct   = r.get("recommended_shift_pct", 0.0)
        s_usd   = r.get("recommended_shift_usd", 0.0)
        new_b   = r.get("new_target_budget_usd", 0.0)
        status  = r.get("status", "?")

        dir_cell    = f"{_DIRECTION_ICON.get(dirn, '')} {dirn}"
        status_icon = _STATUS_ICON.get(status, "?")
        error_snip  = f": {r['error'][:55]}…" if status == "failed" and r.get("error") else ""
        status_cell = f"{status_icon} {status}{error_snip}"

        lines.append(
            f"| {ch} | {dir_cell} | {s_pct:.1f}% | ${s_usd:,.0f} | ${new_b:,.0f} | {status_cell} |"
        )

    lines += [
        "",
        "---",
        "",
        "### Measurement Intelligence",
        "",
        "| Channel | Adj-ROI | Confidence | BSTS Alignment |",
        "|---------|--------:|-----------|----------------|",
    ]

    for r in results:
        ch        = r.get("channel", "?")
        adj_roi   = r.get("adj_roi_mean")
        conf      = r.get("confidence_tier", "")
        bsts      = r.get("bsts_alignment", "")

        roi_cell  = f"{adj_roi:.2f}x" if adj_roi is not None else "—"
        conf_cell = f"{_CONFIDENCE_ICON.get(conf, '?')} {conf}" if conf else "—"
        bsts_cell = f"{_BSTS_ICON.get(bsts, '?')} {bsts}" if bsts else "—"

        lines.append(f"| {ch} | {roi_cell} | {conf_cell} | {bsts_cell} |")

    lines.append("")

    # ── Failure detail block ──────────────────────────────────────────────────
    if failed_count > 0:
        lines += ["---", "", "### ❌ Failure Details", ""]
        for r in results:
            if r.get("status") == "failed":
                lines.append(f"- **{r.get('channel', '?')}:** {r.get('error', 'Unknown error')}")
        lines.append("")

    # ── Sequencing reminder footer ────────────────────────────────────────────
    lines += [
        "---",
        "",
        "> **Sequencing rule (Meridian Framework 3b):** Monitor performance for "
        "2–4 weeks before approving a second channel increase. Simultaneous "
        "multi-channel shifts increase attribution complexity and compound error risk.",
        ">",
        "> All executed changes are live in the respective ad platforms. "
        f"Results logged to `operator_action_log` under action ID `{action_id}`.",
    ]

    return "\n".join(lines)
