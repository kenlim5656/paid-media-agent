# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
HTTP-exposed query/action handlers for the Cloud Run app.

Server-side implementations of the three routes paid-media-mcp calls on
PAID_MEDIA_AGENT_URL:

    POST /query/account-journey       → query_account_journey()
    POST /action/audience-suppression → push_audience_suppression()
    POST /action/reallocate-budget    → reallocate_budget()

Both action handlers run through the Operator's existing tool path
(log_proposed_action → execution tool), so every guardrail that applies to
the autonomous agent applies identically here:

  • OPERATOR_REQUIRE_APPROVAL — platform clients raise ApprovalRequiredError
    and the action stays queued in operator_pending_approvals.
  • MAX_BUDGET_SHIFT_PCT — budget caps enforced inside the platform clients
    (GoogleAdsBudgetGuardrailError, TikTokBudgetGuardrailError, …).
  • Audit trail — every action is logged to operator_action_log before any
    execution attempt.

There is no execution path that bypasses those guardrails.
"""
import structlog

from tools import bigquery_client as bq
from tools.crm_client import _validate_domain

log = structlog.get_logger()


# ── Account journey query (read-only) ──────────────────────────────────────────

def query_account_journey(
    account_domain: str,
    lookback_days: int = 90,
    conversion_type: str | None = None,
) -> dict:
    """
    Return the cross-channel journey for one account domain: touchpoints with
    attribution credit from the latest completed run, plus conversions.
    Mirrors the response shape of the MCP BigQueryAdapter.queryAccountJourney
    so either path returns the same contract.
    """
    domain = _validate_domain(account_domain)
    lookback_days = max(1, min(int(lookback_days), 730))

    conv_filter = "AND c.conversion_type = @conversion_type" if conversion_type else ""

    touch_rows = bq.run_query(
        f"""
        WITH account_entities AS (
            SELECT DISTINCT ies.entity_id
            FROM {bq.table_ref('identity_entity_signals')} ies
            JOIN {bq.table_ref('identity_entities')} e ON e.entity_id = ies.entity_id
            WHERE (e.company_domain = @account_domain
                   OR ies.identifier_value = @account_domain)
              AND ies.is_active = TRUE
        )
        SELECT
            t.touchpoint_id,
            t.entity_id,
            t.touchpoint_at,
            t.touchpoint_type,
            t.platform,
            t.channel,
            t.campaign_id,
            t.funnel_stage,
            t.path_position,
            t.path_total_touches,
            r.credit_weight,
            r.credit_conversions,
            r.model_name
        FROM {bq.table_ref('touchpoint_events')} t
        JOIN account_entities ae ON ae.entity_id = t.entity_id
        LEFT JOIN {bq.table_ref('attribution_results')} r ON r.touchpoint_id = t.touchpoint_id
            AND r.run_id = (
                SELECT run_id FROM {bq.table_ref('attribution_runs')}
                WHERE status = 'completed' ORDER BY completed_at DESC LIMIT 1
            )
        WHERE t.touchpoint_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
        ORDER BY t.touchpoint_at ASC
        LIMIT 500
        """,
        params={"account_domain": domain, "lookback_days": lookback_days},
    )

    conv_params: dict = {"account_domain": domain, "lookback_days": lookback_days}
    if conversion_type:
        conv_params["conversion_type"] = conversion_type
    conv_rows = bq.run_query(
        f"""
        WITH account_entities AS (
            SELECT DISTINCT ies.entity_id
            FROM {bq.table_ref('identity_entity_signals')} ies
            JOIN {bq.table_ref('identity_entities')} e ON e.entity_id = ies.entity_id
            WHERE e.company_domain = @account_domain AND ies.is_active = TRUE
        )
        SELECT
            c.conversion_id, c.entity_id, c.converted_at,
            c.conversion_type, c.conversion_value, c.deal_value, c.pipeline_stage
        FROM {bq.table_ref('conversion_events')} c
        JOIN account_entities ae ON ae.entity_id = c.entity_id
        WHERE c.converted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
        {conv_filter}
        ORDER BY c.converted_at ASC
        """,
        params=conv_params,
    )

    entities = {str(r["entity_id"]) for r in touch_rows}
    channels = sorted({str(r["channel"]) for r in touch_rows if r.get("channel")})
    platforms = sorted({str(r["platform"]) for r in touch_rows if r.get("platform")})
    total_credit = sum(float(r.get("credit_weight") or 0) for r in touch_rows)

    return {
        "account_domain": domain,
        "entity_count": len(entities),
        "touchpoints": touch_rows,
        "conversions": conv_rows,
        "path_summary": {
            "total_touchpoints": len(touch_rows),
            "total_conversions": len(conv_rows),
            "channels_touched": channels,
            "platforms_touched": platforms,
            "total_attributed_credit": total_credit,
        },
    }


# ── Operator-guarded write actions ─────────────────────────────────────────────

def push_audience_suppression(
    platform: str,
    advertiser_id: str,
    audience_list_id: str,
    domains: list[str],
    rationale: str,
) -> dict:
    """Log the action, then execute via the Operator's suppression tool."""
    validated = [_validate_domain(d) for d in domains]

    from agents.operator.agent import OperatorAgent
    operator = OperatorAgent()

    logged = operator._tool_log_proposed_action(
        action_type="audience_exclusion",
        platform=platform,
        platform_entity_id=audience_list_id,
        rationale=rationale,
        summary=f"HTTP request: suppress {len(validated)} domains on {platform}",
        estimated_impact=None,
    )
    result = operator._tool_push_audience_suppression(
        action_id=logged["action_id"],
        platform=platform,
        advertiser_id=advertiser_id,
        audience_list_id=audience_list_id,
        domains=validated,
    )
    result.setdefault("executed", True)
    result["requires_approval"] = logged["requires_approval"]
    log.info(
        "http_actions.audience_suppression",
        platform=platform,
        domains=len(validated),
        executed=result["executed"],
    )
    return result


def reallocate_budget(
    platform: str,
    advertiser_id: str,
    source_campaign_id: str,
    target_campaign_id: str,
    amount_usd: float,
    rationale: str,
) -> dict:
    """Log the action, then execute via the Operator's reallocation tool."""
    from agents.operator.agent import OperatorAgent
    operator = OperatorAgent()

    logged = operator._tool_log_proposed_action(
        action_type="budget_reallocation",
        platform=platform,
        platform_entity_id=source_campaign_id,
        campaign_id=source_campaign_id,
        field_changed="daily_budget",
        change_magnitude=amount_usd,
        rationale=rationale,
        summary=(
            f"HTTP request: move ${amount_usd:,.2f} from {source_campaign_id} "
            f"to {target_campaign_id} on {platform}"
        ),
        estimated_impact=None,
    )
    result = operator._tool_reallocate_budget(
        action_id=logged["action_id"],
        platform=platform,
        advertiser_id=advertiser_id,
        source_entity_id=source_campaign_id,
        target_entity_id=target_campaign_id,
        amount_usd=amount_usd,
    )
    result.setdefault("executed", True)
    result["requires_approval"] = logged["requires_approval"]
    log.info(
        "http_actions.budget_reallocation",
        platform=platform,
        amount_usd=amount_usd,
        executed=result["executed"],
    )
    return result
