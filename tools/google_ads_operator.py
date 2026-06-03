# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Google Ads Operator mutation client — Task 21 write pathway.

Provides the atomic budget mutation layer consumed by the Operator agent's
`execute_system_budget_reallocation` tool. Wraps the two-step
get_campaign() → update_campaign_budget() sequence into a single
`modify_google_campaign_budget()` call that:

  1. Resolves the campaign's CampaignBudget resource name
     (customers/{customer_id}/campaignBudgets/{budget_id}).
  2. Validates the proposed budget against the $5.00 daily floor.
  3. Issues a clean CampaignBudgetService.mutate_campaign_budgets()
     update, converting USD → micros internally.
  4. Enforces the max_budget_shift_pct guardrail from settings
     (inherited from google_ads_client.update_campaign_budget).

All write operations remain gated on OPERATOR_REQUIRE_APPROVAL.
Authentication: inherits the dual-mode (Full Mode env vars / Simple Mode yaml)
credential resolution from google_ads_client._get_client().

Used by:
  agents/operator/agent.py → _tool_execute_system_budget_reallocation()
"""
from __future__ import annotations

from decimal import Decimal

import structlog

from tools.google_ads_client import (
    GoogleAdsBudgetGuardrailError,  # noqa: F401  (re-exported for callers)
    GoogleAdsAPIError,               # noqa: F401
    GoogleAdsSetupError,             # noqa: F401
    _usd_to_micros,
    get_campaign,
    update_campaign_budget,
)

log = structlog.get_logger()

# ── Platform floor ─────────────────────────────────────────────────────────────
#
# Google Ads enforces a minimum daily budget of $1.00, but we set a higher
# operational floor of $5.00 to ensure sufficient data volume for performance
# measurement. Below this threshold, impression and click data is too sparse
# for reliable attribution signal.
#
GOOGLE_ADS_MIN_DAILY_BUDGET_USD: float = 5.00


# ── Public interface ───────────────────────────────────────────────────────────

def modify_google_campaign_budget(
    customer_id: str,
    campaign_id: str,
    new_budget_usd: float | Decimal,
) -> dict:
    """
    Resolve a campaign's CampaignBudget resource name and update it atomically.

    This is the canonical entry point for the Operator agent to mutate a Google Ads
    campaign budget from a task27.v1 MMM optimization recommendation. It wraps the
    two-step get_campaign → update_campaign_budget sequence into one call, ensuring
    the resource name is always resolved from live API state (not a caller-supplied
    string that could become stale).

    Args:
        customer_id:    Google Ads customer ID, digits only — no dashes.
                        For MCC-managed accounts, use the child customer ID.
        campaign_id:    Google Ads campaign ID (numeric string).
        new_budget_usd: New daily budget in USD. Accepts float or Decimal;
                        converted to micros (× 1,000,000) for the API call.
                        Must be ≥ GOOGLE_ADS_MIN_DAILY_BUDGET_USD ($5.00).

    Returns:
        Dict with:
          platform, customer_id, campaign_id, campaign_name,
          campaign_budget_resource_name, budget_period,
          previous_budget_usd (Decimal), new_budget_usd (float),
          new_budget_micros (int), status ("updated").

    Raises:
        GoogleAdsBudgetGuardrailError: if new_budget_usd < $5.00 floor,
            or if the percentage change exceeds settings.max_budget_shift_pct.
        GoogleAdsAPIError: on API-level failures.
        GoogleAdsSetupError: if credentials are not configured.
        ApprovalRequiredError: if OPERATOR_REQUIRE_APPROVAL=true (inherited
            from update_campaign_budget via _require_approval).
    """
    # ── Floor enforcement ──────────────────────────────────────────────────────
    new_budget_float = float(
        Decimal(str(new_budget_usd)).quantize(Decimal("0.01"))
    )
    if new_budget_float < GOOGLE_ADS_MIN_DAILY_BUDGET_USD:
        raise GoogleAdsBudgetGuardrailError(
            f"Proposed budget ${new_budget_float:.2f} is below the Google Ads operational "
            f"floor of ${GOOGLE_ADS_MIN_DAILY_BUDGET_USD:.2f}/day. Insufficient data volume "
            f"below this threshold for reliable attribution measurement. "
            f"Adjust new_budget_usd or remove this channel from the reallocation package."
        )

    # ── 1. Resolve campaign + budget resource name from live API ──────────────
    campaign = get_campaign(customer_id, campaign_id)
    resource_name: str = campaign["campaign_budget_resource_name"]
    current_usd: float = float(campaign["budget_amount"])

    log.debug(
        "google_ads_operator.resolve_campaign",
        customer_id=customer_id,
        campaign_id=campaign_id,
        campaign_name=campaign.get("campaign_name"),
        resource_name=resource_name,
        current_usd=current_usd,
        proposed_usd=new_budget_float,
    )

    # ── 2. Issue budget update (guardrail + approval gate in called function) ──
    update_result = update_campaign_budget(
        customer_id=customer_id,
        campaign_budget_resource_name=resource_name,
        new_amount_usd=new_budget_float,
        current_amount_usd=current_usd,
    )

    log.info(
        "google_ads_operator.budget_modified",
        customer_id=customer_id,
        campaign_id=campaign_id,
        campaign_name=campaign.get("campaign_name"),
        previous_usd=current_usd,
        new_usd=new_budget_float,
        resource_name=resource_name,
    )

    return {
        "platform":                       "google_ads",
        "customer_id":                    customer_id,
        "campaign_id":                    campaign_id,
        "campaign_name":                  campaign.get("campaign_name"),
        "campaign_budget_resource_name":  resource_name,
        "budget_period":                  campaign.get("budget_period", "DAILY"),
        "budget_type":                    campaign.get("budget_type", "STANDARD"),
        "previous_budget_usd":            current_usd,
        "new_budget_usd":                 new_budget_float,
        "new_budget_micros":              _usd_to_micros(new_budget_float),
        "status":                         "updated",
        # Pass-through from update_campaign_budget for audit trail
        "_api_response_resource":         update_result.get("campaign_budget_resource_name"),
    }
