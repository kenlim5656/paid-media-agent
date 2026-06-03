# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
TikTok Ads Operator mutation client — Task 20 write pathway.

Provides the atomic budget mutation layer consumed by the Operator agent's
`execute_system_budget_reallocation` tool. Wraps tiktok_ads_client's
get_campaign() + update_campaign_budget() into a single
`modify_tiktok_campaign_budget()` call that:

  1. Resolves the campaign's current budget and budget_mode from live API state.
  2. Converts the incoming Python Decimal to an exact, 2dp-rounded float
     to match TikTok's API contract (plain numeric, not micros).
  3. Preserves lifetime vs. daily pacing rules by reading budget_mode
     from the live campaign object (BUDGET_MODE_DAY / BUDGET_MODE_TOTAL).
  4. Validates the proposed budget against the platform floor
     ($20.00 daily / $50.00 lifetime).
  5. Enforces the max_budget_shift_pct guardrail from settings
     (inherited from tiktok_ads_client.update_campaign_budget).

All write operations remain gated on OPERATOR_REQUIRE_APPROVAL.
Authentication: inherits dual-mode credential resolution from
tiktok_ads_client._get_context().

API reference: POST /open_api/v1.3/campaign/update/
Docs: https://business-api.tiktok.com/portal/docs?id=1739318962329602

Used by:
  agents/operator/agent.py → _tool_execute_system_budget_reallocation()
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

import structlog

from tools.tiktok_ads_client import (
    TikTokBudgetGuardrailError,  # noqa: F401  (re-exported for callers)
    TikTokAdsError,               # noqa: F401
    TikTokSetupError,             # noqa: F401
    get_campaign,
    update_campaign_budget,
)

log = structlog.get_logger()

# ── Platform floors ────────────────────────────────────────────────────────────
#
# TikTok's platform minimum is $20.00/day for AUCTION campaigns.
# Lifetime budgets require a higher floor due to the campaign duration factor.
# These are operational floors — the actual TikTok API floor may be lower, but
# data volume below these levels is insufficient for attribution measurement.
#
TIKTOK_MIN_DAILY_BUDGET_USD: float = 20.00
TIKTOK_MIN_LIFETIME_BUDGET_USD: float = 50.00

# ── Budget mode → floor mapping ───────────────────────────────────────────────
_BUDGET_MODE_TO_TYPE: dict[str, str] = {
    "BUDGET_MODE_DAY":   "daily",
    "BUDGET_MODE_TOTAL": "lifetime",
    "BUDGET_MODE_INF":   "daily",    # unlimited — treat as daily for floor checks
}

_MIN_FLOOR_BY_TYPE: dict[str, float] = {
    "daily":    TIKTOK_MIN_DAILY_BUDGET_USD,
    "lifetime": TIKTOK_MIN_LIFETIME_BUDGET_USD,
}


# ── Public interface ───────────────────────────────────────────────────────────

def modify_tiktok_campaign_budget(
    advertiser_id: str,
    campaign_id: str,
    new_budget_usd: float | Decimal,
    budget_type: str | None = None,
) -> dict:
    """
    Resolve a TikTok campaign's budget and pacing mode, then update it atomically.

    This is the canonical entry point for the Operator agent to mutate a TikTok
    campaign budget from a task27.v1 MMM optimization recommendation.

    Key design decisions:
    - Decimal→float conversion: Decimal(str(value)).quantize("0.01") ensures exact
      2dp rounding before passing to the TikTok API (which expects a plain float, not
      micros). This avoids floating-point drift from Python float arithmetic.
    - Pacing preservation: budget_mode is read from get_campaign() rather than
      assumed. If the campaign is running on a lifetime budget (BUDGET_MODE_TOTAL),
      the lifetime floor ($50.00) is applied instead of the daily floor.
    - budget_type override: callers may pass "daily" or "lifetime" explicitly if they
      know the pacing type ahead of time; otherwise it is derived from budget_mode.

    Args:
        advertiser_id:  TikTok advertiser ID (numeric string).
        campaign_id:    TikTok campaign ID (numeric string).
        new_budget_usd: New budget in USD. Accepts float or Decimal; rounded to
                        2 decimal places via Decimal quantize before the API call.
                        Must meet the platform minimum for the resolved budget type.
        budget_type:    Optional override for pacing type selection: "daily" or
                        "lifetime". If None, derived from the campaign's budget_mode.

    Returns:
        Dict with:
          platform, advertiser_id, campaign_id, campaign_name,
          budget_mode (raw API string), budget_type ("daily"|"lifetime"),
          previous_budget_usd (float), new_budget_usd (float), status.

    Raises:
        TikTokBudgetGuardrailError: if new_budget_usd < platform floor,
            or if the percentage change exceeds settings.max_budget_shift_pct.
        TikTokAdsError: on API-level failures.
        TikTokSetupError: if credentials are not configured.
        ApprovalRequiredError: if OPERATOR_REQUIRE_APPROVAL=true (inherited
            from update_campaign_budget via _require_approval).
    """
    # ── Exact Decimal→float — no float precision drift ─────────────────────────
    new_budget_decimal = Decimal(str(new_budget_usd)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    new_budget_float = float(new_budget_decimal)

    # ── 1. Resolve live campaign state ────────────────────────────────────────
    campaign = get_campaign(advertiser_id, campaign_id)
    current_budget = float(campaign.get("budget", 0))
    budget_mode: str = campaign.get("budget_mode", "BUDGET_MODE_DAY")
    campaign_name: str = campaign.get("campaign_name", campaign_id)
    objective: str = campaign.get("objective_type", "")

    # Derive effective budget type from budget_mode (override takes precedence)
    effective_type = budget_type or _BUDGET_MODE_TO_TYPE.get(budget_mode, "daily")
    min_floor = _MIN_FLOOR_BY_TYPE.get(effective_type, TIKTOK_MIN_DAILY_BUDGET_USD)

    log.debug(
        "tiktok_ads_operator.resolve_campaign",
        advertiser_id=advertiser_id,
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        budget_mode=budget_mode,
        effective_type=effective_type,
        current_usd=current_budget,
        proposed_usd=new_budget_float,
        min_floor=min_floor,
    )

    # ── 2. Floor enforcement ───────────────────────────────────────────────────
    if new_budget_float < min_floor:
        raise TikTokBudgetGuardrailError(
            f"Proposed budget ${new_budget_float:.2f} is below the TikTok operational "
            f"floor of ${min_floor:.2f} for {effective_type} budgets "
            f"(campaign budget_mode: {budget_mode}). "
            f"Insufficient data volume below this threshold for reliable performance "
            f"measurement. Adjust new_budget_usd or remove this channel from the package."
        )

    # ── 3. Issue update (guardrail + approval gate inside called function) ─────
    update_result = update_campaign_budget(
        advertiser_id=advertiser_id,
        campaign_id=campaign_id,
        new_budget_usd=new_budget_float,
    )

    log.info(
        "tiktok_ads_operator.budget_modified",
        advertiser_id=advertiser_id,
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        previous_usd=current_budget,
        new_usd=new_budget_float,
        budget_mode=budget_mode,
        budget_type=effective_type,
        objective=objective,
    )

    return {
        "platform":             "tiktok",
        "advertiser_id":        advertiser_id,
        "campaign_id":          campaign_id,
        "campaign_name":        campaign_name,
        "objective_type":       objective,
        "budget_mode":          budget_mode,
        "budget_type":          effective_type,
        "previous_budget_usd":  current_budget,
        "new_budget_usd":       new_budget_float,
        "status":               update_result.get("status", "updated"),
    }
