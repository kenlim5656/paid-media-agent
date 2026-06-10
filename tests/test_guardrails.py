# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""±MAX_BUDGET_SHIFT_PCT guardrail math (tools/google_ads_client.update_campaign_budget)."""
import pytest

from tools.gmp_client import ApprovalRequiredError
from tools.google_ads_client import (
    GoogleAdsBudgetGuardrailError,
    update_campaign_budget,
)

RESOURCE = "customers/123/campaignBudgets/456"


def test_shift_above_cap_rejected():
    # 11% > 10% cap — must raise before any approval gate or API call
    with pytest.raises(GoogleAdsBudgetGuardrailError, match="11.0%"):
        update_campaign_budget("123", RESOURCE, new_amount_usd=111.0, current_amount_usd=100.0)


def test_decrease_above_cap_rejected():
    # Direction doesn't matter: |−11%| also breaches
    with pytest.raises(GoogleAdsBudgetGuardrailError):
        update_campaign_budget("123", RESOURCE, new_amount_usd=89.0, current_amount_usd=100.0)


def test_shift_exactly_at_cap_passes_guardrail():
    # 10.0% is allowed (cap is exclusive); the next gate is the approval
    # requirement, which proves the guardrail passed without an API call.
    with pytest.raises(ApprovalRequiredError):
        update_campaign_budget("123", RESOURCE, new_amount_usd=110.0, current_amount_usd=100.0)


def test_shift_below_cap_passes_guardrail():
    with pytest.raises(ApprovalRequiredError):
        update_campaign_budget("123", RESOURCE, new_amount_usd=105.0, current_amount_usd=100.0)


def test_no_current_budget_skips_pct_check():
    # Without current_amount_usd there is no percentage to evaluate — the
    # approval gate is the first stop.
    with pytest.raises(ApprovalRequiredError):
        update_campaign_budget("123", RESOURCE, new_amount_usd=500.0)
