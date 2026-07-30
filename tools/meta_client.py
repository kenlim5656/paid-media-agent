# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Meta Marketing API client.
Covers the two Operator actions: audience exclusion and campaign budget updates.

API version: v20.0
Docs: https://developers.facebook.com/docs/marketing-apis

Authentication: System User long-lived access token (recommended for server-to-server).
Permissions needed: ads_management, ads_read.

All write operations gate on settings.operator_require_approval.
"""
import hashlib
import json

import httpx
import structlog

from config import settings
from tools.http_retry import get_with_retry

log = structlog.get_logger()

META_API_BASE = "https://graph.facebook.com/v20.0"
META_GRAPH_VERSION = "v20.0"


class MetaAPIError(Exception):
    """Raised when the Meta Graph API returns an error."""


def _require_approval(action: str, payload: dict) -> None:
    from tools.gmp_client import ApprovalRequiredError
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending Meta action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2, default=str)}"
        )


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.meta_access_token}",
        "Content-Type": "application/json",
    }


def _check_credentials() -> None:
    if not settings.meta_access_token or not settings.meta_ad_account_id:
        raise MetaAPIError(
            "META_ACCESS_TOKEN and META_AD_ACCOUNT_ID must be set to use the Meta client. "
            "See .env.example for setup instructions."
        )


# ── Custom Audience management ─────────────────────────────────────────────────

def get_custom_audiences() -> list[dict]:
    """List all custom audiences in the ad account."""
    _check_credentials()
    url = f"{META_API_BASE}/{settings.meta_ad_account_id}/customaudiences"
    resp = get_with_retry(
        url,
        params={
            "fields": "id,name,approximate_count,description,subtype",
            "access_token": settings.meta_access_token,
        },
        timeout=30,
    )
    _raise_for_meta_error(resp)
    return resp.json().get("data", [])


def add_domains_to_exclusion_audience(
    audience_id: str,
    domains: list[str],
) -> dict:
    """
    Add company domains to a Custom Audience for ad exclusion.
    Uses the EXTERN_ID schema — domains are hashed and added as external identifiers.

    NOTE: For domain-based exclusion, the audience must be a CUSTOM audience type.
    The most reliable approach for B2B domain suppression is to:
    1. Look up employee emails per domain from CRM
    2. Add hashed emails via the customer_file schema (below)
    Use this function when only domains are available (no email CRM data).
    """
    _check_credentials()
    payload = {
        "schema": "EXTERN_ID",
        "data": [hashlib.sha256(d.lower().strip().encode()).hexdigest() for d in domains],
    }
    _require_approval("meta_audience_domain_add", {"audience_id": audience_id, "domain_count": len(domains)})

    url = f"{META_API_BASE}/{audience_id}/users"
    resp = httpx.post(
        url,
        params={"access_token": settings.meta_access_token},
        json=payload,
        timeout=30,
    )
    _raise_for_meta_error(resp)
    result = resp.json()
    log.info("meta.audience_domains_added", audience_id=audience_id, count=len(domains))
    return {"audience_id": audience_id, "domains_added": len(domains), "response": result}


def add_hashed_emails_to_exclusion_audience(
    audience_id: str,
    hashed_emails: list[str],
) -> dict:
    """
    Add SHA-256 hashed emails to a Custom Audience.
    This is the highest-match-rate method for B2B audience suppression.
    Pass emails already hashed (SHA-256, normalized lowercase + trimmed).
    """
    _check_credentials()
    payload = {
        "schema": "EMAIL_SHA256",
        "data": hashed_emails,
    }
    _require_approval(
        "meta_audience_email_add",
        {"audience_id": audience_id, "email_count": len(hashed_emails)}
    )

    url = f"{META_API_BASE}/{audience_id}/users"
    resp = httpx.post(
        url,
        params={"access_token": settings.meta_access_token},
        json=payload,
        timeout=30,
    )
    _raise_for_meta_error(resp)
    result = resp.json()
    log.info("meta.audience_emails_added", audience_id=audience_id, count=len(hashed_emails))
    return {"audience_id": audience_id, "emails_added": len(hashed_emails), "response": result}


def create_exclusion_audience(name: str, description: str = "") -> dict:
    """Create a new Custom Audience to use as an exclusion list."""
    _check_credentials()
    _require_approval("meta_create_audience", {"name": name})

    url = f"{META_API_BASE}/{settings.meta_ad_account_id}/customaudiences"
    resp = httpx.post(
        url,
        params={"access_token": settings.meta_access_token},
        json={
            "name": name,
            "description": description,
            "subtype": "CUSTOM",
            "customer_file_source": "PARTNER_PROVIDED_ONLY",
        },
        timeout=30,
    )
    _raise_for_meta_error(resp)
    result = resp.json()
    log.info("meta.audience_created", audience_id=result.get("id"), name=name)
    return result


# ── Budget management ──────────────────────────────────────────────────────────

def get_campaign(campaign_id: str) -> dict:
    """Fetch campaign details including current budget."""
    _check_credentials()
    url = f"{META_API_BASE}/{campaign_id}"
    resp = get_with_retry(
        url,
        params={
            "fields": "id,name,status,objective,daily_budget,lifetime_budget,budget_remaining",
            "access_token": settings.meta_access_token,
        },
        timeout=30,
    )
    _raise_for_meta_error(resp)
    return resp.json()


def get_ad_set(ad_set_id: str) -> dict:
    """Fetch ad set details including current budget and targeting."""
    _check_credentials()
    url = f"{META_API_BASE}/{ad_set_id}"
    resp = get_with_retry(
        url,
        params={
            "fields": "id,name,status,daily_budget,lifetime_budget,optimization_goal,campaign_id",
            "access_token": settings.meta_access_token,
        },
        timeout=30,
    )
    _raise_for_meta_error(resp)
    return resp.json()


def update_campaign_daily_budget(
    campaign_id: str,
    new_daily_budget_cents: int,
) -> dict:
    """
    Update a campaign's daily budget.
    Meta API uses cents (USD × 100). E.g. $500/day = 50000.
    Enforces max_budget_shift_pct guardrail.
    """
    _check_credentials()

    # Fetch current budget for guardrail check
    current = get_campaign(campaign_id)
    current_budget_cents = int(current.get("daily_budget", 0))

    if current_budget_cents > 0:
        change_pct = abs(new_daily_budget_cents - current_budget_cents) / current_budget_cents * 100
        if change_pct > settings.max_budget_shift_pct:
            raise ValueError(
                f"Budget change of {change_pct:.1f}% exceeds guardrail of "
                f"{settings.max_budget_shift_pct}%. "
                f"Current: ${current_budget_cents/100:.0f}, "
                f"Proposed: ${new_daily_budget_cents/100:.0f}."
            )

    _require_approval(
        "meta_campaign_budget_update",
        {
            "campaign_id": campaign_id,
            "current_daily_budget_usd": current_budget_cents / 100,
            "new_daily_budget_usd": new_daily_budget_cents / 100,
        }
    )

    url = f"{META_API_BASE}/{campaign_id}"
    resp = httpx.post(
        url,
        params={"access_token": settings.meta_access_token},
        json={"daily_budget": str(new_daily_budget_cents)},
        timeout=30,
    )
    _raise_for_meta_error(resp)
    log.info(
        "meta.budget_updated",
        campaign_id=campaign_id,
        old_cents=current_budget_cents,
        new_cents=new_daily_budget_cents,
    )
    return {
        "campaign_id": campaign_id,
        "previous_daily_budget_usd": current_budget_cents / 100,
        "new_daily_budget_usd": new_daily_budget_cents / 100,
        "response": resp.json(),
    }


def update_ad_set_daily_budget(
    ad_set_id: str,
    new_daily_budget_cents: int,
) -> dict:
    """Update an ad set's daily budget. Enforces guardrail."""
    _check_credentials()

    current = get_ad_set(ad_set_id)
    current_budget_cents = int(current.get("daily_budget", 0))

    if current_budget_cents > 0:
        change_pct = abs(new_daily_budget_cents - current_budget_cents) / current_budget_cents * 100
        if change_pct > settings.max_budget_shift_pct:
            raise ValueError(
                f"Budget change of {change_pct:.1f}% exceeds guardrail of {settings.max_budget_shift_pct}%."
            )

    _require_approval(
        "meta_ad_set_budget_update",
        {
            "ad_set_id": ad_set_id,
            "current_daily_budget_usd": current_budget_cents / 100,
            "new_daily_budget_usd": new_daily_budget_cents / 100,
        }
    )

    url = f"{META_API_BASE}/{ad_set_id}"
    resp = httpx.post(
        url,
        params={"access_token": settings.meta_access_token},
        json={"daily_budget": str(new_daily_budget_cents)},
        timeout=30,
    )
    _raise_for_meta_error(resp)
    log.info("meta.ad_set_budget_updated", ad_set_id=ad_set_id, new_cents=new_daily_budget_cents)
    return {
        "ad_set_id": ad_set_id,
        "previous_daily_budget_usd": current_budget_cents / 100,
        "new_daily_budget_usd": new_daily_budget_cents / 100,
        "response": resp.json(),
    }


# ── Reporting ──────────────────────────────────────────────────────────────────

def get_campaign_insights(
    campaign_id: str,
    date_preset: str = "last_30d",
) -> dict:
    """
    Pull campaign-level performance insights.
    date_preset options: today, yesterday, last_7d, last_14d, last_30d,
                         last_quarter, last_year, this_month, last_month
    """
    _check_credentials()
    url = f"{META_API_BASE}/{campaign_id}/insights"
    resp = get_with_retry(
        url,
        params={
            "fields": "impressions,clicks,spend,actions,action_values,reach,frequency,cpc,cpm,ctr",
            "date_preset": date_preset,
            "access_token": settings.meta_access_token,
        },
        timeout=30,
    )
    _raise_for_meta_error(resp)
    data = resp.json().get("data", [])
    return data[0] if data else {}


# ── Error handling ─────────────────────────────────────────────────────────────

def _raise_for_meta_error(resp: httpx.Response) -> None:
    if resp.status_code >= 400:
        try:
            error = resp.json().get("error", {})
            msg = error.get("message", resp.text)
            code = error.get("code", resp.status_code)
        except Exception:
            msg = resp.text
            code = resp.status_code
        raise MetaAPIError(f"Meta API error {code}: {msg}")
