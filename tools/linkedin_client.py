"""
LinkedIn Marketing API client.
Covers the two Operator actions: audience exclusion (DMP segments) and campaign budget updates.

API version: LinkedIn Marketing API v202404+
Docs: https://learn.microsoft.com/en-us/linkedin/marketing/

Authentication: OAuth 2.0 access token with scopes:
  r_ads, rw_ads, r_dmp_profile, rw_dmp_profile

Token expiry: 60 days. Implement token refresh using refresh_token if long-term operation is needed.

All write operations gate on settings.operator_require_approval.
"""
import hashlib
import json
import structlog
import httpx
from config import settings

log = structlog.get_logger()

LINKEDIN_API_BASE = "https://api.linkedin.com/rest"
LINKEDIN_VERSION_HEADER = "202404"     # LinkedIn versioned API header


class LinkedInAPIError(Exception):
    """Raised when the LinkedIn API returns an error."""


def _require_approval(action: str, payload: dict) -> None:
    from tools.gmp_client import ApprovalRequiredError
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending LinkedIn action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2, default=str)}"
        )


def _headers(extra: dict | None = None) -> dict:
    h = {
        "Authorization": f"Bearer {settings.linkedin_access_token}",
        "LinkedIn-Version": LINKEDIN_VERSION_HEADER,
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }
    if extra:
        h.update(extra)
    return h


def _check_credentials() -> None:
    if not settings.linkedin_access_token:
        raise LinkedInAPIError(
            "LINKEDIN_ACCESS_TOKEN must be set to use the LinkedIn client. "
            "See .env.example for OAuth setup instructions."
        )


def _encode_urn(urn: str) -> str:
    """URL-encode a LinkedIn URN for use in REST paths."""
    import urllib.parse
    return urllib.parse.quote(urn, safe="")


# ── DMP Segment management (audience exclusion) ────────────────────────────────

def list_dmp_segments() -> list[dict]:
    """List all DMP segments for the partner account."""
    _check_credentials()
    url = f"{LINKEDIN_API_BASE}/dmpSegments"
    resp = httpx.get(
        url,
        headers=_headers(),
        params={"q": "account", "account": f"urn:li:sponsoredAccount:{settings.linkedin_partner_id}"},
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    return resp.json().get("elements", [])


def create_company_exclusion_segment(name: str, description: str = "") -> dict:
    """
    Create a new DMP segment for company-level exclusion.
    Returns the segment URN which is used in add_companies_to_segment.
    """
    _check_credentials()
    _require_approval("linkedin_create_segment", {"name": name})

    url = f"{LINKEDIN_API_BASE}/dmpSegments"
    body = {
        "name":        name,
        "description": description,
        "account":     f"urn:li:sponsoredAccount:{settings.linkedin_partner_id}",
        "accessPolicy": "PRIVATE",
        "type":         "USER",
        "destinations": [{"destination": "LINKEDIN"}],
    }
    resp = httpx.post(url, headers=_headers(), json=body, timeout=30)
    _raise_for_linkedin_error(resp)
    segment_urn = resp.headers.get("x-linkedin-id") or resp.json().get("id")
    log.info("linkedin.segment_created", segment_urn=segment_urn, name=name)
    return {"segment_urn": segment_urn, "name": name}


def add_companies_to_segment(
    segment_id: str,
    company_domains: list[str],
) -> dict:
    """
    Add companies to a DMP segment by domain for audience exclusion.
    LinkedIn matches domains to company pages.

    segment_id: the numeric ID of the DMP segment (from create_company_exclusion_segment
    or list_dmp_segments).
    """
    _check_credentials()
    _require_approval(
        "linkedin_segment_add_companies",
        {"segment_id": segment_id, "domain_count": len(company_domains)}
    )

    # LinkedIn DMP uses hashed identifiers for privacy compliance
    # For company matching, use the hashedCompanyDomain schema
    url = f"{LINKEDIN_API_BASE}/dmpSegments/{segment_id}/users"
    elements = [
        {
            "action": "ADD",
            "userSchema": "HASHED_EMAIL",           # LinkedIn accepts SHA-256 hashed company emails
            "idData": [
                hashlib.sha256(d.lower().strip().encode()).hexdigest()
                for d in company_domains
            ],
        }
    ]
    resp = httpx.post(
        url,
        headers=_headers({"Content-Type": "application/json"}),
        json={"elements": elements},
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    log.info("linkedin.companies_added_to_segment", segment_id=segment_id, count=len(company_domains))
    return {
        "segment_id": segment_id,
        "domains_added": len(company_domains),
        "note": "Match rate typically 40–60% for B2B domain lists. Allow 24–48h for segment to refresh.",
    }


def add_hashed_emails_to_segment(
    segment_id: str,
    hashed_emails: list[str],
) -> dict:
    """
    Add SHA-256 hashed emails to a DMP segment.
    Higher match rate than domain matching — use when CRM emails are available.
    """
    _check_credentials()
    _require_approval(
        "linkedin_segment_add_emails",
        {"segment_id": segment_id, "email_count": len(hashed_emails)}
    )

    url = f"{LINKEDIN_API_BASE}/dmpSegments/{segment_id}/users"
    elements = [
        {
            "action": "ADD",
            "userSchema": "HASHED_EMAIL",
            "idData": hashed_emails,
        }
    ]
    resp = httpx.post(url, headers=_headers(), json={"elements": elements}, timeout=30)
    _raise_for_linkedin_error(resp)
    log.info("linkedin.emails_added_to_segment", segment_id=segment_id, count=len(hashed_emails))
    return {
        "segment_id": segment_id,
        "emails_added": len(hashed_emails),
        "note": "Allow 24–48h for segment size to update in Campaign Manager.",
    }


def apply_segment_exclusion_to_campaign(
    campaign_id: str,
    segment_id: str,
) -> dict:
    """
    Add a DMP segment as an audience exclusion on a LinkedIn campaign.
    Fetches the current campaign targeting and appends the exclusion.
    """
    _check_credentials()

    # Fetch current campaign
    campaign = get_campaign(campaign_id)
    targeting = campaign.get("targetingCriteria", {})
    exclusions = targeting.get("exclude", {}).get("and", [])

    # Add the DMP segment exclusion
    segment_urn = f"urn:li:dmpSegment:{segment_id}"
    exclusions.append({"or": {"urn:li:adTargetingFacet:audienceMatchingSegments": [segment_urn]}})

    _require_approval(
        "linkedin_campaign_exclusion",
        {"campaign_id": campaign_id, "segment_id": segment_id}
    )

    url = f"{LINKEDIN_API_BASE}/adCampaigns/{campaign_id}"
    body = {
        "targetingCriteria": {
            **targeting,
            "exclude": {"and": exclusions},
        }
    }
    resp = httpx.post(
        url,
        headers=_headers({"X-Restli-Method": "partial_update"}),
        json={"patch": {"$set": body}},
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    log.info("linkedin.exclusion_applied", campaign_id=campaign_id, segment_id=segment_id)
    return {"campaign_id": campaign_id, "segment_id": segment_id, "status": "exclusion_applied"}


# ── Campaign budget management ─────────────────────────────────────────────────

def get_campaign(campaign_id: str) -> dict:
    """Fetch campaign details including budget."""
    _check_credentials()
    url = f"{LINKEDIN_API_BASE}/adCampaigns/{campaign_id}"
    resp = httpx.get(
        url,
        headers=_headers(),
        params={
            "fields": "id,name,status,type,costType,dailyBudget,totalBudget,targetingCriteria,objectiveType",
        },
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    return resp.json()


def update_campaign_daily_budget(
    campaign_id: str,
    new_daily_budget_usd: float,
) -> dict:
    """
    Update a LinkedIn campaign's daily budget.
    LinkedIn budget amounts are in the campaign's configured currency (usually USD).
    Enforces max_budget_shift_pct guardrail.
    """
    _check_credentials()

    current = get_campaign(campaign_id)
    current_budget = current.get("dailyBudget", {})
    current_amount = float(current_budget.get("amount", 0))

    if current_amount > 0:
        change_pct = abs(new_daily_budget_usd - current_amount) / current_amount * 100
        if change_pct > settings.max_budget_shift_pct:
            raise ValueError(
                f"Budget change of {change_pct:.1f}% exceeds guardrail of "
                f"{settings.max_budget_shift_pct}%. "
                f"Current: ${current_amount:.0f}, Proposed: ${new_daily_budget_usd:.0f}."
            )

    _require_approval(
        "linkedin_campaign_budget_update",
        {
            "campaign_id": campaign_id,
            "current_daily_budget_usd": current_amount,
            "new_daily_budget_usd": new_daily_budget_usd,
        }
    )

    currency = current_budget.get("currencyCode", "USD")
    url = f"{LINKEDIN_API_BASE}/adCampaigns/{campaign_id}"
    resp = httpx.post(
        url,
        headers=_headers({"X-Restli-Method": "partial_update"}),
        json={
            "patch": {
                "$set": {
                    "dailyBudget": {
                        "amount": str(new_daily_budget_usd),
                        "currencyCode": currency,
                    }
                }
            }
        },
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    log.info(
        "linkedin.budget_updated",
        campaign_id=campaign_id,
        old_usd=current_amount,
        new_usd=new_daily_budget_usd,
    )
    return {
        "campaign_id": campaign_id,
        "previous_daily_budget_usd": current_amount,
        "new_daily_budget_usd": new_daily_budget_usd,
        "currency": currency,
    }


# ── Reporting ──────────────────────────────────────────────────────────────────

def get_campaign_analytics(
    campaign_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Fetch campaign analytics for a date range.
    start_date / end_date: "YYYY-MM-DD"
    """
    _check_credentials()
    start = start_date.replace("-", "/")
    end = end_date.replace("-", "/")

    url = f"{LINKEDIN_API_BASE}/adAnalytics"
    resp = httpx.get(
        url,
        headers=_headers(),
        params={
            "q":          "analytics",
            "pivot":      "CAMPAIGN",
            "dateRange.start.year":  start.split("/")[0],
            "dateRange.start.month": start.split("/")[1],
            "dateRange.start.day":   start.split("/")[2],
            "dateRange.end.year":    end.split("/")[0],
            "dateRange.end.month":   end.split("/")[1],
            "dateRange.end.day":     end.split("/")[2],
            "campaigns":  f"urn:li:sponsoredCampaign:{campaign_id}",
            "fields":     "impressions,clicks,costInLocalCurrency,leads,conversions,videoCompletions",
            "timeGranularity": "DAILY",
        },
        timeout=30,
    )
    _raise_for_linkedin_error(resp)
    return resp.json().get("elements", [])


# ── Error handling ─────────────────────────────────────────────────────────────

def _raise_for_linkedin_error(resp: httpx.Response) -> None:
    if resp.status_code >= 400:
        try:
            body = resp.json()
            msg = body.get("message") or body.get("error", {}).get("message") or resp.text
            code = body.get("status", resp.status_code)
        except Exception:
            msg = resp.text
            code = resp.status_code
        raise LinkedInAPIError(f"LinkedIn API error {code}: {msg}")
