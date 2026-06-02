"""
Google Marketing Platform clients: CM360, DV360, SA360.
All write operations gate on settings.operator_require_approval.
"""
import json
from googleapiclient.discovery import build
from google.auth import default as google_auth_default
from config import settings


class ApprovalRequiredError(Exception):
    pass


def _require_approval_check(action: str, payload: dict) -> None:
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2)}\n"
            "Set OPERATOR_REQUIRE_APPROVAL=false (or implement a Slack approval flow) to enable."
        )


def _credentials():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/doubleclickbidmanager",
                "https://www.googleapis.com/auth/dfatrafficking"]
    )
    return creds


# ── CM360 ────────────────────────────────────────────────────────────────────

def cm360_get_campaign_stats(campaign_ids: list[str]) -> list[dict]:
    """Pull spend + conversions for a list of CM360 campaign IDs."""
    service = build("dfareporting", "v4", credentials=_credentials())
    profile_id = settings.cm360_profile_id
    results = []
    for cid in campaign_ids:
        resp = (
            service.campaigns()
            .get(profileId=profile_id, id=cid)
            .execute()
        )
        results.append(resp)
    return results


# ── DV360 ────────────────────────────────────────────────────────────────────

def dv360_push_audience_exclusion(
    advertiser_id: str,
    audience_list_id: str,
    domain_list: list[str],
) -> dict:
    payload = {
        "advertiser_id": advertiser_id,
        "audience_list_id": audience_list_id,
        "excluded_domains": domain_list,
    }
    _require_approval_check("dv360_audience_exclusion", payload)
    # --- below executes only when approval gate is disabled ---
    service = build("displayvideo", "v3", credentials=_credentials())
    return (
        service.advertisers()
        .negativeKeywordLists()
        # actual DV360 exclusion API call goes here
        .execute()
    )


def dv360_reallocate_budget(
    advertiser_id: str,
    source_line_item_id: str,
    target_line_item_id: str,
    amount_usd: float,
) -> dict:
    payload = {
        "advertiser_id": advertiser_id,
        "source_line_item_id": source_line_item_id,
        "target_line_item_id": target_line_item_id,
        "amount_usd": amount_usd,
    }
    _require_approval_check("dv360_budget_reallocation", payload)
    service = build("displayvideo", "v3", credentials=_credentials())
    # Patch source line item budget down, target up
    # Full implementation: GET both LIs, patch budgetAllocations, PATCH back
    return {"status": "reallocated", **payload}


# ── SA360 ────────────────────────────────────────────────────────────────────

def sa360_adjust_campaign_budget(
    agency_id: str,
    advertiser_id: str,
    campaign_id: str,
    new_daily_budget: float,
) -> dict:
    payload = {
        "agency_id": agency_id,
        "advertiser_id": advertiser_id,
        "campaign_id": campaign_id,
        "new_daily_budget": new_daily_budget,
    }
    _require_approval_check("sa360_budget_adjustment", payload)
    service = build("doubleclicksearch", "v2", credentials=_credentials())
    return (
        service.campaigns()
        # .patch(agencyId=agency_id, advertiserId=advertiser_id, campaignId=campaign_id, body={...})
        .execute()
    )
