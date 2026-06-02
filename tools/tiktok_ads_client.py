# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
TikTok Ads Marketing API client — Task 20.

Covers:
  - Dual-mode authentication (headless Cloud Run env vars + local ~/tiktok-ads.yaml)
  - Multi-tenant advertiser ID resolution (comma-separated list or single ID)
  - Integrated reporting at campaign, ad, and geo granularity
  - Campaign budget read + update with guardrail enforcement
  - Custom audience creation and hashed-email upload for domain suppression
  - Budget reallocation across campaigns

API version: v1.3 (centralized — override TIKTOK_API_VERSION env var)
Docs: https://business-api.tiktok.com/portal/docs

Authentication modes:
  Full Mode (headless / Cloud Run / GCP Secret Manager):
    Set TIKTOK_ACCESS_TOKEN and TIKTOK_ADVERTISER_IDS (comma-separated).
    Cloud Run injects these from Secret Manager at startup.

  Simple Mode (local practitioner):
    Leave env vars blank and run: python tools/setup_tiktok_ads.py
    This opens a browser OAuth flow and writes ~/tiktok-ads.yaml automatically.

Financial fields: all monetary values are converted to Python Decimal to match
the NUMERIC BigQuery schema defined in paid-media-schema (Task 28).

Geographic alignment: get_geo_performance() pulls country_id and province_id
alongside raw spend and impressions — the required inputs for Meridian MMM (Task 27).
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import structlog

from config import settings

log = structlog.get_logger()

# ── Version & base URL ─────────────────────────────────────────────────────────

TIKTOK_API_VERSION: str = os.getenv("TIKTOK_API_VERSION", "v1.3")
TIKTOK_API_BASE: str = f"https://business-api.tiktok.com/open_api/{TIKTOK_API_VERSION}"

# v1.3 is the current stable version (as of 2026-06).
# To upgrade: set TIKTOK_API_VERSION env var — no code changes needed anywhere else.

YAML_FILENAME = "tiktok-ads.yaml"

# ── Exceptions ─────────────────────────────────────────────────────────────────


class TikTokSetupError(Exception):
    """Raised when credentials are missing or the YAML config is invalid."""


class TikTokAdsError(Exception):
    """Raised when the TikTok Marketing API returns a non-zero error code."""


class TikTokBudgetGuardrailError(Exception):
    """Raised when a proposed budget change exceeds max_budget_shift_pct."""


# ── Auth — dual-mode credential resolution ────────────────────────────────────


def _get_context() -> tuple[str, list[str]]:
    """
    Resolve TikTok credentials and return (access_token, advertiser_ids).

    Full Mode (Cloud Run / headless):
        Reads TIKTOK_ACCESS_TOKEN and TIKTOK_ADVERTISER_IDS from environment.
        TIKTOK_ADVERTISER_IDS may be a comma-separated list (e.g. "111,222,333")
        or a single ID. Falls back to TIKTOK_ADVERTISER_ID for single-account setups.

    Simple Mode (local):
        Reads ~/tiktok-ads.yaml written by tools/setup_tiktok_ads.py.
        Expects keys: access_token, advertiser_ids (list).

    Raises TikTokSetupError with actionable instructions if neither mode succeeds.
    """
    # Full Mode: env vars present
    access_token = settings.tiktok_access_token.strip()
    advertiser_ids_env = os.getenv("TIKTOK_ADVERTISER_IDS", "").strip()

    if access_token and advertiser_ids_env:
        ids = [aid.strip() for aid in advertiser_ids_env.split(",") if aid.strip()]
        log.debug("tiktok.auth.full_mode", advertiser_count=len(ids))
        return access_token, ids

    # Also accept single TIKTOK_ADVERTISER_ID in Full Mode
    single_id = settings.tiktok_advertiser_id.strip()
    if access_token and single_id:
        log.debug("tiktok.auth.full_mode_single", advertiser_id=single_id)
        return access_token, [single_id]

    # Simple Mode: load from ~/tiktok-ads.yaml
    yaml_path = Path.home() / YAML_FILENAME
    if yaml_path.exists():
        try:
            import yaml  # type: ignore[import]
        except ImportError:
            raise TikTokSetupError(
                "pyyaml is required to load ~/tiktok-ads.yaml. "
                "Install with: pip install pyyaml"
            )
        config = yaml.safe_load(yaml_path.read_text())
        token = config.get("access_token", "").strip()
        ids_raw = config.get("advertiser_ids", [])
        ids = [str(i).strip() for i in ids_raw if str(i).strip()] if isinstance(ids_raw, list) else [str(ids_raw).strip()]
        if token and ids:
            log.debug("tiktok.auth.simple_mode", yaml_path=str(yaml_path), advertiser_count=len(ids))
            return token, ids

    raise TikTokSetupError(
        "No TikTok Ads credentials found.\n\n"
        "Full Mode (Cloud Run / headless):\n"
        "  Set TIKTOK_ACCESS_TOKEN and TIKTOK_ADVERTISER_IDS environment variables.\n"
        "  TIKTOK_ADVERTISER_IDS accepts a comma-separated list of advertiser IDs.\n\n"
        "Simple Mode (local development):\n"
        "  Run the interactive setup: python tools/setup_tiktok_ads.py\n"
        "  This opens a browser OAuth flow and writes ~/tiktok-ads.yaml.\n\n"
        "Getting credentials:\n"
        "  App ID / Secret: https://ads.tiktok.com/marketing_api/apps\n"
        "  Advertiser IDs: TikTok Ads Manager → Account → Advertiser ID\n"
        "  Access Token: generated by setup_tiktok_ads.py or via the OAuth portal"
    )


def _headers(access_token: str) -> dict:
    return {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }


def _require_approval(action: str, payload: dict) -> None:
    from tools.gmp_client import ApprovalRequiredError
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending TikTok action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2, default=str)}"
        )


# ── API response handling ──────────────────────────────────────────────────────


def _raise_for_tiktok_error(resp: httpx.Response) -> None:
    """Raise on HTTP-level errors before checking the TikTok response body."""
    if resp.status_code >= 400:
        try:
            body = resp.json()
            msg = body.get("message", resp.text)
            code = body.get("code", resp.status_code)
        except Exception:
            msg = resp.text
            code = resp.status_code
        raise TikTokAdsError(f"TikTok API HTTP error {code}: {msg}")


def _check_response(body: dict) -> Any:
    """
    Check TikTok's application-level response code.
    TikTok wraps all responses in {"code": 0, "message": "OK", "data": {...}}.
    Code 0 = success. Any other code is an application error.
    Returns the data payload on success.
    """
    code = body.get("code", -1)
    if code != 0:
        raise TikTokAdsError(
            f"TikTok API error {code}: {body.get('message', 'unknown')} "
            f"(request_id: {body.get('request_id', 'n/a')})"
        )
    return body.get("data", {})


# ── Financial field conversion ─────────────────────────────────────────────────


def _to_numeric(value: Any) -> Decimal:
    """
    Convert a TikTok API monetary value to Decimal.
    TikTok returns spend/cpc/cpm as plain floats or numeric strings (not micros).
    Using Decimal(str(value)) avoids float precision drift.
    Matches the NUMERIC BigQuery schema from Task 28.
    """
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _to_int(value: Any) -> int:
    """Convert impressions, clicks, conversions to int."""
    if value is None or value == "":
        return 0
    return int(float(str(value)))


# ── Reporting ──────────────────────────────────────────────────────────────────


def _integrated_report(
    advertiser_id: str,
    access_token: str,
    data_level: str,
    dimensions: list[str],
    metrics: list[str],
    date_from: str,
    date_to: str,
    filters: list[dict] | None = None,
    page_size: int = 100,
) -> list[dict]:
    """
    Core wrapper for POST /open_api/{version}/report/integrated/get/

    data_level options:
        "AUCTION_CAMPAIGN"  — campaign-level roll-up
        "AUCTION_ADGROUP"   — ad group level
        "AUCTION_AD"        — ad level

    Returns the raw list of row dicts from data.list.
    All pagination is handled internally (fetches all pages).
    """
    url = f"{TIKTOK_API_BASE}/report/integrated/get/"
    rows: list[dict] = []
    page = 1

    while True:
        body: dict = {
            "advertiser_id": advertiser_id,
            "report_type":   "BASIC",
            "data_level":    data_level,
            "dimensions":    dimensions,
            "metrics":       metrics,
            "start_date":    date_from,
            "end_date":      date_to,
            "page":          page,
            "page_size":     page_size,
        }
        if filters:
            body["filters"] = filters

        resp = httpx.post(url, headers=_headers(access_token), json=body, timeout=60)
        _raise_for_tiktok_error(resp)
        data = _check_response(resp.json())

        page_data = data.get("list", [])
        rows.extend(page_data)

        page_info = data.get("page_info", {})
        total_number = page_info.get("total_number", 0)
        if len(rows) >= total_number or not page_data:
            break
        page += 1

    return rows


def get_campaign_performance(
    advertiser_id: str,
    date_from: str,
    date_to: str,
    campaign_ids: list[str] | None = None,
) -> list[dict]:
    """
    Pull daily campaign-level performance metrics from the integrated report.

    Args:
        advertiser_id: TikTok advertiser account ID.
        date_from: Start date, "YYYY-MM-DD".
        date_to:   End date, "YYYY-MM-DD".
        campaign_ids: Optional list of campaign IDs to filter; None = all campaigns.

    Returns list of dicts with schema-aligned field types:
        campaign_id (str), stat_time_day (str),
        spend (Decimal), cpc (Decimal), cpm (Decimal),
        impressions (int), clicks (int), conversions (int),
        cost_per_conversion (Decimal), ctr (Decimal)

    Maps to the platform_daily_performance table in paid-media-schema (Task 28).
    """
    access_token, _ = _get_context()

    filters = []
    if campaign_ids:
        filters.append({"field_name": "campaign_id", "filter_type": "IN", "filter_value": json.dumps(campaign_ids)})

    rows = _integrated_report(
        advertiser_id=advertiser_id,
        access_token=access_token,
        data_level="AUCTION_CAMPAIGN",
        dimensions=["campaign_id", "stat_time_day"],
        metrics=[
            "spend", "impressions", "clicks", "cpc", "cpm", "ctr",
            "conversion", "cost_per_conversion",
            "campaign_name",
        ],
        date_from=date_from,
        date_to=date_to,
        filters=filters or None,
    )

    result = []
    for row in rows:
        dims = row.get("dimensions", {})
        m = row.get("metrics", {})
        result.append({
            "advertiser_id":      advertiser_id,
            "campaign_id":        dims.get("campaign_id"),
            "campaign_name":      m.get("campaign_name"),
            "stat_time_day":      dims.get("stat_time_day"),
            # Financial fields → Decimal (NUMERIC in BigQuery)
            "spend":              _to_numeric(m.get("spend")),
            "cpc":                _to_numeric(m.get("cpc")),
            "cpm":                _to_numeric(m.get("cpm")),
            "ctr":                _to_numeric(m.get("ctr")),
            "cost_per_conversion": _to_numeric(m.get("cost_per_conversion")),
            # Count fields → INT64 in BigQuery
            "impressions":        _to_int(m.get("impressions")),
            "clicks":             _to_int(m.get("clicks")),
            "conversions":        _to_int(m.get("conversion")),
        })

    log.info(
        "tiktok.campaign_performance.fetched",
        advertiser_id=advertiser_id,
        rows=len(result),
        date_from=date_from,
        date_to=date_to,
    )
    return result


def get_ad_performance(
    advertiser_id: str,
    date_from: str,
    date_to: str,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Pull daily ad-level performance metrics from the integrated report.

    Args:
        advertiser_id: TikTok advertiser account ID.
        date_from: "YYYY-MM-DD"
        date_to:   "YYYY-MM-DD"
        campaign_id: Optional filter to a single campaign.

    Returns list of dicts with ad_id, ad_name, adgroup_id, campaign_id,
    stat_time_day, and the same financial/count fields as get_campaign_performance.
    """
    access_token, _ = _get_context()

    filters = []
    if campaign_id:
        filters.append({"field_name": "campaign_id", "filter_type": "IN", "filter_value": json.dumps([campaign_id])})

    rows = _integrated_report(
        advertiser_id=advertiser_id,
        access_token=access_token,
        data_level="AUCTION_AD",
        dimensions=["ad_id", "adgroup_id", "campaign_id", "stat_time_day"],
        metrics=[
            "spend", "impressions", "clicks", "cpc", "cpm", "ctr",
            "conversion", "cost_per_conversion",
            "ad_name", "adgroup_name", "campaign_name",
        ],
        date_from=date_from,
        date_to=date_to,
        filters=filters or None,
    )

    result = []
    for row in rows:
        dims = row.get("dimensions", {})
        m = row.get("metrics", {})
        result.append({
            "advertiser_id":      advertiser_id,
            "campaign_id":        dims.get("campaign_id"),
            "campaign_name":      m.get("campaign_name"),
            "adgroup_id":         dims.get("adgroup_id"),
            "adgroup_name":       m.get("adgroup_name"),
            "ad_id":              dims.get("ad_id"),
            "ad_name":            m.get("ad_name"),
            "stat_time_day":      dims.get("stat_time_day"),
            "spend":              _to_numeric(m.get("spend")),
            "cpc":                _to_numeric(m.get("cpc")),
            "cpm":                _to_numeric(m.get("cpm")),
            "ctr":                _to_numeric(m.get("ctr")),
            "cost_per_conversion": _to_numeric(m.get("cost_per_conversion")),
            "impressions":        _to_int(m.get("impressions")),
            "clicks":             _to_int(m.get("clicks")),
            "conversions":        _to_int(m.get("conversion")),
        })

    log.info(
        "tiktok.ad_performance.fetched",
        advertiser_id=advertiser_id,
        rows=len(result),
        campaign_id=campaign_id,
    )
    return result


def get_geo_performance(
    advertiser_id: str,
    date_from: str,
    date_to: str,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Pull daily geo-level performance at country + province granularity.

    This is the primary Meridian MMM input function (Task 27).
    Dimensions include country_id and province_id, paired with raw impressions
    and spend per day — exactly the geographic reach + cost inputs that
    Meridian uses to build the media contribution surface.

    Args:
        advertiser_id: TikTok advertiser account ID.
        date_from: "YYYY-MM-DD"
        date_to:   "YYYY-MM-DD"
        campaign_id: Optional campaign filter.

    Returns list of dicts with:
        campaign_id, stat_time_day, country_id, province_id,
        impressions (INT64), spend (Decimal / NUMERIC)

    NOTE (Task 27 hook):
        When feeding Meridian, aggregate to weekly if daily geo data is sparse
        (< 1k impressions/day per region risks zero-inflation in the MMM prior).
        Configure XLA_FLAGS on Cloud Run to stay within the 60-minute JAX timeout.
    """
    access_token, _ = _get_context()

    filters = []
    if campaign_id:
        filters.append({"field_name": "campaign_id", "filter_type": "IN", "filter_value": json.dumps([campaign_id])})

    rows = _integrated_report(
        advertiser_id=advertiser_id,
        access_token=access_token,
        data_level="AUCTION_CAMPAIGN",
        dimensions=["campaign_id", "stat_time_day", "country_id", "province_id"],
        metrics=["spend", "impressions"],
        date_from=date_from,
        date_to=date_to,
        filters=filters or None,
    )

    result = []
    for row in rows:
        dims = row.get("dimensions", {})
        m = row.get("metrics", {})
        result.append({
            "advertiser_id": advertiser_id,
            "campaign_id":   dims.get("campaign_id"),
            "stat_time_day": dims.get("stat_time_day"),
            "country_id":    dims.get("country_id"),
            "province_id":   dims.get("province_id"),
            "impressions":   _to_int(m.get("impressions")),
            "spend":         _to_numeric(m.get("spend")),
        })

    log.info(
        "tiktok.geo_performance.fetched",
        advertiser_id=advertiser_id,
        rows=len(result),
        date_from=date_from,
        date_to=date_to,
    )
    return result


# ── Campaign management ────────────────────────────────────────────────────────


def get_campaign(advertiser_id: str, campaign_id: str) -> dict:
    """
    Fetch campaign details including budget, status, and objective.
    Returns the raw campaign object from the TikTok API.
    """
    access_token, _ = _get_context()
    url = f"{TIKTOK_API_BASE}/campaign/get/"
    resp = httpx.get(
        url,
        headers=_headers(access_token),
        params={
            "advertiser_id": advertiser_id,
            "campaign_ids":  json.dumps([campaign_id]),
            "fields":        json.dumps([
                "campaign_id", "campaign_name", "status", "objective_type",
                "budget", "budget_mode",
            ]),
        },
        timeout=30,
    )
    _raise_for_tiktok_error(resp)
    data = _check_response(resp.json())
    campaigns = data.get("list", [])
    if not campaigns:
        raise TikTokAdsError(f"Campaign {campaign_id} not found for advertiser {advertiser_id}.")
    return campaigns[0]


def update_campaign_budget(
    advertiser_id: str,
    campaign_id: str,
    new_budget_usd: float,
) -> dict:
    """
    Update a campaign's daily budget.

    TikTok budgets are set in the advertiser's billing currency (usually USD).
    budget_mode defaults to BUDGET_MODE_DAY (daily). For lifetime budgets,
    use budget_mode="BUDGET_MODE_TOTAL".

    Enforces settings.max_budget_shift_pct guardrail before any write.
    Gated by OPERATOR_REQUIRE_APPROVAL.
    """
    access_token, _ = _get_context()

    current = get_campaign(advertiser_id, campaign_id)
    current_budget = float(current.get("budget", 0))

    if current_budget > 0:
        change_pct = abs(new_budget_usd - current_budget) / current_budget * 100
        if change_pct > settings.max_budget_shift_pct:
            raise TikTokBudgetGuardrailError(
                f"Budget change of {change_pct:.1f}% exceeds guardrail of "
                f"{settings.max_budget_shift_pct}%. "
                f"Current: ${current_budget:.2f}, Proposed: ${new_budget_usd:.2f}. "
                "Adjust new_budget_usd or raise MAX_BUDGET_SHIFT_PCT."
            )

    _require_approval(
        "tiktok_campaign_budget_update",
        {
            "advertiser_id":      advertiser_id,
            "campaign_id":        campaign_id,
            "current_budget_usd": current_budget,
            "new_budget_usd":     new_budget_usd,
        },
    )

    url = f"{TIKTOK_API_BASE}/campaign/update/"
    resp = httpx.post(
        url,
        headers=_headers(access_token),
        json={
            "advertiser_id": advertiser_id,
            "campaign_id":   campaign_id,
            "budget":        new_budget_usd,
            "budget_mode":   current.get("budget_mode", "BUDGET_MODE_DAY"),
        },
        timeout=30,
    )
    _raise_for_tiktok_error(resp)
    _check_response(resp.json())

    log.info(
        "tiktok.budget_updated",
        advertiser_id=advertiser_id,
        campaign_id=campaign_id,
        old_usd=current_budget,
        new_usd=new_budget_usd,
    )
    return {
        "advertiser_id":      advertiser_id,
        "campaign_id":        campaign_id,
        "previous_budget_usd": current_budget,
        "new_budget_usd":     new_budget_usd,
        "budget_mode":        current.get("budget_mode", "BUDGET_MODE_DAY"),
        "status":             "updated",
    }


def reallocate_campaign_budget(
    advertiser_id: str,
    source_campaign_id: str,
    target_campaign_id: str,
    amount_usd: float,
) -> dict:
    """
    Move a budget amount from one campaign to another within the same advertiser.

    Fetches current budgets for both campaigns, applies the shift,
    enforces the max_budget_shift_pct guardrail against the source campaign's
    current budget, then writes both updates.

    Minimum post-reduction budget is $1.00 (TikTok enforces a floor).
    Gated by OPERATOR_REQUIRE_APPROVAL.
    """
    access_token, _ = _get_context()  # noqa: F841 — validates credentials before fetching

    source = get_campaign(advertiser_id, source_campaign_id)
    target = get_campaign(advertiser_id, target_campaign_id)

    source_budget = float(source.get("budget", 0))
    target_budget = float(target.get("budget", 0))

    if source_budget > 0:
        change_pct = amount_usd / source_budget * 100
        if change_pct > settings.max_budget_shift_pct:
            raise TikTokBudgetGuardrailError(
                f"Reallocation of ${amount_usd:.2f} is {change_pct:.1f}% of source campaign "
                f"budget (${source_budget:.2f}), exceeding the {settings.max_budget_shift_pct}% "
                "guardrail. Reduce amount_usd or raise MAX_BUDGET_SHIFT_PCT."
            )

    new_source_budget = max(1.0, source_budget - amount_usd)
    new_target_budget = target_budget + amount_usd

    _require_approval(
        "tiktok_budget_reallocation",
        {
            "advertiser_id":       advertiser_id,
            "source_campaign_id":  source_campaign_id,
            "target_campaign_id":  target_campaign_id,
            "source_before":       source_budget,
            "source_after":        new_source_budget,
            "target_before":       target_budget,
            "target_after":        new_target_budget,
            "amount_usd":          amount_usd,
        },
    )

    r_source = update_campaign_budget(advertiser_id, source_campaign_id, new_source_budget)
    r_target = update_campaign_budget(advertiser_id, target_campaign_id, new_target_budget)

    log.info(
        "tiktok.budget_reallocated",
        advertiser_id=advertiser_id,
        source=source_campaign_id,
        target=target_campaign_id,
        amount_usd=amount_usd,
    )
    return {
        "advertiser_id":      advertiser_id,
        "source_campaign":    r_source,
        "target_campaign":    r_target,
        "amount_moved_usd":   amount_usd,
    }


# ── Audience management ────────────────────────────────────────────────────────


def list_custom_audiences(advertiser_id: str) -> list[dict]:
    """List all custom audiences for the advertiser."""
    access_token, _ = _get_context()
    url = f"{TIKTOK_API_BASE}/dmp/custom_audience/list/"
    resp = httpx.get(
        url,
        headers=_headers(access_token),
        params={
            "advertiser_id": advertiser_id,
            "page":          1,
            "page_size":     100,
        },
        timeout=30,
    )
    _raise_for_tiktok_error(resp)
    data = _check_response(resp.json())
    return data.get("list", [])


def create_custom_audience(
    advertiser_id: str,
    name: str,
    description: str = "",
) -> dict:
    """
    Create a new CUSTOMER_FILE custom audience for use as an exclusion list.
    Returns the audience object including the audience_id.
    """
    access_token, _ = _get_context()

    _require_approval("tiktok_create_audience", {"advertiser_id": advertiser_id, "name": name})

    url = f"{TIKTOK_API_BASE}/dmp/custom_audience/reach/create/"
    resp = httpx.post(
        url,
        headers=_headers(access_token),
        json={
            "advertiser_id":   advertiser_id,
            "name":            name,
            "audience_type":   "CUSTOMER_FILE_CONTACT_INFO",
            "description":     description,
            "calculate_type":  "SHA256",
            "rule":            {
                "inclusions": {
                    "operator": "OR",
                    "rules": []
                }
            },
        },
        timeout=30,
    )
    _raise_for_tiktok_error(resp)
    data = _check_response(resp.json())
    audience_id = data.get("audience_id")
    log.info("tiktok.audience_created", advertiser_id=advertiser_id, audience_id=audience_id, name=name)
    return {"advertiser_id": advertiser_id, "audience_id": audience_id, "name": name}


def hash_emails_for_tiktok(emails: list[str]) -> list[str]:
    """
    Normalize and SHA-256 hash a list of email addresses for TikTok Customer File upload.
    Normalization: lowercase, strip whitespace (TikTok requirement).
    Returns the list of hex-encoded SHA-256 digests.
    """
    import hashlib
    return [
        hashlib.sha256(email.lower().strip().encode()).hexdigest()
        for email in emails
        if email.strip()
    ]


def add_hashed_emails_to_audience(
    advertiser_id: str,
    audience_id: str,
    hashed_emails: list[str],
) -> dict:
    """
    Upload SHA-256 hashed emails to an existing TikTok custom audience.

    TikTok accepts pre-hashed email identifiers via the file upload endpoint.
    Builds an in-memory newline-delimited file and sends it as multipart/form-data.
    calculate_type must match the hashing method used (SHA256 here).

    Allow 24–48 hours for audience population and match rate reporting.
    """
    access_token, _ = _get_context()

    _require_approval(
        "tiktok_audience_upload",
        {"advertiser_id": advertiser_id, "audience_id": audience_id, "email_count": len(hashed_emails)},
    )

    # Build in-memory file: one hashed identifier per line
    file_content = "\n".join(hashed_emails).encode()

    url = f"{TIKTOK_API_BASE}/dmp/custom_audience/file/upload/"

    # Multipart upload — exclude Content-Type from headers (let httpx set the boundary)
    auth_headers = {"Access-Token": access_token}
    resp = httpx.post(
        url,
        headers=auth_headers,
        data={
            "advertiser_id":  advertiser_id,
            "calculate_type": "SHA256",
            "context_info":   json.dumps({"audience_ids": [audience_id]}),
        },
        files={"file": ("audience.txt", file_content, "text/plain")},
        timeout=120,
    )
    _raise_for_tiktok_error(resp)
    data = _check_response(resp.json())
    batch_id = data.get("batch_transfer_id")

    log.info(
        "tiktok.audience_emails_uploaded",
        advertiser_id=advertiser_id,
        audience_id=audience_id,
        email_count=len(hashed_emails),
        batch_id=batch_id,
    )
    return {
        "advertiser_id": advertiser_id,
        "audience_id":   audience_id,
        "emails_uploaded": len(hashed_emails),
        "batch_transfer_id": batch_id,
        "note": "Allow 24–48h for audience population and match rate reporting in Ads Manager.",
    }


def push_domain_suppression(
    advertiser_id: str,
    audience_id: str,
    domains: list[str],
    crm_emails_by_domain: dict[str, list[str]] | None = None,
) -> dict:
    """
    Add company domains to a TikTok custom audience for ad suppression.

    TikTok does not support direct domain-based audience matching.
    Strategy follows the same pattern as Google Ads (Task 21):

    If crm_emails_by_domain is provided:
        Collects all emails for the given domains, hashes them (SHA-256),
        and uploads via add_hashed_emails_to_audience(). Higher match rate.

    If crm_emails_by_domain is None (no CRM data yet):
        Returns a queued_manual status with instructions.
        Wire crm_emails_by_domain in Task 22/24 when the CRM lookup tool is built.

    Args:
        advertiser_id: TikTok advertiser ID.
        audience_id: Existing custom audience ID (create with create_custom_audience).
        domains: List of company domains, e.g. ['acme.com', 'bigcorp.com'].
        crm_emails_by_domain: Optional dict mapping domain → list of raw email strings.
            Emails are hashed inside this function — do NOT pre-hash.

    Returns status dict with either upload result or manual-action instructions.
    """
    # ── Resolve crm_emails_by_domain ──────────────────────────────────────────
    # If not passed explicitly, auto-fetch from crm_leads_staging via crm_client.
    # This resolves the Task 22 TODO: CRM lookup is now wired automatically.
    if crm_emails_by_domain is None:
        try:
            from tools.crm_client import get_crm_emails_by_domain
            crm_emails_by_domain = get_crm_emails_by_domain(domains=domains)
            log.info(
                "tiktok.suppression.crm_auto_fetched",
                advertiser_id=advertiser_id,
                domains=len(domains),
                domains_matched=len(crm_emails_by_domain),
            )
        except Exception as exc:
            log.warning(
                "tiktok.suppression.crm_fetch_failed",
                error=str(exc),
                note="Falling back to manual suppression workflow.",
            )
            crm_emails_by_domain = {}

    if crm_emails_by_domain:
        # Gather emails for the requested domains
        raw_emails: list[str] = []
        for domain in domains:
            raw_emails.extend(crm_emails_by_domain.get(domain, []))

        if not raw_emails:
            return {
                "status": "queued_manual",
                "platform": "tiktok",
                "advertiser_id": advertiser_id,
                "audience_id": audience_id,
                "domains_requested": len(domains),
                "note": (
                    "CRM data found but no emails matched the given domains. "
                    "Verify domain keys in crm_emails_by_domain match the domains list exactly. "
                    "Run crm_client.summarize_domain_coverage(domains) to diagnose coverage gaps."
                ),
            }

        hashed = hash_emails_for_tiktok(raw_emails)
        result = add_hashed_emails_to_audience(advertiser_id, audience_id, hashed)
        return {
            **result,
            "status": "uploaded",
            "domains": len(domains),
        }

    # No CRM data available — return manual fallback with clear instructions
    log.warning(
        "tiktok.suppression.no_crm_data",
        advertiser_id=advertiser_id,
        audience_id=audience_id,
        domain_count=len(domains),
    )
    return {
        "status": "queued_manual",
        "platform": "tiktok",
        "advertiser_id": advertiser_id,
        "audience_id": audience_id,
        "domains_requested": len(domains),
        "domain_list": domains,
        "note": (
            "TikTok does not support direct domain-based audience matching. "
            "No CRM email records were found for the requested domains in crm_leads_staging. "
            "To resolve: (1) ensure crm_leads_staging is populated for these domains, "
            "or (2) pass crm_emails_by_domain explicitly with employee emails from your CRM."
        ),
    }
