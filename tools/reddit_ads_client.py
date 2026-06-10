# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Reddit Ads Marketing API v3 client — Task 33.

Covers:
  - Dual-mode OAuth 2.0 authentication (headless env vars + local ~/reddit-ads.yaml)
  - Account ID structural validation (t2_ / a2_ prefix enforcement)
  - Reddit-compliance User-Agent header format
  - Client-side rate limiting (1 request/second token-bucket)
  - Campaign + ad-group daily performance extraction with explicit timezone injection
  - Geo-level performance extraction (country + DMA region → Meridian MMM feeds)
  - Campaign budget modification (daily + lifetime, guardrail-gated)
  - CRM-backed custom audience creation and hashed-email suppression upload

API version: v3
Base URL:    https://ads-api.reddit.com/api/v3
Docs:        https://ads-api.reddit.com/docs/v3

Authentication modes:
  Full Mode (Cloud Run / headless / GCP Secret Manager):
    Set REDDIT_ADS_CLIENT_ID, REDDIT_ADS_CLIENT_SECRET, REDDIT_ADS_REFRESH_TOKEN.
    Cloud Run injects these from Secret Manager at startup.

  Simple Mode (local practitioner):
    Leave env vars blank and run: python tools/setup_reddit_ads.py
    Writes ~/reddit-ads.yaml automatically; reads it back on each client init.

Financial fields:
  Reddit API returns monetary values in micro-USD (1 USD = 1,000,000 micro-USD).
  All monetary fields are converted to Python Decimal (USD, 6 decimal places max)
  before being passed to the data layer, matching the NUMERIC BigQuery schema.

Rate limit:
  Reddit enforces 1 request per second per OAuth token.
  This client implements a module-level token-bucket with a 1.0s refill period.
  All HTTP calls route through _request() which enforces the bucket before dispatch.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from base64 import b64encode
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from tools.http_retry import get_with_retry
import structlog

from config import settings

log = structlog.get_logger()

# ── API constants ──────────────────────────────────────────────────────────────

REDDIT_ADS_API_BASE  = "https://ads-api.reddit.com/api/v3"
REDDIT_TOKEN_URL     = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_VERSION   = "v3"

YAML_FILENAME = "reddit-ads.yaml"

# Reddit-compliance User-Agent format: "<app>:<version> (by u/<username>)"
# Pulled from settings so the username can be configured per deployment.
_USER_AGENT_TEMPLATE = "paid-media-agent:v1.0 (by u/{username})"

# ── Structural account ID validation ──────────────────────────────────────────

_VALID_ACCOUNT_PREFIXES = ("t2_", "a2_")


def _validate_account_id(account_id: str) -> None:
    """
    Enforce Reddit ad account ID structural constraints.

    Every Reddit ad account ID must begin with 't2_' (user account) or 'a2_'
    (ad account entity prefix). IDs that fail this check will be rejected before
    any API call is made to prevent silent failures on malformed inputs.

    Raises ValueError with a clear diagnostic if the prefix is missing or wrong.
    """
    if not account_id or not any(account_id.startswith(p) for p in _VALID_ACCOUNT_PREFIXES):
        raise ValueError(
            f"Invalid Reddit ad account ID: {account_id!r}. "
            f"Account IDs must begin with one of: {_VALID_ACCOUNT_PREFIXES}. "
            "Example valid IDs: 't2_abc123', 'a2_xyz789'. "
            "Find your account ID in Reddit Ads Manager → Account → Account ID."
        )


# ── Exceptions ─────────────────────────────────────────────────────────────────


class RedditAdsSetupError(Exception):
    """Raised when credentials are missing or the YAML config is invalid."""


class RedditAdsError(Exception):
    """Raised when the Reddit Ads API returns a non-success HTTP or app-level error."""


class RedditAdsBudgetGuardrailError(Exception):
    """Raised when a proposed budget change exceeds settings.max_budget_shift_pct."""


# ── Token-bucket rate limiter (1 req / second) ─────────────────────────────────

class _TokenBucket:
    """
    Minimal token-bucket rate limiter enforcing Reddit's 1 request/second limit.

    Tokens refill at `rate_per_second`. Each call to acquire() blocks until a
    token is available, then consumes one. Thread-safe via monotonic clock only
    (single-threaded agent loop; add a threading.Lock() if parallelised).
    """
    def __init__(self, rate_per_second: float = 1.0) -> None:
        self._rate    = rate_per_second
        self._tokens  = rate_per_second          # start full
        self._last_ts = time.monotonic()

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        now = time.monotonic()
        elapsed = now - self._last_ts
        self._last_ts = now
        self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
        if self._tokens < 1.0:
            sleep_for = (1.0 - self._tokens) / self._rate
            time.sleep(sleep_for)
            self._tokens = 0.0
        else:
            self._tokens -= 1.0


# Module-level bucket — shared across all calls within a process.
_BUCKET = _TokenBucket(rate_per_second=1.0)


# ── OAuth token cache ──────────────────────────────────────────────────────────

_token_cache: dict[str, Any] = {}   # {"token": str, "expires_at": float}


def _get_access_token(client_id: str, client_secret: str, refresh_token: str | None) -> str:
    """
    Obtain an OAuth 2.0 Bearer token from Reddit.

    Uses the Refresh Token grant when refresh_token is provided (recommended for
    ad account access). Falls back to Client Credentials grant when no refresh
    token is available (limited to public Reddit API access — not suitable for
    production ad account operations).

    Tokens are cached in-process until 60 seconds before expiry to avoid
    redundant round-trips on every API call.

    Returns the raw access token string.
    """
    now = time.monotonic()
    cached = _token_cache.get("token")
    expires_at = _token_cache.get("expires_at", 0.0)
    if cached and now < expires_at - 60:
        return cached  # type: ignore[return-value]

    credentials = b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "User-Agent":    _build_user_agent(),
        "Content-Type":  "application/x-www-form-urlencoded",
    }

    if refresh_token:
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    else:
        data = {"grant_type": "client_credentials"}

    resp = httpx.post(REDDIT_TOKEN_URL, headers=headers, data=data, timeout=20)
    if resp.status_code != 200:
        raise RedditAdsSetupError(
            f"Reddit OAuth token request failed ({resp.status_code}): {resp.text[:300]}. "
            "Check REDDIT_ADS_CLIENT_ID / REDDIT_ADS_CLIENT_SECRET / REDDIT_ADS_REFRESH_TOKEN."
        )

    body = resp.json()
    token = body.get("access_token", "")
    if not token:
        raise RedditAdsSetupError(
            f"Reddit OAuth response missing access_token: {body}. "
            "Ensure your app has the required scopes (ads:read, ads:write)."
        )

    expires_in = int(body.get("expires_in", 3600))
    _token_cache["token"]      = token
    _token_cache["expires_at"] = time.monotonic() + expires_in

    log.debug("reddit_ads.token_refreshed", expires_in=expires_in)
    return token


def _build_user_agent() -> str:
    username = getattr(settings, "reddit_ads_username", "") or "paid-media-agent"
    return _USER_AGENT_TEMPLATE.format(username=username)


# ── Auth — dual-mode credential resolution ────────────────────────────────────


def _get_context() -> tuple[str, list[str]]:
    """
    Resolve Reddit Ads credentials and return (access_token, account_ids).

    Full Mode (Cloud Run / headless):
        Reads REDDIT_ADS_CLIENT_ID, REDDIT_ADS_CLIENT_SECRET,
        REDDIT_ADS_REFRESH_TOKEN from environment / settings.
        REDDIT_ADS_ACCOUNT_IDS may be a comma-separated list (multi-tenant)
        or fall back to REDDIT_ADS_ACCOUNT_ID for single-account setups.

    Simple Mode (local practitioner):
        Reads ~/reddit-ads.yaml written by tools/setup_reddit_ads.py.
        Keys: client_id, client_secret, refresh_token, account_ids (list).

    Raises RedditAdsSetupError with actionable instructions if neither succeeds.
    """
    # ── Full Mode ──────────────────────────────────────────────────────────────
    client_id     = settings.reddit_ads_client_id.strip()
    client_secret = settings.reddit_ads_client_secret.strip()
    refresh_token = settings.reddit_ads_refresh_token.strip() or None
    ids_env       = os.getenv("REDDIT_ADS_ACCOUNT_IDS", "").strip()

    if client_id and client_secret:
        if ids_env:
            ids = [aid.strip() for aid in ids_env.split(",") if aid.strip()]
        else:
            single = settings.reddit_ads_account_id.strip()
            ids = [single] if single else []
        if ids:
            for aid in ids:
                _validate_account_id(aid)
            access_token = _get_access_token(client_id, client_secret, refresh_token)
            log.debug("reddit_ads.auth.full_mode", account_count=len(ids))
            return access_token, ids

    # ── Simple Mode ───────────────────────────────────────────────────────────
    yaml_path = Path.home() / YAML_FILENAME
    if yaml_path.exists():
        try:
            import yaml  # type: ignore[import]
        except ImportError:
            raise RedditAdsSetupError(
                "pyyaml is required to load ~/reddit-ads.yaml. "
                "Install with: pip install pyyaml"
            )
        cfg = yaml.safe_load(yaml_path.read_text())
        cid = cfg.get("client_id", "").strip()
        csc = cfg.get("client_secret", "").strip()
        rtk = cfg.get("refresh_token", "").strip() or None
        raw_ids = cfg.get("account_ids", [])
        ids = [str(i).strip() for i in raw_ids if str(i).strip()] if isinstance(raw_ids, list) else [str(raw_ids).strip()]
        if cid and csc and ids:
            for aid in ids:
                _validate_account_id(aid)
            access_token = _get_access_token(cid, csc, rtk)
            log.debug("reddit_ads.auth.simple_mode", yaml_path=str(yaml_path), account_count=len(ids))
            return access_token, ids

    raise RedditAdsSetupError(
        "No Reddit Ads credentials found.\n\n"
        "Full Mode (Cloud Run / headless):\n"
        "  Set REDDIT_ADS_CLIENT_ID, REDDIT_ADS_CLIENT_SECRET, REDDIT_ADS_REFRESH_TOKEN,\n"
        "  and REDDIT_ADS_ACCOUNT_IDS (comma-separated) environment variables.\n\n"
        "Simple Mode (local development):\n"
        "  Run the interactive setup: python tools/setup_reddit_ads.py\n"
        "  This writes ~/reddit-ads.yaml with your credentials.\n\n"
        "Getting credentials:\n"
        "  App ID / Secret: https://ads.reddit.com → Developer Access\n"
        "  Account IDs: Reddit Ads Manager → Account → Account ID (t2_xxx format)\n"
        "  Refresh Token: generated by setup_reddit_ads.py or the Reddit Ads developer portal"
    )


# ── Request primitives ─────────────────────────────────────────────────────────


def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json",
        "User-Agent":    _build_user_agent(),
    }


def _request(
    method: str,
    path: str,
    access_token: str,
    params: dict | None = None,
    json_body: dict | None = None,
    timeout: int = 60,
) -> Any:
    """
    Single HTTP dispatch with rate-limit enforcement and error unwrapping.

    All Reddit Ads API calls route through this function to ensure the
    token bucket is checked before every request.

    Returns the parsed JSON body. Raises RedditAdsError on API errors.
    """
    _BUCKET.acquire()   # enforce 1 req/sec before every call

    url = f"{REDDIT_ADS_API_BASE}{path}"
    resp = httpx.request(
        method,
        url,
        headers=_headers(access_token),
        params=params,
        json=json_body,
        timeout=timeout,
    )

    if resp.status_code == 429:
        # 429 should not happen with the bucket in place, but guard anyway.
        retry_after = int(resp.headers.get("Retry-After", "2"))
        log.warning("reddit_ads.rate_limited", retry_after=retry_after)
        time.sleep(retry_after)
        _BUCKET.acquire()
        resp = httpx.request(method, url, headers=_headers(access_token),
                             params=params, json=json_body, timeout=timeout)

    if resp.status_code >= 400:
        try:
            err_body = resp.json()
            msg = err_body.get("message") or err_body.get("error") or resp.text[:300]
        except Exception:
            msg = resp.text[:300]
        raise RedditAdsError(
            f"Reddit Ads API {method} {path} → {resp.status_code}: {msg}"
        )

    return resp.json()


def _require_approval(action: str, payload: dict) -> None:
    from tools.gmp_client import ApprovalRequiredError
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending Reddit Ads action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2, default=str)}"
        )


# ── Financial field conversions ────────────────────────────────────────────────


def _micro_usd_to_decimal(micro_usd: Any) -> Decimal:
    """
    Convert Reddit's micro-USD value (1 USD = 1,000,000 micro-USD) to Decimal USD.

    Reddit Ads API v3 returns all monetary values in micro-USD.
    Example: 1500000 micro-USD → Decimal("1.500000") USD.

    Using Decimal(str()) avoids float precision drift.
    Matches the NUMERIC BigQuery schema from paid-media-schema.
    """
    if micro_usd is None or micro_usd == "":
        return Decimal("0")
    return Decimal(str(int(micro_usd))) / Decimal("1000000")


def _decimal_usd_to_micro(usd: float) -> int:
    """Convert caller-facing USD float to Reddit micro-USD integer for write operations."""
    return int(Decimal(str(usd)) * Decimal("1000000"))


def _to_numeric(value: Any) -> Decimal:
    """
    Convert a raw financial value that may already be in USD (not micro-USD).
    Used for derived fields like cpc, cpm that Reddit returns as floats.
    """
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _to_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(float(str(value)))


# ── Reporting ──────────────────────────────────────────────────────────────────


def _get_reports(
    account_id: str,
    access_token: str,
    breakdowns: list[str],
    fields: list[str],
    starts_at: str,
    ends_at: str,
    time_zone_id: str = "America/New_York",
    campaign_ids: list[str] | None = None,
) -> list[dict]:
    """
    Core wrapper for GET /api/v3/ad_accounts/{account_id}/reports.

    Time-zone synchronisation:
        starts_at and ends_at are ISO 8601 date strings (YYYY-MM-DD).
        time_zone_id is always injected explicitly to override Reddit's default
        UTC reporting boundary and align transaction dates with our BigQuery
        midnight partitioning.

    Breakdowns: DATE | CAMPAIGN | AD_GROUP | AD | COUNTRY | DMA_REGION
    Multiple breakdowns may be passed; they are sent as repeated query params.

    Pagination is handled internally. Returns the flat list of data rows.
    """
    _validate_account_id(account_id)

    path   = f"/ad_accounts/{account_id}/reports"
    params: list[tuple[str, str]] = [
        ("starts_at",    starts_at),
        ("ends_at",      ends_at),
        ("time_zone_id", time_zone_id),
    ]
    for bd in breakdowns:
        params.append(("breakdown", bd))
    for f in fields:
        params.append(("fields", f))
    if campaign_ids:
        for cid in campaign_ids:
            params.append(("campaign_ids", cid))

    # httpx accepts list-of-tuples for repeated query params
    _BUCKET.acquire()
    url = f"{REDDIT_ADS_API_BASE}{path}"
    resp = get_with_retry(
        url,
        headers=_headers(access_token),
        params=params,
        timeout=60,
    )

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "2"))
        log.warning("reddit_ads.reports.rate_limited", account=account_id, retry_after=retry_after)
        time.sleep(retry_after)
        _BUCKET.acquire()
        resp = get_with_retry(url, headers=_headers(access_token), params=params, timeout=60)

    if resp.status_code >= 400:
        try:
            err = resp.json()
            msg = err.get("message") or err.get("error") or resp.text[:300]
        except Exception:
            msg = resp.text[:300]
        raise RedditAdsError(
            f"Reddit Ads reports {account_id} → {resp.status_code}: {msg}"
        )

    body = resp.json()
    # Reddit v3 wraps results in {"data": {"results": [...]}} or just a list
    if isinstance(body, list):
        return body
    data = body.get("data", body)
    if isinstance(data, list):
        return data
    return data.get("results", data.get("items", []))


def get_campaign_performance(
    account_id: str,
    date_from: str,
    date_to: str,
    time_zone_id: str = "America/New_York",
    campaign_ids: list[str] | None = None,
) -> list[dict]:
    """
    Pull daily campaign + ad-group performance from the Reddit Ads reports endpoint.

    Breakdown: DATE × CAMPAIGN × AD_GROUP

    Args:
        account_id:   Reddit ad account ID (t2_xxx / a2_xxx — validated).
        date_from:    "YYYY-MM-DD" start of reporting window (inclusive).
        date_to:      "YYYY-MM-DD" end of reporting window (inclusive).
        time_zone_id: IANA timezone for date boundary alignment (default: America/New_York).
        campaign_ids: Optional filter to specific campaigns.

    Returns list of dicts with schema-aligned field types mapping to
    reddit_daily_spend in paid-media-schema/bigquery/13_reddit_ads.sql.

    Financial fields are Decimal (USD); count fields are int.
    """
    access_token, _ = _get_context()

    rows = _get_reports(
        account_id=account_id,
        access_token=access_token,
        breakdowns=["DATE", "CAMPAIGN", "AD_GROUP"],
        fields=[
            "spend",
            "impressions",
            "clicks",
            "ctr",
            "ecpm",
            "cpc",
            "conversions",
            "view_conversions",
            "cost_per_conversion",
            "video_plays",
            "video_views_25",
            "video_views_50",
            "video_views_75",
            "video_views_100",
        ],
        starts_at=date_from,
        ends_at=date_to,
        time_zone_id=time_zone_id,
        campaign_ids=campaign_ids,
    )

    result: list[dict] = []
    for row in rows:
        # Reddit v3 report rows nest dimensions and metrics differently;
        # flatten both common formats: flat dict or {"dimensions": {}, "metrics": {}}
        dims = row.get("dimensions", row)
        m    = row.get("metrics",    row)

        spend_raw = m.get("spend") or row.get("spend", 0)
        # Reddit reports spend in micro-USD; derived unit fields (cpc, cpm) in USD floats
        spend = _micro_usd_to_decimal(spend_raw) if isinstance(spend_raw, int) and spend_raw > 1000 else _to_numeric(spend_raw)

        impr  = _to_int(m.get("impressions") or row.get("impressions"))
        clicks_cnt = _to_int(m.get("clicks") or row.get("clicks"))
        convs = _to_int(m.get("conversions") or row.get("conversions"))

        result.append({
            "account_id":           account_id,
            "campaign_id":          dims.get("campaign_id") or row.get("campaign_id"),
            "campaign_name":        dims.get("campaign_name") or row.get("campaign_name"),
            "campaign_objective":   dims.get("objective") or row.get("objective"),
            "ad_group_id":          dims.get("ad_group_id") or row.get("ad_group_id"),
            "ad_group_name":        dims.get("ad_group_name") or row.get("ad_group_name"),
            "date":                 dims.get("date") or row.get("date", date_from),
            "time_zone_id":         time_zone_id,
            # Financial — Decimal (NUMERIC in BigQuery)
            "spend":                spend,
            "cpc":                  _to_numeric(m.get("cpc") or row.get("cpc")),
            "cpm":                  _to_numeric(m.get("ecpm") or row.get("ecpm")),
            "ecpm":                 _to_numeric(m.get("ecpm") or row.get("ecpm")),
            "cost_per_conversion":  _to_numeric(m.get("cost_per_conversion") or row.get("cost_per_conversion")),
            # Counts — INT64
            "impressions":          impr,
            "clicks":               clicks_cnt,
            "conversions":          convs,
            "view_conversions":     _to_int(m.get("view_conversions") or row.get("view_conversions")),
            "ctr":                  float(m.get("ctr") or row.get("ctr") or 0),
            "video_plays":          _to_int(m.get("video_plays") or row.get("video_plays")),
            "video_views_25pct":    _to_int(m.get("video_views_25") or row.get("video_views_25")),
            "video_views_50pct":    _to_int(m.get("video_views_50") or row.get("video_views_50")),
            "video_views_75pct":    _to_int(m.get("video_views_75") or row.get("video_views_75")),
            "video_views_100pct":   _to_int(m.get("video_views_100") or row.get("video_views_100")),
            "video_completion_rate": (
                _to_int(m.get("video_views_100") or 0) / max(_to_int(m.get("video_plays") or 0), 1)
            ),
        })

    log.info(
        "reddit_ads.campaign_performance.fetched",
        account_id=account_id,
        rows=len(result),
        date_from=date_from,
        date_to=date_to,
    )
    return result


def get_spatial_performance(
    account_id: str,
    date_from: str,
    date_to: str,
    time_zone_id: str = "America/New_York",
    include_dma: bool = True,
    campaign_ids: list[str] | None = None,
) -> list[dict]:
    """
    Pull geo-segmented performance data at country and DMA region grain.

    Issues two separate report calls:
      1. Breakdown=CAMPAIGN × COUNTRY (all geographies)
      2. Breakdown=CAMPAIGN × DMA_REGION (US only, if include_dma=True)

    Merges results into a single list mapped to reddit_spatial_performance schema.
    Date range is stored as date_range_start / date_range_end (aggregate window).

    Args:
        account_id:   Reddit ad account ID (validated).
        date_from:    Aggregate window start "YYYY-MM-DD".
        date_to:      Aggregate window end "YYYY-MM-DD".
        time_zone_id: IANA timezone for date boundary alignment.
        include_dma:  Whether to issue the DMA_REGION breakdown call (US accounts).
        campaign_ids: Optional campaign filter.

    Returns list of dicts mapping to reddit_spatial_performance.
    """
    access_token, _ = _get_context()

    _geo_fields = ["spend", "impressions", "clicks", "ctr", "ecpm"]
    results: list[dict] = []

    # ── Country breakdown ──────────────────────────────────────────────────────
    country_rows = _get_reports(
        account_id=account_id,
        access_token=access_token,
        breakdowns=["CAMPAIGN", "COUNTRY"],
        fields=_geo_fields,
        starts_at=date_from,
        ends_at=date_to,
        time_zone_id=time_zone_id,
        campaign_ids=campaign_ids,
    )
    for row in country_rows:
        dims = row.get("dimensions", row)
        m    = row.get("metrics",    row)
        spend_raw = m.get("spend") or row.get("spend", 0)
        results.append({
            "account_id":       account_id,
            "campaign_id":      dims.get("campaign_id") or row.get("campaign_id"),
            "campaign_name":    dims.get("campaign_name") or row.get("campaign_name"),
            "date_range_start": date_from,
            "date_range_end":   date_to,
            "time_zone_id":     time_zone_id,
            "country_code":     (dims.get("country") or row.get("country") or "").upper()[:2] or None,
            "dma_region":       None,
            "spend":            _micro_usd_to_decimal(spend_raw) if isinstance(spend_raw, int) and spend_raw > 1000 else _to_numeric(spend_raw),
            "impressions":      _to_int(m.get("impressions") or row.get("impressions")),
            "clicks":           _to_int(m.get("clicks") or row.get("clicks")),
            "conversions":      _to_int(m.get("conversions") or row.get("conversions")),
            "ctr":              float(m.get("ctr") or row.get("ctr") or 0),
            "cpm":              _to_numeric(m.get("ecpm") or row.get("ecpm")),
        })

    # ── DMA breakdown (US only) ────────────────────────────────────────────────
    if include_dma:
        try:
            dma_rows = _get_reports(
                account_id=account_id,
                access_token=access_token,
                breakdowns=["CAMPAIGN", "DMA_REGION"],
                fields=_geo_fields,
                starts_at=date_from,
                ends_at=date_to,
                time_zone_id=time_zone_id,
                campaign_ids=campaign_ids,
            )
            for row in dma_rows:
                dims = row.get("dimensions", row)
                m    = row.get("metrics",    row)
                spend_raw = m.get("spend") or row.get("spend", 0)
                results.append({
                    "account_id":       account_id,
                    "campaign_id":      dims.get("campaign_id") or row.get("campaign_id"),
                    "campaign_name":    dims.get("campaign_name") or row.get("campaign_name"),
                    "date_range_start": date_from,
                    "date_range_end":   date_to,
                    "time_zone_id":     time_zone_id,
                    "country_code":     "US",
                    "dma_region":       dims.get("dma_region") or row.get("dma_region"),
                    "spend":            _micro_usd_to_decimal(spend_raw) if isinstance(spend_raw, int) and spend_raw > 1000 else _to_numeric(spend_raw),
                    "impressions":      _to_int(m.get("impressions") or row.get("impressions")),
                    "clicks":           _to_int(m.get("clicks") or row.get("clicks")),
                    "conversions":      _to_int(m.get("conversions") or row.get("conversions")),
                    "ctr":              float(m.get("ctr") or row.get("ctr") or 0),
                    "cpm":              _to_numeric(m.get("ecpm") or row.get("ecpm")),
                })
        except RedditAdsError as exc:
            log.warning("reddit_ads.dma_breakdown_failed", account_id=account_id, error=str(exc))

    log.info(
        "reddit_ads.spatial_performance.fetched",
        account_id=account_id,
        rows=len(results),
        date_from=date_from,
        date_to=date_to,
        dma_included=include_dma,
    )
    return results


# ── Campaign management ────────────────────────────────────────────────────────


def get_campaign(account_id: str, campaign_id: str) -> dict:
    """Fetch a single campaign's current metadata including budget."""
    _validate_account_id(account_id)
    access_token, _ = _get_context()
    body = _request("GET", f"/campaigns/{campaign_id}", access_token)
    data = body.get("data", body)
    if isinstance(data, list):
        data = data[0] if data else {}
    return data


def modify_reddit_campaign_budget(
    account_id: str,
    campaign_id: str,
    new_budget_usd: float,
    budget_type: str = "daily",
) -> dict:
    """
    Modify a Reddit Ads campaign budget via PATCH /api/v3/campaigns/{campaign_id}.

    Budget is converted from caller-facing USD to Reddit's micro-USD format
    before the API call. Enforces the max_budget_shift_pct guardrail against
    the current budget. Gated by OPERATOR_REQUIRE_APPROVAL.

    Args:
        account_id:     Reddit ad account ID (validated before API call).
        campaign_id:    Reddit campaign ID to update.
        new_budget_usd: New budget in USD (positive float).
        budget_type:    "daily" (default) or "lifetime" — maps to
                        daily_budget / total_budget in the PATCH payload.

    Returns a dict with before/after budget values and update status.
    Raises RedditAdsBudgetGuardrailError if change exceeds max_budget_shift_pct.
    """
    _validate_account_id(account_id)
    access_token, _ = _get_context()

    current = get_campaign(account_id, campaign_id)
    if budget_type == "lifetime":
        current_micro = int(current.get("total_budget", 0) or 0)
        budget_key = "total_budget"
    else:
        current_micro = int(current.get("daily_budget", 0) or 0)
        budget_key = "daily_budget"

    current_usd = float(_micro_usd_to_decimal(current_micro))

    if current_usd > 0:
        change_pct = abs(new_budget_usd - current_usd) / current_usd * 100
        if change_pct > settings.max_budget_shift_pct:
            raise RedditAdsBudgetGuardrailError(
                f"Budget change of {change_pct:.1f}% exceeds the "
                f"{settings.max_budget_shift_pct}% guardrail. "
                f"Current: ${current_usd:.2f}, Proposed: ${new_budget_usd:.2f}. "
                "Adjust new_budget_usd or raise MAX_BUDGET_SHIFT_PCT."
            )

    _require_approval(
        "reddit_ads_campaign_budget_update",
        {
            "account_id":         account_id,
            "campaign_id":        campaign_id,
            "budget_type":        budget_type,
            "current_budget_usd": current_usd,
            "new_budget_usd":     new_budget_usd,
        },
    )

    new_micro = _decimal_usd_to_micro(new_budget_usd)
    body = _request(
        "PATCH",
        f"/campaigns/{campaign_id}",
        access_token,
        json_body={budget_key: new_micro},
    )

    log.info(
        "reddit_ads.budget_modified",
        account_id=account_id,
        campaign_id=campaign_id,
        budget_type=budget_type,
        old_usd=current_usd,
        new_usd=new_budget_usd,
    )
    return {
        "account_id":           account_id,
        "campaign_id":          campaign_id,
        "budget_type":          budget_type,
        "previous_budget_usd":  current_usd,
        "new_budget_usd":       new_budget_usd,
        "api_response":         body,
        "status":               "updated",
    }


def reallocate_campaign_budget(
    account_id: str,
    source_campaign_id: str,
    target_campaign_id: str,
    amount_usd: float,
) -> dict:
    """
    Move budget from one Reddit Ads campaign to another within the same account.

    Fetches current budgets, checks the max_budget_shift_pct guardrail against
    the source campaign, then writes both PATCH calls.
    Minimum post-reduction budget: $1.00.
    """
    _validate_account_id(account_id)
    access_token, _ = _get_context()  # noqa: F841

    source = get_campaign(account_id, source_campaign_id)
    target = get_campaign(account_id, target_campaign_id)

    src_usd = float(_micro_usd_to_decimal(int(source.get("daily_budget", 0) or 0)))
    tgt_usd = float(_micro_usd_to_decimal(int(target.get("daily_budget", 0) or 0)))

    if src_usd > 0:
        pct = amount_usd / src_usd * 100
        if pct > settings.max_budget_shift_pct:
            raise RedditAdsBudgetGuardrailError(
                f"Reallocation of ${amount_usd:.2f} is {pct:.1f}% of source campaign "
                f"budget (${src_usd:.2f}), exceeding the {settings.max_budget_shift_pct}% "
                "guardrail. Reduce amount_usd or raise MAX_BUDGET_SHIFT_PCT."
            )

    r_source = modify_reddit_campaign_budget(account_id, source_campaign_id, max(1.0, src_usd - amount_usd))
    r_target = modify_reddit_campaign_budget(account_id, target_campaign_id, tgt_usd + amount_usd)

    log.info(
        "reddit_ads.budget_reallocated",
        account_id=account_id,
        source=source_campaign_id,
        target=target_campaign_id,
        amount_usd=amount_usd,
    )
    return {
        "account_id":      account_id,
        "source_campaign": r_source,
        "target_campaign": r_target,
        "amount_moved_usd": amount_usd,
    }


# ── Audience management ────────────────────────────────────────────────────────


def create_audience(
    account_id: str,
    name: str,
    audience_type: str = "CUSTOMER_LIST",
    description: str = "",
) -> dict:
    """
    Create a new custom audience via POST /api/v3/audiences.

    audience_type options:
        "CUSTOMER_LIST"     — hashed email upload (for CRM-based suppression)
        "REMARKETING"       — pixel-based retargeting
        "LOOKALIKE"         — lookalike expansion from a seed audience

    Gated by OPERATOR_REQUIRE_APPROVAL.
    Returns the created audience object including audience_id.
    """
    _validate_account_id(account_id)
    access_token, _ = _get_context()

    _require_approval(
        "reddit_ads_create_audience",
        {"account_id": account_id, "name": name, "audience_type": audience_type},
    )

    body = _request(
        "POST",
        "/audiences",
        access_token,
        json_body={
            "account_id":    account_id,
            "name":          name,
            "audience_type": audience_type,
            "description":   description,
        },
    )
    data = body.get("data", body)
    audience_id = data.get("audience_id") or data.get("id")
    log.info(
        "reddit_ads.audience_created",
        account_id=account_id,
        audience_id=audience_id,
        name=name,
    )
    return {"account_id": account_id, "audience_id": audience_id, "name": name, "type": audience_type}


def hash_emails_for_reddit(emails: list[str]) -> list[str]:
    """
    Normalize and SHA-256 hash email addresses for Reddit Customer List upload.

    Normalisation (Reddit requirement): lowercase, strip whitespace.
    Returns a list of hex-encoded SHA-256 digests.
    """
    import hashlib
    return [
        hashlib.sha256(email.lower().strip().encode()).hexdigest()
        for email in emails
        if email.strip()
    ]


def upload_hashed_emails_to_audience(
    account_id: str,
    audience_id: str,
    hashed_emails: list[str],
) -> dict:
    """
    Upload SHA-256 hashed emails to an existing Reddit Customer List audience.

    Reddit Ads accepts pre-hashed email identifiers via the audience users endpoint.
    All hashing must be done client-side before calling this function
    (use hash_emails_for_reddit() for normalised SHA-256 encoding).

    Allow 24–48 hours for audience population and match rate reporting.
    Gated by OPERATOR_REQUIRE_APPROVAL.
    """
    _validate_account_id(account_id)
    access_token, _ = _get_context()

    _require_approval(
        "reddit_ads_audience_upload",
        {"account_id": account_id, "audience_id": audience_id, "email_count": len(hashed_emails)},
    )

    body = _request(
        "POST",
        f"/audiences/{audience_id}/users",
        access_token,
        json_body={
            "account_id":  account_id,
            "users":       [{"email": h} for h in hashed_emails],
            "hash_type":   "SHA256",
        },
    )

    log.info(
        "reddit_ads.audience_emails_uploaded",
        account_id=account_id,
        audience_id=audience_id,
        email_count=len(hashed_emails),
    )
    return {
        "account_id":      account_id,
        "audience_id":     audience_id,
        "emails_uploaded": len(hashed_emails),
        "api_response":    body,
        "note": "Allow 24–48h for audience population and match rate reporting in Ads Manager.",
    }


def push_domain_suppression(
    account_id: str,
    audience_id: str,
    domains: list[str],
    crm_emails_by_domain: dict[str, list[str]] | None = None,
) -> dict:
    """
    Add company domains to a Reddit custom audience for ad suppression.

    Reddit Ads does not support direct domain-based audience targeting.
    Strategy (mirrors TikTok Task 20 / Google Ads Task 21):

    If crm_emails_by_domain is provided (or auto-fetched via CRM hook):
        Collects all emails for the given domains, hashes them (SHA-256),
        and uploads via upload_hashed_emails_to_audience(). Higher match rate.

    If no CRM data is available:
        Returns a queued_manual status with step-by-step instructions for
        manual CSV upload in Reddit Ads Manager.

    Args:
        account_id:           Reddit ad account ID (t2_xxx / a2_xxx).
        audience_id:          Existing custom audience ID.
        domains:              List of company domains, e.g. ['acme.com'].
        crm_emails_by_domain: Optional dict mapping domain → raw email list.
            Emails are hashed inside this function — do NOT pre-hash.

    Returns status dict with either upload result or manual-action instructions.
    """
    _validate_account_id(account_id)

    # ── Auto-fetch CRM if not provided ────────────────────────────────────────
    if crm_emails_by_domain is None:
        try:
            from tools.crm_client import get_crm_emails_by_domain
            crm_emails_by_domain = get_crm_emails_by_domain(domains=domains)
            log.info(
                "reddit_ads.suppression.crm_auto_fetched",
                account_id=account_id,
                domains=len(domains),
                domains_matched=len(crm_emails_by_domain),
            )
        except Exception as exc:
            log.warning(
                "reddit_ads.suppression.crm_fetch_failed",
                error=str(exc),
                note="Falling back to manual suppression workflow.",
            )
            crm_emails_by_domain = {}

    if crm_emails_by_domain:
        raw_emails: list[str] = []
        for domain in domains:
            raw_emails.extend(crm_emails_by_domain.get(domain, []))

        if raw_emails:
            hashed = hash_emails_for_reddit(raw_emails)
            result = upload_hashed_emails_to_audience(account_id, audience_id, hashed)
            return {
                **result,
                "status":            "uploaded",
                "platform":          "reddit_ads",
                "domains_processed": len(domains),
                "emails_matched":    len(raw_emails),
            }

    # ── No email data → manual fallback ───────────────────────────────────────
    return {
        "status":     "queued_manual",
        "platform":   "reddit_ads",
        "account_id": account_id,
        "audience_id": audience_id,
        "domains_requested": len(domains),
        "note": (
            f"No CRM email data found for {len(domains)} domain(s). "
            "To suppress manually: Reddit Ads Manager → Audiences → "
            f"select audience '{audience_id}' → Upload CSV → "
            "paste hashed emails (SHA-256 of lowercase email addresses). "
            "Run tools/crm_client.summarize_domain_coverage(domains) to check coverage."
        ),
    }


# ── Bulk extraction helper ─────────────────────────────────────────────────────


def run_daily_extraction(
    date_from: str,
    date_to: str,
    time_zone_id: str = "America/New_York",
    account_ids: list[str] | None = None,
    include_spatial: bool = True,
) -> dict:
    """
    Run a full daily extraction across all configured Reddit Ads accounts.

    Fetches campaign-level daily spend + spatial (country/DMA) performance for
    each account in the configured account list (or the provided override).
    Writes results to reddit_daily_spend and reddit_spatial_performance in BigQuery.
    Writes a run record to reddit_ads_runs.

    Args:
        date_from:      Reporting window start "YYYY-MM-DD".
        date_to:        Reporting window end "YYYY-MM-DD".
        time_zone_id:   IANA timezone for date alignment (default: America/New_York).
        account_ids:    Override the configured account list. None = use _get_context().
        include_spatial: Whether to also fetch geo breakdown (default True).

    Returns a summary dict with run_id, accounts processed, rows written, status.
    """
    from tools import bigquery_client as bq

    run_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc)

    if account_ids is None:
        _, account_ids = _get_context()

    errors: list[str] = []
    spend_rows:   list[dict] = []
    spatial_rows: list[dict] = []
    all_campaign_ids: set[str] = set()

    for account_id in account_ids:
        try:
            perf = get_campaign_performance(
                account_id=account_id,
                date_from=date_from,
                date_to=date_to,
                time_zone_id=time_zone_id,
            )
            for row in perf:
                if row.get("campaign_id"):
                    all_campaign_ids.add(row["campaign_id"])
                spend_rows.append({
                    "row_id":    str(uuid.uuid4()),
                    "run_id":    run_id,
                    **{k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()},
                    "capture_timestamp": now.isoformat(),
                })
        except Exception as exc:
            errors.append(f"{account_id} spend: {exc}")
            log.warning("reddit_ads.extraction.spend_failed", account=account_id, error=str(exc))

        if include_spatial:
            try:
                geo = get_spatial_performance(
                    account_id=account_id,
                    date_from=date_from,
                    date_to=date_to,
                    time_zone_id=time_zone_id,
                )
                for row in geo:
                    spatial_rows.append({
                        "row_id":    str(uuid.uuid4()),
                        "run_id":    run_id,
                        **{k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()},
                        "capture_timestamp": now.isoformat(),
                    })
            except Exception as exc:
                errors.append(f"{account_id} spatial: {exc}")
                log.warning("reddit_ads.extraction.spatial_failed", account=account_id, error=str(exc))

    # ── Write to BigQuery ───────────────────────────────────────────────────────
    spend_written   = 0
    spatial_written = 0
    if spend_rows:
        bq.insert_rows("reddit_daily_spend", spend_rows)
        spend_written = len(spend_rows)
    if spatial_rows:
        bq.insert_rows("reddit_spatial_performance", spatial_rows)
        spatial_written = len(spatial_rows)

    status = (
        "failed"    if errors and spend_written == 0 and spatial_written == 0 else
        "partial"   if errors else
        "completed"
    )

    run_row = {
        "run_id":               run_id,
        "account_ids_processed": json.dumps(account_ids),
        "start_date":           date_from,
        "end_date":             date_to,
        "time_zone_id":         time_zone_id,
        "campaigns_fetched":    len(all_campaign_ids),
        "spend_rows_written":   spend_written,
        "spatial_rows_written": spatial_written,
        "status":               status,
        "error_message":        "; ".join(errors) if errors else None,
        "created_by":           "operator_agent",
        "created_at":           now.isoformat(),
    }
    bq.insert_rows("reddit_ads_runs", [run_row])

    log.info(
        "reddit_ads.extraction_complete",
        run_id=run_id,
        accounts=len(account_ids),
        spend_rows=spend_written,
        spatial_rows=spatial_written,
        status=status,
    )
    return {
        "run_id":           run_id,
        "status":           status,
        "accounts":         account_ids,
        "spend_rows":       spend_written,
        "spatial_rows":     spatial_written,
        "campaigns_seen":   len(all_campaign_ids),
        "errors":           errors,
    }
