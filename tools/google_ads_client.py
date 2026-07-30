# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Google Ads API client — Task 21.

Covers:
  - Dual-mode authentication (headless Cloud Run + local CLI OAuth)
  - GAQL campaign / ad-group / keyword performance queries
  - Performance Max (shopping_performance_view) with June 15, 2026 expanded-field handling
  - Geo-segmented spend (state + DMA) for Google Meridian MMM input
  - Customer Match audience management (B2B domain suppression via hashed emails)
  - Campaign budget management with max_budget_shift_pct guardrail enforcement

API reference: https://developers.google.com/google-ads/api/docs/start
Python client:  https://github.com/googleads/google-ads-python

Authentication:
  Full Mode  — set GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID,
               GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN in environment.
               Cloud Run pulls these from GCP Secret Manager at startup.
  Simple Mode — run `python tools/setup_google_ads.py` to generate ~/google-ads.yaml
               via interactive browser OAuth (local practitioners only).

All write operations gate on settings.operator_require_approval.
"""
from __future__ import annotations

import hashlib
import json
import os
from decimal import Decimal
from typing import Any

import structlog

from config import settings

log = structlog.get_logger()

# ── Phase 1: Version centralization ───────────────────────────────────────────
#
# Single source of truth for the Google Ads API version.
# Do NOT hardcode version strings elsewhere in this module or any caller.
#
# Version lifecycle (check https://ads.google.com/api/docs/sunset-dates):
#   v20  — SUNSETS 2026-06-10. All v20 requests fail after this date.
#   v21  — Current stable. Default below.
#   v22+ — Check release notes before upgrading; field names and enum values
#           may change between minor versions.
#
# To upgrade without code changes: set GOOGLE_ADS_API_VERSION=v22 in environment.
GOOGLE_ADS_API_VERSION: str = os.getenv("GOOGLE_ADS_API_VERSION", "v21")

# Micros conversion factor (Google Ads stores all monetary values as micros)
_MICROS: int = 1_000_000


# ── Exceptions ────────────────────────────────────────────────────────────────

class GoogleAdsSetupError(Exception):
    """Credentials not configured. See module docstring for setup instructions."""


class GoogleAdsAPIError(Exception):
    """Google Ads API returned an error response."""


class GoogleAdsBudgetGuardrailError(ValueError):
    """Proposed budget change exceeds max_budget_shift_pct guardrail."""


# ── Phase 1: Auth factory ─────────────────────────────────────────────────────

def _get_client() -> Any:
    """
    Return an authenticated GoogleAdsClient.

    Full Mode (headless / Cloud Run / GCP Secret Manager):
        All four env vars must be present:
          GOOGLE_ADS_DEVELOPER_TOKEN — issued by Google for your developer account
          GOOGLE_ADS_CLIENT_ID       — OAuth 2.0 client ID from Google Cloud Console
          GOOGLE_ADS_CLIENT_SECRET   — OAuth 2.0 client secret
          GOOGLE_ADS_REFRESH_TOKEN   — long-lived refresh token for the ads account

        Optional:
          GOOGLE_ADS_LOGIN_CUSTOMER_ID — Manager account (MCC) ID, required if
                                         the developer token belongs to an MCC.

        Client is instantiated via load_from_dict() so no file system is touched.
        GCP Secret Manager → env vars is the recommended injection pattern.

    Simple Mode (local practitioner):
        Falls back to ~/google-ads.yaml if env vars are absent.
        Generate it once with: python tools/setup_google_ads.py

    Raises:
        GoogleAdsSetupError if neither configuration path is available.
    """
    try:
        from google.ads.googleads.client import GoogleAdsClient  # type: ignore[import]
    except ImportError as exc:
        raise GoogleAdsSetupError(
            "google-ads package is not installed. "
            "Run: pip install 'google-ads>=24.1.0'"
        ) from exc

    dev_token     = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
    client_id     = os.getenv("GOOGLE_ADS_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
    refresh_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")

    if all([dev_token, client_id, client_secret, refresh_token]):
        # ── Full mode: headless, Cloud Run / Secret Manager ──────────────────
        config: dict[str, Any] = {
            "developer_token": dev_token,
            "client_id":       client_id,
            "client_secret":   client_secret,
            "refresh_token":   refresh_token,
            "use_proto_plus":  True,
        }
        login_cid = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
        if login_cid:
            config["login_customer_id"] = login_cid.replace("-", "")

        log.debug("google_ads.auth_mode", mode="full_headless", api_version=GOOGLE_ADS_API_VERSION)
        return GoogleAdsClient.load_from_dict(config, version=GOOGLE_ADS_API_VERSION)

    # ── Simple mode: local yaml ───────────────────────────────────────────────
    yaml_path = os.path.expanduser("~/google-ads.yaml")
    if os.path.exists(yaml_path):
        log.debug("google_ads.auth_mode", mode="local_yaml", path=yaml_path, api_version=GOOGLE_ADS_API_VERSION)
        return GoogleAdsClient.load_from_storage(yaml_path, version=GOOGLE_ADS_API_VERSION)

    raise GoogleAdsSetupError(
        "No Google Ads credentials found.\n\n"
        "Full Mode (Cloud Run / headless): set the following in your environment "
        "(pull from GCP Secret Manager via Secret Manager env var injection):\n"
        "  GOOGLE_ADS_DEVELOPER_TOKEN\n"
        "  GOOGLE_ADS_CLIENT_ID\n"
        "  GOOGLE_ADS_CLIENT_SECRET\n"
        "  GOOGLE_ADS_REFRESH_TOKEN\n"
        "  GOOGLE_ADS_LOGIN_CUSTOMER_ID  (if using an MCC / manager account)\n\n"
        "Simple Mode (local practitioner): run once to generate ~/google-ads.yaml:\n"
        "  python tools/setup_google_ads.py\n\n"
        f"Current API version: {GOOGLE_ADS_API_VERSION} "
        "(set GOOGLE_ADS_API_VERSION env var to override)"
    )


def _require_approval(action: str, payload: dict) -> None:
    """Raise ApprovalRequiredError if OPERATOR_REQUIRE_APPROVAL=true."""
    from tools.gmp_client import ApprovalRequiredError  # type: ignore[import]
    if settings.operator_require_approval:
        raise ApprovalRequiredError(
            f"OPERATOR_REQUIRE_APPROVAL=true. Pending Google Ads action: {action}\n"
            f"Payload: {json.dumps(payload, indent=2, default=str)}"
        )


def _check_credentials() -> None:
    """Probe credentials. Raises GoogleAdsSetupError with setup guidance if not configured."""
    _get_client()  # will raise descriptively if not configured


# ── Helpers ────────────────────────────────────────────────────────────────────

def _micros_to_numeric(micros: int | None) -> Decimal:
    """
    Convert Google Ads micros (millionths of currency unit) to NUMERIC Decimal.
    All financial fields in the Google Ads API are stored as micros.
    Using Decimal prevents floating-point rounding errors on aggregation.
    """
    if micros is None:
        return Decimal("0")
    return Decimal(str(micros)) / Decimal(str(_MICROS))


def _usd_to_micros(usd: float) -> int:
    """Convert USD float to micros integer for API writes."""
    return int(round(usd * _MICROS))


def _hash_email(email: str) -> str:
    """SHA-256 hash a normalized email for Customer Match upload."""
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


# Hard ceiling on a single GAQL stream — without it a stalled gRPC stream
# holds the Cloud Run instance until the platform request timeout.
GAQL_TIMEOUT_SECONDS = 120.0


def _run_gaql(customer_id: str, query: str) -> list[Any]:
    """Execute a GAQL query and return the list of GoogleAdsRow results."""
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")
    try:
        stream = ga_service.search_stream(
            customer_id=customer_id,
            query=query,
            timeout=GAQL_TIMEOUT_SECONDS,
        )
        rows = []
        for batch in stream:
            rows.extend(batch.results)
        return rows
    except Exception as exc:
        _raise_for_google_ads_error(exc)
        return []  # unreachable but satisfies type checkers


def _raise_for_google_ads_error(exc: Exception) -> None:
    try:
        from google.ads.googleads.errors import GoogleAdsException  # type: ignore[import]
        if isinstance(exc, GoogleAdsException):
            errors = [e.message for e in exc.failure.errors]
            raise GoogleAdsAPIError(f"Google Ads API error: {'; '.join(errors)}") from exc
    except ImportError:
        pass
    raise GoogleAdsAPIError(str(exc)) from exc


# ── Phase 2: GAQL campaign performance ────────────────────────────────────────

def get_campaign_performance(
    customer_id: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """
    Daily campaign-level performance via GAQL.

    Returns one row per (campaign, date) with:
      campaign_id, campaign_name, advertising_channel_type, status,
      campaign_budget_resource_name, budget_amount (NUMERIC),
      impressions (INT64), clicks (INT64), interactions (INT64),
      spend (NUMERIC), conversions (NUMERIC), conversion_value (NUMERIC),
      ctr (FLOAT), avg_cpc (NUMERIC), date

    Financial fields (spend, conversion_value, budget_amount) are NUMERIC (Decimal)
    to prevent floating-point rounding errors in downstream aggregations.
    impressions / clicks / interactions are INT64.
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.advertising_channel_sub_type,
            campaign_budget.resource_name,
            campaign_budget.amount_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.interactions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.ctr,
            metrics.average_cpc,
            segments.date
        FROM campaign
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """
    rows = _run_gaql(customer_id, query)
    return [_parse_campaign_row(r) for r in rows]


def _parse_campaign_row(row: Any) -> dict:
    c = row.campaign
    m = row.metrics
    s = row.segments
    b = row.campaign_budget
    return {
        "campaign_id":                   str(c.id),
        "campaign_name":                 str(c.name),
        "status":                        str(c.status.name),
        "advertising_channel_type":      str(c.advertising_channel_type.name),
        "advertising_channel_sub_type":  str(c.advertising_channel_sub_type.name),
        "campaign_budget_resource_name": str(b.resource_name),
        "budget_amount":                 _micros_to_numeric(b.amount_micros),
        "date":                          str(s.date),
        # INT64 — never float for counts
        "impressions":   int(m.impressions),
        "clicks":        int(m.clicks),
        "interactions":  int(m.interactions),
        # NUMERIC — Decimal for all financial values
        "spend":             _micros_to_numeric(m.cost_micros),
        "conversions":       Decimal(str(m.conversions)),
        "conversion_value":  Decimal(str(m.conversions_value)),
        # Rates — float acceptable
        "ctr":      float(m.ctr),
        "avg_cpc":  _micros_to_numeric(m.average_cpc),
    }


def get_keyword_performance(
    customer_id: str,
    date_from: str,
    date_to: str,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Daily keyword-level performance. Returns quality score, match type,
    impression share metrics, and spend. All financial fields are NUMERIC.
    """
    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.status,
            ad_group_criterion.quality_info.quality_score,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.search_impression_share,
            metrics.search_budget_lost_impression_share,
            metrics.search_rank_lost_impression_share,
            segments.date
        FROM keyword_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND ad_group_criterion.status != 'REMOVED'
          AND ad_group_criterion.negative = FALSE
          {campaign_filter}
        ORDER BY metrics.cost_micros DESC
    """
    rows = _run_gaql(customer_id, query)
    result = []
    for r in rows:
        c = r.campaign
        ag = r.ad_group
        crit = r.ad_group_criterion
        m = r.metrics
        s = r.segments
        result.append({
            "campaign_id":   str(c.id),
            "campaign_name": str(c.name),
            "ad_group_id":   str(ag.id),
            "ad_group_name": str(ag.name),
            "criterion_id":  str(crit.criterion_id),
            "keyword_text":  str(crit.keyword.text),
            "match_type":    str(crit.keyword.match_type.name),
            "status":        str(crit.status.name),
            "quality_score": int(crit.quality_info.quality_score) if crit.quality_info.quality_score else None,
            "date":          str(s.date),
            # INT64
            "impressions": int(m.impressions),
            "clicks":      int(m.clicks),
            # NUMERIC
            "spend":            _micros_to_numeric(m.cost_micros),
            "conversions":      Decimal(str(m.conversions)),
            "conversion_value": Decimal(str(m.conversions_value)),
            # Rates — float
            "search_impression_share":             float(m.search_impression_share or 0),
            "search_budget_lost_impression_share": float(m.search_budget_lost_impression_share or 0),
            "search_rank_lost_impression_share":   float(m.search_rank_lost_impression_share or 0),
        })
    return result


# ── Phase 2: Performance Max + shopping_performance_view ──────────────────────

def get_pmax_performance(
    customer_id: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """
    Performance Max campaign performance via shopping_performance_view.

    IMPORTANT — June 15, 2026 API change:
        Google unifies PMax network reporting inside shopping_performance_view.
        Before June 15: only Standard Shopping rows are returned.
        After June 15:  PMax rows appear with advertising_channel_type=PERFORMANCE_MAX.
        This causes an apparent metric spike in the view — it is NOT real volume growth.
        It reflects data that was previously absent from this resource.

        Downstream dashboards comparing date ranges across June 15 should apply a
        channel_type split or restrict to a consistent post-June-15 window.

        The parser below handles both row types natively via _parse_pmax_row().
        New multi-network fields are extracted if present; missing fields default to None.
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            segments.product_item_id,
            segments.product_type_l1,
            segments.product_type_l2,
            segments.product_brand,
            segments.product_channel,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            segments.date
        FROM shopping_performance_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """
    rows = _run_gaql(customer_id, query)
    return [_parse_pmax_row(r) for r in rows]


def _parse_pmax_row(row: Any) -> dict:
    """
    Parse a shopping_performance_view row for both Standard Shopping and PMax.

    Post-June-15 expanded fields on PMax rows (advertising_channel_type=PERFORMANCE_MAX):
      - product-level cost, conversions, and multi-network breakdown fields
    These are accessed defensively via getattr so the parser never throws a schema
    mismatch error regardless of whether the June 15 rollout has occurred.
    """
    c = row.campaign
    m = row.metrics
    s = row.segments

    channel_type = str(c.advertising_channel_type.name)
    is_pmax = (channel_type == "PERFORMANCE_MAX")

    parsed: dict[str, Any] = {
        "campaign_id":          str(c.id),
        "campaign_name":        str(c.name),
        "advertising_channel_type": channel_type,
        "is_pmax":              is_pmax,
        "product_item_id":      str(s.product_item_id) if s.product_item_id else None,
        "product_type_l1":      str(s.product_type_l1) if s.product_type_l1 else None,
        "product_type_l2":      str(s.product_type_l2) if s.product_type_l2 else None,
        "product_brand":        str(s.product_brand) if s.product_brand else None,
        "product_channel":      str(s.product_channel.name) if s.product_channel else None,
        "date":                 str(s.date),
        # INT64
        "impressions": int(m.impressions),
        "clicks":      int(m.clicks),
        # NUMERIC — enforced for all financial fields
        "spend":            _micros_to_numeric(m.cost_micros),
        "conversions":      Decimal(str(m.conversions)),
        "conversion_value": Decimal(str(m.conversions_value)),
    }

    # ── Post-June-15 PMax expanded fields ──────────────────────────────────
    # These attributes appear on PMax rows after the June 15 shopping_performance_view
    # unification. Use getattr with defaults so this parser works identically before
    # and after the rollout — no schema mismatch, no KeyError.
    expanded_micros_fields = [
        "cross_sell_revenue_micros",
        "lead_revenue_micros",
        "all_conversions_value",
    ]
    expanded_count_fields = [
        "all_conversions",
        "cross_sell_units_sold",
        "lead_units_sold",
    ]

    if is_pmax:
        for field in expanded_micros_fields:
            val = getattr(m, field, None)
            parsed[field.replace("_micros", "")] = (
                _micros_to_numeric(val) if val is not None else None
            )
        for field in expanded_count_fields:
            val = getattr(m, field, None)
            parsed[field] = Decimal(str(val)) if val is not None else None

    return parsed


# ── Phase 3: Geo dimensions for Meridian MMM ──────────────────────────────────

def get_geo_performance(
    customer_id: str,
    date_from: str,
    date_to: str,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Geo-segmented campaign performance for Google Meridian MMM input.

    Returns spend and impressions broken down by geographic criterion (state / DMA),
    which Meridian uses as the geo-level input for its Bayesian hierarchical model.

    Geo fields returned:
      geo_target_constant — resource name of the geo target (e.g. "geoTargetConstants/1014044")
      country_criterion_id — ISO numeric code of the matched country
      location_type — LOCATION_OF_PRESENCE | AREA_OF_INTEREST

    To map geo_target_constant to state/DMA names, join against the GeoTargetConstant
    reference table via the GeoTargetConstantService (not done here to keep this
    function fast and dependency-light). For Meridian, the numeric criterion ID is
    sufficient as the geo key — name resolution is a display concern.

    NOTE — Meridian JAX backend requirement (see Task 27):
        Meridian's MCMC sampling relies on TensorFlow Probability with a JAX backend.
        When running the MMM agent on Cloud Run, configure the JAX XLA backend
        (XLA_FLAGS=--xla_force_host_platform_device_count=N) to stay within the
        60-minute execution timeout. The default TF backend is significantly slower
        on CPU-only Cloud Run instances.
    """
    campaign_filter = f"AND campaign.id = {campaign_id}" if campaign_id else ""
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            geographic_view.country_criterion_id,
            geographic_view.location_type,
            segments.geo_target_constant,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            segments.date
        FROM geographic_view
        WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
          AND campaign.status != 'REMOVED'
          {campaign_filter}
        ORDER BY metrics.cost_micros DESC
    """
    rows = _run_gaql(customer_id, query)
    result = []
    for r in rows:
        c = r.campaign
        gv = r.geographic_view
        m = r.metrics
        s = r.segments
        # Extract numeric criterion ID from resource name
        # e.g. "geoTargetConstants/1014044" → "1014044"
        geo_constant = str(s.geo_target_constant)
        geo_criterion_id = geo_constant.split("/")[-1] if "/" in geo_constant else geo_constant

        result.append({
            "campaign_id":           str(c.id),
            "campaign_name":         str(c.name),
            "channel_type":          str(c.advertising_channel_type.name),
            "geo_target_constant":   geo_constant,
            "geo_criterion_id":      geo_criterion_id,
            "country_criterion_id":  str(gv.country_criterion_id),
            "location_type":         str(gv.location_type.name),
            "date":                  str(s.date),
            # INT64
            "impressions": int(m.impressions),
            "clicks":      int(m.clicks),
            # NUMERIC
            "spend":            _micros_to_numeric(m.cost_micros),
            "conversions":      Decimal(str(m.conversions)),
            "conversion_value": Decimal(str(m.conversions_value)),
        })
    return result


# ── Phase 3: Incrementality calibration hook ──────────────────────────────────

def get_incrementality_lift_stub(
    customer_id: str,
    campaign_id: str,
    experiment_id: str | None = None,
) -> dict:
    """
    Stub interface for Task 22/24 incrementality lift results.

    This function provides a clean ingestion interface for the Analyst agent
    to pass causal lift percentages from incrementality experiments into the
    paid-media-schema `incrementality_lift_results` table, which will in turn
    calibrate Google Meridian's Bayesian priors in Task 27.

    Shape of the expected incrementality_lift_results table (to be created in Task 22):
      experiment_id        STRING    — identifier linking to the lift study
      campaign_id          STRING    — Google Ads campaign being tested
      channel              STRING    — e.g. "paid_search_brand"
      lift_pct             NUMERIC   — measured incremental lift (e.g. 0.35 = 35%)
      confidence_interval_lower NUMERIC
      confidence_interval_upper NUMERIC
      measurement_window_days INT64
      methodology          STRING    — "geo_holdout" | "conversion_lift" | "brand_lift"
      measured_at          TIMESTAMP
      calibration_weight   NUMERIC   — weight to apply when setting Meridian priors (0-1)

    NOTE (Task 27 — Meridian calibration):
        Meridian accepts rf_spend (reach + frequency) and roi_rf as prior distributions.
        The lift_pct from this table should be used to set the roi_calibration_data
        argument when constructing the Meridian model object. See Task 27 for full
        integration details.

    Returns a dict describing what the Analyst agent should write to BQ.
    Full implementation: Task 22 (incrementality testing).
    """
    return {
        "stub": True,
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "experiment_id": experiment_id,
        "message": (
            "Incrementality lift ingestion not yet implemented. "
            "Task 22 will build the full EnrichedLiftResult → BQ pipeline. "
            "This stub documents the expected interface."
        ),
        "expected_table": "incrementality_lift_results",
        "meridian_calibration_field": "roi_calibration_data",
    }


# ── Customer Match audience management ────────────────────────────────────────

def get_customer_match_user_lists(customer_id: str) -> list[dict]:
    """
    List all Customer Match user lists for the account.
    Returns id, name, size, and resource_name for each list.
    """
    query = """
        SELECT
            user_list.id,
            user_list.name,
            user_list.description,
            user_list.membership_status,
            user_list.size_for_display,
            user_list.size_for_search,
            user_list.resource_name,
            user_list.type
        FROM user_list
        WHERE user_list.type = 'CRM_BASED'
        ORDER BY user_list.name
    """
    rows = _run_gaql(customer_id, query)
    result = []
    for r in rows:
        ul = r.user_list
        result.append({
            "user_list_id":        str(ul.id),
            "name":                str(ul.name),
            "description":         str(ul.description),
            "membership_status":   str(ul.membership_status.name),
            "size_for_display":    int(ul.size_for_display or 0),
            "size_for_search":     int(ul.size_for_search or 0),
            "resource_name":       str(ul.resource_name),
        })
    return result


def create_customer_match_user_list(
    customer_id: str,
    list_name: str,
    description: str = "",
    membership_life_span_days: int = 540,
) -> dict:
    """
    Create a new Customer Match user list for B2B audience suppression.

    For B2B domain-based suppression: create one list per domain segment
    (e.g. "Suppressed — Open Pipeline Accounts") and load hashed emails from CRM.

    Returns the new user_list resource_name to pass to add_emails_to_customer_match().
    """
    _require_approval("google_ads_create_user_list", {"list_name": list_name})

    client = _get_client()
    user_list_service = client.get_service("UserListService")
    user_list_op = client.get_type("UserListOperation")

    user_list = user_list_op.create
    user_list.name = list_name
    user_list.description = description
    user_list.membership_life_span = membership_life_span_days
    user_list.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )

    try:
        response = user_list_service.mutate_user_lists(
            customer_id=customer_id,
            operations=[user_list_op],
        )
        resource_name = response.results[0].resource_name
        log.info("google_ads.user_list_created", customer_id=customer_id, name=list_name, resource_name=resource_name)
        return {"resource_name": resource_name, "list_name": list_name}
    except Exception as exc:
        _raise_for_google_ads_error(exc)
        return {}


def add_emails_to_customer_match(
    customer_id: str,
    user_list_resource_name: str,
    hashed_emails: list[str],
) -> dict:
    """
    Upload SHA-256 hashed emails to a Customer Match user list.

    For B2B domain suppression: pull all employee emails for each target domain
    from CRM, hash them here via hash_emails_for_customer_match(), and upload.

    hashed_emails must be normalized: SHA-256(email.lower().strip()).hexdigest()
    Use hash_emails_for_customer_match() to normalize before calling this function.

    Uploads via OfflineUserDataJobService (current recommended approach).
    The job runs asynchronously — list membership updates within ~24h.
    """
    _require_approval(
        "google_ads_customer_match_upload",
        {"user_list": user_list_resource_name, "email_count": len(hashed_emails)}
    )

    client = _get_client()
    offline_job_service = client.get_service("OfflineUserDataJobService")

    # 1 — Create the offline user data job
    job = client.get_type("OfflineUserDataJob")
    job.type_ = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    job.customer_match_user_list_metadata.user_list = user_list_resource_name

    try:
        create_resp = offline_job_service.create_offline_user_data_job(
            customer_id=customer_id,
            job=job,
        )
        job_resource_name = create_resp.resource_name

        # 2 — Build add operations (batched to respect API limits)
        _BATCH_SIZE = 1_000
        operations = []
        for hashed_email in hashed_emails:
            op = client.get_type("OfflineUserDataJobOperation")
            user_data = op.create
            identifier = client.get_type("UserIdentifier")
            identifier.hashed_email = hashed_email
            user_data.user_identifiers.append(identifier)
            operations.append(op)

        for i in range(0, len(operations), _BATCH_SIZE):
            batch = operations[i : i + _BATCH_SIZE]
            offline_job_service.add_offline_user_data_job_operations(
                resource_name=job_resource_name,
                operations=batch,
            )

        # 3 — Run the job (async — completes within ~24h)
        offline_job_service.run_offline_user_data_job(resource_name=job_resource_name)

        log.info(
            "google_ads.customer_match_upload",
            customer_id=customer_id,
            user_list=user_list_resource_name,
            emails=len(hashed_emails),
            job=job_resource_name,
        )
        return {
            "job_resource_name":    job_resource_name,
            "emails_uploaded":      len(hashed_emails),
            "user_list":            user_list_resource_name,
            "status":               "running",
            "note":                 "Customer Match jobs complete asynchronously (typically within 24h).",
        }
    except Exception as exc:
        _raise_for_google_ads_error(exc)
        return {}


def remove_emails_from_customer_match(
    customer_id: str,
    user_list_resource_name: str,
    hashed_emails: list[str],
) -> dict:
    """Remove hashed emails from a Customer Match list (reverse of add_emails_to_customer_match)."""
    _require_approval(
        "google_ads_customer_match_remove",
        {"user_list": user_list_resource_name, "email_count": len(hashed_emails)}
    )

    client = _get_client()
    offline_job_service = client.get_service("OfflineUserDataJobService")

    job = client.get_type("OfflineUserDataJob")
    job.type_ = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    job.customer_match_user_list_metadata.user_list = user_list_resource_name

    try:
        create_resp = offline_job_service.create_offline_user_data_job(
            customer_id=customer_id, job=job
        )
        job_resource_name = create_resp.resource_name

        operations = []
        for hashed_email in hashed_emails:
            op = client.get_type("OfflineUserDataJobOperation")
            user_data = op.remove
            identifier = client.get_type("UserIdentifier")
            identifier.hashed_email = hashed_email
            user_data.user_identifiers.append(identifier)
            operations.append(op)

        offline_job_service.add_offline_user_data_job_operations(
            resource_name=job_resource_name, operations=operations
        )
        offline_job_service.run_offline_user_data_job(resource_name=job_resource_name)

        log.info("google_ads.customer_match_remove", emails=len(hashed_emails), job=job_resource_name)
        return {"job_resource_name": job_resource_name, "emails_removed": len(hashed_emails)}
    except Exception as exc:
        _raise_for_google_ads_error(exc)
        return {}


def hash_emails_for_customer_match(emails: list[str]) -> list[str]:
    """
    Normalize and SHA-256 hash a list of emails for Customer Match upload.
    Normalization: lowercase + strip whitespace (Google's required format).
    """
    return [_hash_email(e) for e in emails if e.strip()]


def push_domain_suppression(
    customer_id: str,
    user_list_resource_name: str,
    domains: list[str],
    crm_emails_by_domain: dict[str, list[str]] | None = None,
) -> dict:
    """
    Add company domains to a Customer Match exclusion list.

    Google Ads does not natively support domain-based suppression (unlike Meta).
    This function uses the best available approach:

    Preferred (if CRM emails provided):
        Hash all known employee emails per domain and upload via Customer Match.
        crm_emails_by_domain = {"acme.com": ["alice@acme.com", "bob@acme.com"], ...}
        Higher match rate than domain matching. Recommended for B2B suppression.

    Fallback (no CRM emails):
        Domain-level suppression is not directly supported by Google Ads API.
        Returns a queued status with a note to use placement exclusions or manually
        upload a customer list from the UI.
    """
    # ── Resolve crm_emails_by_domain ──────────────────────────────────────────
    # If not passed explicitly, auto-fetch from crm_leads_staging via crm_client.
    # This resolves the Task 22 TODO: CRM lookup is now wired automatically.
    if crm_emails_by_domain is None:
        try:
            from tools.crm_client import get_crm_emails_by_domain
            crm_emails_by_domain = get_crm_emails_by_domain(domains=domains)
            log.info(
                "google_ads.suppression.crm_auto_fetched",
                customer_id=customer_id,
                domains=len(domains),
                domains_matched=len(crm_emails_by_domain),
            )
        except Exception as exc:
            log.warning(
                "google_ads.suppression.crm_fetch_failed",
                error=str(exc),
                note="Falling back to manual suppression workflow.",
            )
            crm_emails_by_domain = {}

    if crm_emails_by_domain:
        all_emails: list[str] = []
        for domain in domains:
            all_emails.extend(crm_emails_by_domain.get(domain, []))

        if not all_emails:
            return {
                "status": "no_emails",
                "platform": "google_ads",
                "domains_requested": len(domains),
                "note": (
                    "CRM data found but no emails matched the given domains. "
                    "Verify domain keys in crm_emails_by_domain match the domains list exactly. "
                    "Run crm_client.summarize_domain_coverage(domains) to diagnose coverage gaps."
                ),
            }

        hashed = hash_emails_for_customer_match(all_emails)
        return add_emails_to_customer_match(customer_id, user_list_resource_name, hashed)

    # Fallback: no emails available in CRM for these domains
    log.warning("google_ads.domain_suppression_fallback", domains=len(domains))
    return {
        "status": "queued_manual",
        "platform": "google_ads",
        "domains_requested": len(domains),
        "domain_list": domains,
        "note": (
            "Google Ads does not support domain-based Customer Match directly. "
            "No CRM email records found for the requested domains in crm_leads_staging. "
            "To resolve: (1) ensure crm_leads_staging is populated for these domains, "
            "or (2) pass crm_emails_by_domain explicitly; "
            "or (3) manually upload a customer list via Google Ads UI → Audience Manager."
        ),
    }


# ── Budget management ──────────────────────────────────────────────────────────

def get_campaign(customer_id: str, campaign_id: str) -> dict:
    """
    Fetch a single campaign with its current budget.
    Returns campaign name, status, channel type, budget resource name, and
    budget amount (NUMERIC Decimal in account currency).
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign_budget.resource_name,
            campaign_budget.amount_micros,
            campaign_budget.total_amount_micros,
            campaign_budget.type,
            campaign_budget.period
        FROM campaign
        WHERE campaign.id = {campaign_id}
        LIMIT 1
    """
    rows = _run_gaql(customer_id, query)
    if not rows:
        raise GoogleAdsAPIError(f"Campaign {campaign_id} not found in customer {customer_id}")

    r = rows[0]
    c = r.campaign
    b = r.campaign_budget
    return {
        "campaign_id":                   str(c.id),
        "campaign_name":                 str(c.name),
        "status":                        str(c.status.name),
        "advertising_channel_type":      str(c.advertising_channel_type.name),
        "campaign_budget_resource_name": str(b.resource_name),
        "budget_amount":                 _micros_to_numeric(b.amount_micros),
        "budget_amount_micros":          int(b.amount_micros or 0),
        "budget_type":                   str(b.type_.name) if b.type_ else "STANDARD",
        "budget_period":                 str(b.period.name) if b.period else "DAILY",
    }


def update_campaign_budget(
    customer_id: str,
    campaign_budget_resource_name: str,
    new_amount_usd: float,
    current_amount_usd: float | None = None,
) -> dict:
    """
    Set a campaign's budget to new_amount_usd.
    Enforces max_budget_shift_pct guardrail if current_amount_usd is provided.
    Converts USD to micros for the API call.
    """
    if current_amount_usd and current_amount_usd > 0:
        change_pct = abs(new_amount_usd - current_amount_usd) / current_amount_usd * 100
        if change_pct > settings.max_budget_shift_pct:
            raise GoogleAdsBudgetGuardrailError(
                f"Budget change of {change_pct:.1f}% exceeds guardrail of "
                f"{settings.max_budget_shift_pct}%. "
                f"Current: ${current_amount_usd:.2f}, Proposed: ${new_amount_usd:.2f}. "
                f"Override by adjusting MAX_BUDGET_SHIFT_PCT in settings."
            )

    _require_approval(
        "google_ads_budget_update",
        {
            "campaign_budget_resource": campaign_budget_resource_name,
            "new_amount_usd": new_amount_usd,
            "current_amount_usd": current_amount_usd,
        }
    )

    client = _get_client()
    campaign_budget_service = client.get_service("CampaignBudgetService")
    op = client.get_type("CampaignBudgetOperation")

    budget = op.update
    budget.resource_name = campaign_budget_resource_name
    budget.amount_micros = _usd_to_micros(new_amount_usd)

    from google.protobuf import field_mask_pb2  # type: ignore[import]
    op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["amount_micros"]))

    try:
        response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[op],
        )
        log.info(
            "google_ads.budget_updated",
            customer_id=customer_id,
            resource=campaign_budget_resource_name,
            new_usd=new_amount_usd,
        )
        return {
            "campaign_budget_resource_name": response.results[0].resource_name,
            "new_amount_usd": new_amount_usd,
            "new_amount_micros": _usd_to_micros(new_amount_usd),
            "previous_amount_usd": current_amount_usd,
        }
    except Exception as exc:
        _raise_for_google_ads_error(exc)
        return {}


def reallocate_campaign_budget(
    customer_id: str,
    source_campaign_id: str,
    target_campaign_id: str,
    amount_usd: float,
) -> dict:
    """
    Move amount_usd from source_campaign's budget to target_campaign's budget.
    Enforces max_budget_shift_pct on both campaigns.
    Both campaigns must use separate CampaignBudget objects (not a shared budget).
    """
    source = get_campaign(customer_id, source_campaign_id)
    target = get_campaign(customer_id, target_campaign_id)

    source_current = float(source["budget_amount"])
    target_current = float(target["budget_amount"])

    new_source = max(1.0, source_current - amount_usd)
    new_target = target_current + amount_usd

    r_source = update_campaign_budget(
        customer_id,
        source["campaign_budget_resource_name"],
        new_source,
        source_current,
    )
    r_target = update_campaign_budget(
        customer_id,
        target["campaign_budget_resource_name"],
        new_target,
        target_current,
    )

    log.info(
        "google_ads.budget_reallocated",
        customer_id=customer_id,
        source=source_campaign_id,
        target=target_campaign_id,
        amount_usd=amount_usd,
    )
    return {
        "platform":        "google_ads",
        "customer_id":     customer_id,
        "source_campaign": {**r_source, "campaign_id": source_campaign_id, "campaign_name": source["campaign_name"]},
        "target_campaign": {**r_target, "campaign_id": target_campaign_id, "campaign_name": target["campaign_name"]},
        "amount_moved_usd": amount_usd,
    }
