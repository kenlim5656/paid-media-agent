# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

import os

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env relative to the repo root (parent of this config/ directory).
# This makes the path work regardless of the process working directory,
# which is critical for Cloud Run, pytest, and direct CLI invocations.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV_FILE  = os.path.join(_REPO_ROOT, ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_ENV_FILE, extra="ignore")

    # Anthropic
    anthropic_api_key: str
    claude_model: str = "claude-opus-4-8"
    agent_max_iterations: int = 50          # cap on tool-use loop turns per run
    agent_api_timeout_seconds: float = 300.0  # per-call timeout on messages.create()

    # GCP / BigQuery
    # Canonical env vars (shared with paid-media-mcp): PAID_MEDIA_GCP_PROJECT,
    # PAID_MEDIA_BQ_DATASET. Legacy names GCP_PROJECT_ID / GCP_DATASET_ID are
    # accepted as fallbacks — validate_settings() warns when only legacy is set.
    gcp_project_id: str = Field(
        validation_alias=AliasChoices(
            "PAID_MEDIA_GCP_PROJECT", "GCP_PROJECT_ID", "gcp_project_id"
        ),
    )
    gcp_dataset_id: str = Field(
        default="paid_media",
        validation_alias=AliasChoices(
            "PAID_MEDIA_BQ_DATASET", "GCP_DATASET_ID", "gcp_dataset_id"
        ),
    )

    # Salesforce
    sf_username: str = ""
    sf_password: str = ""
    sf_security_token: str = ""
    sf_domain: str = "login"

    # Google Marketing Platform (GMP)
    cm360_profile_id: str = ""
    dv360_partner_id: str = ""
    sa360_agency_id: str = ""

    # Meta Ads
    meta_app_id: str = ""
    meta_app_secret: str = ""
    meta_access_token: str = ""
    meta_ad_account_id: str = ""        # format: act_123456789

    # LinkedIn Marketing API
    linkedin_client_id: str = ""
    linkedin_client_secret: str = ""
    linkedin_access_token: str = ""
    linkedin_partner_id: str = ""       # LinkedIn DMP partner ID

    # Google Ads API (Task 21)
    # Full Mode (headless / Cloud Run): set all four vars — pull from GCP Secret Manager.
    # Simple Mode (local practitioner): leave blank and run `python tools/setup_google_ads.py`.
    google_ads_developer_token: str = ""       # issued by Google at your developer account
    google_ads_client_id: str = ""             # OAuth 2.0 client ID (Google Cloud Console)
    google_ads_client_secret: str = ""         # OAuth 2.0 client secret
    google_ads_refresh_token: str = ""         # long-lived refresh token
    google_ads_login_customer_id: str = ""     # MCC / manager account ID (digits only, no dashes)
    google_ads_customer_id: str = ""           # default advertiser customer ID
    google_ads_api_version: str = "v21"        # centralized version — override GOOGLE_ADS_API_VERSION env var
    # v20 sunsets 2026-06-10. v21 is current stable. Do not hardcode elsewhere.

    # TikTok Ads Marketing API (Task 20)
    # Full Mode (headless / Cloud Run): set TIKTOK_ACCESS_TOKEN + TIKTOK_ADVERTISER_IDS.
    # Simple Mode (local practitioner): leave blank and run `python tools/setup_tiktok_ads.py`.
    tiktok_access_token: str = ""           # long-lived access token (~1 year expiry)
    tiktok_advertiser_id: str = ""          # default single advertiser ID
    tiktok_app_id: str = ""                 # OAuth app ID (from TikTok Ads Manager → API)
    tiktok_app_secret: str = ""             # OAuth app secret
    tiktok_api_version: str = "v1.3"        # centralized version — override TIKTOK_API_VERSION env var
    # v1.3 is current stable. To upgrade: change this value only — no code changes needed elsewhere.

    # GTM / Server-Side
    gtm_server_container_url: str = ""

    # Operator guardrails
    max_budget_shift_pct: float = 10.0      # max % of any budget to move in one run
    operator_require_approval: bool = True   # set False only after extended validation
    alert_webhook_url: str = ""             # Slack incoming webhook URL for alerts

    # HTTP auth (Cloud Run app) — verifies the OIDC token Cloud Scheduler / the
    # MCP proxy sends, in addition to Cloud Run's --no-allow-unauthenticated.
    http_auth_enabled: bool = True          # set False ONLY for local development
    http_auth_audience: str = ""            # expected `aud` claim — your Cloud Run URL
    http_auth_allowed_emails: str = ""      # comma-separated service-account emails
    #                                         allowed to call /run, /query/*, /action/*

    # Monitoring thresholds
    capture_floor_pct: float = 90.0         # alert if any signal capture rate drops below this
    null_field_spike_pct: float = 5.0       # alert if CRM null media fields exceed this %

    # Attribution model config
    attribution_lookback_days: int = 90     # default path lookback window
    shapley_max_channels: int = 10          # cap channel count for Shapley computation
    shapley_max_paths: int = 10_000         # sample paths if dataset exceeds this

    # IP intelligence — account-based analytics enrichment (Task 30)
    ip_intelligence_provider: str = "ipinfo"
    # "ipinfo"     ipinfo.io Company API (free tier: 50k lookups/month)
    # "clearbit"   Clearbit Reveal (premium B2B firmographics)
    # "composite"  Clearbit → ipinfo.io fallback chain

    ipinfo_access_token: str = ""           # ipinfo.io access token
    clearbit_api_key: str = ""              # Clearbit secret key
    rb2b_api_key: str = ""                  # RB2B webhook secret (push-mode verification)

    ip_resolution_cache_ttl_hours: int = 72         # cache TTL before re-resolving
    ip_resolution_confidence_threshold: float = 0.70  # minimum confidence to accept
    ip_enrichment_batch_size: int = 1_000            # sessions per enrichment run
    ip_enrichment_lookback_hours: int = 48           # look back this many hours for unenriched sessions

    # Reddit API — Social Listening (Task 25)
    # Developer app credentials from reddit.com/prefs/apps (script-type app).
    # Usage: pip install 'paid-media-agent[social]'
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "paid-media-agent/1.0 (social-listening)"

    # Reddit Ads Marketing API — Task 33
    # Full Mode (headless / Cloud Run): set all three vars.
    # Simple Mode (local): leave blank and run python tools/setup_reddit_ads.py → ~/reddit-ads.yaml
    # Account IDs use the Reddit entity prefix: t2_ (user account) or a2_ (ad account).
    # Multiple accounts: set REDDIT_ADS_ACCOUNT_IDS as comma-separated env var.
    # Scopes required: ads:read, ads:write
    # NOTE: distinct from REDDIT_CLIENT_ID/SECRET (Task 25 PRAW social listening).
    reddit_ads_client_id: str = ""
    reddit_ads_client_secret: str = ""
    reddit_ads_refresh_token: str = ""        # OAuth refresh token for persistent ad account access
    reddit_ads_account_id: str = ""           # default single account ID (t2_xxx or a2_xxx)
    reddit_ads_username: str = "paid-media-agent"  # used in User-Agent header
    reddit_ads_api_version: str = "v3"        # centralized version — override REDDIT_ADS_API_VERSION

    # Google Trends data provider — Social Listening (Task 25)
    # Set these three vars to use an authenticated marketing data provider
    # (DataForSEO Google Trends API or compatible service).
    # If unset, the client falls back to direct HTTP with rate-limit handling.
    # DataForSEO endpoint: https://api.dataforseo.com/v3/keywords_data/google_trends/explore/live
    google_trends_provider_url: str = ""       # POST endpoint URL
    google_trends_provider_username: str = ""  # Basic Auth username (email for DataForSEO)
    google_trends_provider_password: str = ""  # Basic Auth password (API key for DataForSEO)

    # Legacy aliases (kept for backwards compat with existing code)
    @property
    def gclid_capture_floor_pct(self) -> float:
        return self.capture_floor_pct


settings = Settings()  # type: ignore[call-arg]


# ── Startup validation ─────────────────────────────────────────────────────────

def _env_is_set(name: str) -> bool:
    """True if the var is set in the process environment or the .env file."""
    if os.environ.get(name):
        return True
    try:
        with open(_ENV_FILE) as fh:
            return any(
                line.strip().startswith(f"{name}=") and line.strip() != f"{name}="
                for line in fh
            )
    except OSError:
        return False


def validate_settings() -> None:
    """
    Fail fast with an actionable message before any agent or HTTP route runs.

    Call this at process startup (Cloud Run app lifespan, orchestrator main)
    rather than relying on a mid-run BigQuery or Anthropic error. Also emits
    deprecation warnings when only the legacy GCP env-var names are set.
    """
    import structlog
    log = structlog.get_logger()

    problems: list[str] = []
    if not settings.anthropic_api_key:
        problems.append("ANTHROPIC_API_KEY is not set")
    if not settings.gcp_project_id:
        problems.append(
            "PAID_MEDIA_GCP_PROJECT is not set (legacy fallback GCP_PROJECT_ID also empty)"
        )
    if not settings.gcp_dataset_id:
        problems.append(
            "PAID_MEDIA_BQ_DATASET is not set and no default applied "
            "(legacy fallback GCP_DATASET_ID also empty)"
        )
    if problems:
        raise RuntimeError(
            "Invalid configuration — fix .env or the deploy env vars:\n  - "
            + "\n  - ".join(problems)
        )

    for canonical, legacy in (
        ("PAID_MEDIA_GCP_PROJECT", "GCP_PROJECT_ID"),
        ("PAID_MEDIA_BQ_DATASET", "GCP_DATASET_ID"),
    ):
        if _env_is_set(legacy) and not _env_is_set(canonical):
            log.warning(
                "settings.deprecated_env_var",
                legacy=legacy,
                canonical=canonical,
                hint=f"{legacy} still works but is deprecated — rename it to {canonical}.",
            )

    if not settings.http_auth_enabled:
        log.warning(
            "settings.http_auth_disabled",
            hint="HTTP_AUTH_ENABLED=false — every route except /health is unauthenticated. "
                 "Local development only; never deploy like this.",
        )
    elif not settings.http_auth_allowed_emails:
        log.warning(
            "settings.http_auth_no_email_allowlist",
            hint="HTTP_AUTH_ALLOWED_EMAILS is empty — any Google-signed identity token "
                 "passes. Set it to your scheduler/MCP service-account email(s).",
        )
