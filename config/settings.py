# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Anthropic
    anthropic_api_key: str
    claude_model: str = "claude-opus-4-8"

    # GCP / BigQuery
    gcp_project_id: str
    gcp_dataset_id: str = "paid_media"

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

    # Legacy aliases (kept for backwards compat with existing code)
    @property
    def gclid_capture_floor_pct(self) -> float:
        return self.capture_floor_pct


settings = Settings()  # type: ignore[call-arg]
