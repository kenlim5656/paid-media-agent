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

    # TikTok Ads (future)
    tiktok_access_token: str = ""
    tiktok_advertiser_id: str = ""

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

    # Legacy aliases (kept for backwards compat with existing code)
    @property
    def gclid_capture_floor_pct(self) -> float:
        return self.capture_floor_pct


settings = Settings()  # type: ignore[call-arg]
