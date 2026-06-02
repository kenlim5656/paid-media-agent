from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Anthropic
    anthropic_api_key: str
    claude_model: str = "claude-opus-4-8"

    # GCP / BigQuery
    gcp_project_id: str
    gcp_dataset_id: str = "attribution"

    # Salesforce
    sf_username: str
    sf_password: str
    sf_security_token: str
    sf_domain: str = "login"

    # Google Marketing Platform
    cm360_profile_id: str
    dv360_partner_id: str
    sa360_agency_id: str

    # GTM
    gtm_server_container_url: str

    # Guardrails
    max_budget_shift_pct: float = 10.0
    operator_require_approval: bool = True
    alert_webhook_url: str = ""

    # Monitoring thresholds
    gclid_capture_floor_pct: float = 90.0   # alert if gclid rate drops below this
    null_field_spike_pct: float = 5.0        # alert if null spike exceeds this


settings = Settings()  # type: ignore[call-arg]
