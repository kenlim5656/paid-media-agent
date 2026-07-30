# Cross-Repo Data Contract — paid-media-agent ↔ paid-media-mcp

This document is the single source of truth for the interfaces shared between
`paid-media-agent` (writer) and `paid-media-mcp` (reader). If either side
changes a table, column, or package field listed here, update this file in the
same PR and mirror the change in the other repo before merging.

- Agent (writer): [kenlim5656/paid-media-agent](https://github.com/kenlim5656/paid-media-agent)
- MCP (reader): [kenlim5656/paid-media-mcp](https://github.com/kenlim5656/paid-media-mcp)

---

## 1. Agent-output tables the MCP reads

DDL lives in `schema/bigquery/` (this repo). Column lists below are the
*contract subset* — the columns the MCP actually maps. Adding columns is safe;
renaming or removing any of these is a breaking change.

| Table | DDL | MCP consumer | Contract columns |
|---|---|---|---|
| `watchdog_alerts` | `05_agent_outputs.sql` | `get_watchdog_alerts` | `alert_id`, `alert_type`, `severity` (info\|warning\|critical), `status` (open\|acknowledged\|resolved\|suppressed), `affected_namespace`, `affected_platform`, `metric_name`, `metric_value`, `threshold_value`, `description`, `probable_cause`, `recommended_action`, `detected_at`, `resolved_at` |
| `analyst_insights` | `05_agent_outputs.sql` | `get_analyst_insights` | `insight_id`, `insight_type`, `period_start`, `period_end`, `affected_platform`, `affected_channel`, `headline`, `detail`, `confidence`, `has_recommendation`, `recommendation`, `estimated_impact`, `priority` (high\|medium\|low), `status`, `generated_at` |
| `operator_action_log` | `05_agent_outputs.sql` | (audit trail; written via MCP-triggered actions) | `action_id`, `action_type`, `platform`, `status` (proposed\|approved\|executed\|partial\|failed\|rejected\|superseded), `guardrail_notes` (carries `package_hash=<sha256>` on task27 execution records), `executed_at` |
| `operator_pending_approvals` | `05_agent_outputs.sql` | `get_pending_approvals` | `action_id`, `platform`, `action_type`, `platform_entity_id`, `campaign_id`, `summary`, `rationale`, `estimated_impact`, `spend_at_risk`, `change_magnitude_pct`, `proposed_at`, `expires_at` |
| `attribution_runs` | `04_attribution.sql` | `get_attribution_run_history` | `run_id`, `model_name`, `period_start`, `period_end`, `paths_modeled`, `conversions_attributed`, `identity_match_rate`, `avg_path_length`, `status` (`completed` selects the latest run), `started_at`, `completed_at`, `triggered_by` |
| `attribution_channel_summary` | `04_attribution.sql` | `get_attribution_results` | `run_id`, `platform`, `channel`, `conversion_type`, `attributed_conversions`, `attributed_value`, `credit_share_pct`, `total_spend`, `attributed_cpa`, `attributed_roas` |
| `attribution_results` | `04_attribution.sql` | `query_account_journey` (credit join) | `run_id`, `touchpoint_id`, `credit_weight`, `credit_conversions`, `model_name` |
| `identity_entities` / `identity_entity_signals` | `01_identity.sql` | `query_account_journey`, identity tools | `entity_id`, `company_domain`, `identifier_value`, `is_active` |
| `platform_campaigns` | `03_platform.sql` | `list_campaigns` | `campaign_id`, `campaign_name`, `platform`, `platform_account_id`, `team_id`, `status`, `objective`, `funnel_stage`, `budget_amount`, `budget_type`, `budget_currency`, `start_date`, `end_date`, `notes`, `tags` |
| `platform_daily_spend` | `03_platform.sql` | `get_campaign_performance`, `get_daily_performance` | `date`, `campaign_id`, `platform`, `impressions`, `clicks`, `spend`, `platform_conversions`, `platform_conversion_value`, `reach`, `video_views`, `ctr`, `cpc`, `cpm`, `platform_cpa`, `platform_roas` |
| `company_profiles` / `company_sessions` / `company_engagement` / `target_account_activity` | `07_account_analytics.sql` | account-analytics tools | `company_domain` is the join key everywhere; `is_active` on profiles |

External staging tables (`sessions`, `crm_leads_staging`,
`crm_opportunities_staging`) are populated by *your* ETL — column contract in
`schema/bigquery/18_external_staging.sql`.

## 2. HTTP routes the MCP calls on `PAID_MEDIA_AGENT_URL`

| Route | Request body | Response (success) |
|---|---|---|
| `POST /run?agent=<watchdog\|analyst\|operator>` | `{reason?}` | `{agent, result}` |
| `POST /query/account-journey` | `{account_domain, lookback_days=90, conversion_type?}` | `{account_domain, entity_count, touchpoints[], conversions[], path_summary{}}` |
| `POST /action/audience-suppression` | `{platform, advertiser_id, audience_list_id, domains[], rationale}` | `{executed, requires_approval, action_id, action_log_updated, ...platform fields}` |
| `POST /action/reallocate-budget` | `{platform, advertiser_id, source_campaign_id, target_campaign_id, amount_usd, rationale}` | `{executed, requires_approval, action_id, action_log_updated, ...platform fields}` |

All routes require a Google-signed OIDC identity token
(`Authorization: Bearer <id-token>`); see SETUP.md. Non-2xx responses carry a
JSON `{detail}` body which the MCP surfaces verbatim.

## 3. `task27.v1` MMM execution package

Produced by `tools/mmm_optimizer_analyst.py` (Analyst), consumed by
`agents/operator/agent.py::execute_system_budget_reallocation` (Operator).
Not yet consumed by the MCP — a package-aware tool is a noted future
integration point (`src/tools/media-actions.ts` TODO).

```jsonc
{
  "schema_version": "task27.v1",            // REQUIRED — operator rejects others
  "generated_at": "<ISO timestamp>",
  "mmm_run_id": "<mmm_runs.run_id>",
  "model_window_from": "YYYY-MM-DD",
  "model_window_to": "YYYY-MM-DD",
  "r_hat_max": 1.01,                        // MCMC convergence diagnostic
  "operator_approval_required": true,        // REQUIRED true — tamper-checked
  "max_shift_pct_policy": 10.0,              // pre-flight ceiling on |shift_pct|
  "recommendations": [
    {
      "action": "adjust_channel_budget",
      "channel": "google_ads",               // google_ads | tiktok | meta | linkedin | reddit_ads
      "direction": "increase",               // increase | decrease
      "recommended_shift_pct": 8.0,          // |value| must be ≤ max_shift_pct_policy
      "recommended_shift_usd": 120.0,
      "new_target_budget_usd": 54.0,         // absolute target; floors: google_ads ≥ $5, tiktok ≥ $20
      "adj_roi_mean": 1.42,
      "portfolio_avg_roi": 1.10,
      "confidence_tier": "high",
      "bsts_alignment": "aligned",
      "roi_prior_injected": false,
      "roi_prior_source": null
    }
  ],
  "note": "free-text caveats"
}
```

Execution semantics (enforced by the Operator):
- **Pre-flight is all-or-nothing**: any violation ⇒ zero mutations, full error list.
- **Idempotency**: the SHA-256 of the canonical package is recorded in
  `operator_action_log.guardrail_notes` (`package_hash=<hash>`) whenever any
  channel mutates; an identical package is rejected with `replay_blocked: true`.
- **Terminal status**: `executed` | `partial` | `failed` — the execution loop is
  *not* transactional across platforms; `partial` requires human reconciliation.
