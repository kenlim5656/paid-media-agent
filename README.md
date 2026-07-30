# Paid Media Agent

**The autonomous execution layer of the Paid Media AI Suite** — three AI agents that forensically audit marketing data, model causal performance with Bayesian math, and execute programmatic budget shifts across five ad networks with strict pre-flight guardrails. Not a dashboard wrapper. Not another SaaS integration. Infrastructure.

Part of the [Paid Media AI Suite](https://github.com/kenlim5656/paid-media-suite).

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                   PAID MEDIA AGENT — CLOSED-LOOP ENGINE                         │
└─────────────────────────────────────────────────────────────────────────────────┘

  ① TELEMETRY           ② FORENSIC AUDIT      ③ BAYESIAN MODELS     ④ OPEN CORE
     INGESTION             (Watchdog)            (Analyst)              ISOLATION
  ─────────────────     ─────────────────     ─────────────────     ─────────────
  Google Ads            Signal capture        BSTS Causal Model     SkillResolver
  TikTok Ads            CRM null spikes       • Incrementality      • Loads private
  Meta Ads              Forensic State        • Counterfactuals       .md playbooks
  LinkedIn              Machine (T37)         • Credible bands        at runtime
  Reddit Ads            • Overwrite traps                           • Never logged
  GA4 Sessions          • systemmodstamp      Meridian MMM          • Zero data
  Salesforce CRM          drift detection     • Posterior ROI         outflow from
  IP Intelligence       • Anomaly scoring     • Budget packages       corporate IP
       │                → watchdog_alerts          │
       └──────────────────────┬───────────────────┘
                              │
                    ┌─────────▼──────────┐        ← Open Core private context
                    │   Analyst Agent    │ ◄─────────────────────────────────────┐
                    │ • Identity graph   │                                        │
                    │ • Shapley / Markov │    private_market_intelligence.md      │
                    │ • Account-based    │    private_account_intent.md           │
                    │   analytics        │    private_meridian_priors.md          │
                    │ • Social signals   │                                        │
                    │ • Creative intel   │                                        │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────────────────────────────────────────────┐
                    │   Operator Agent  ⑤ PROGRAMMATIC EXECUTION                 │
                    │                                                             │
                    │   Pre-flight guardrail sweep  (all-or-nothing gate)         │
                    │   ──────────────────────────────────────────────────        │
                    │   • Schema version + approval flag validated                │
                    │   • Max shift cap: ±10% per channel per run (in code)       │
                    │   • Platform floor minimums enforced before any mutation    │
                    │   • Full error list returned if any check fails             │
                    │                                                             │
                    │   Sequential mutation loop                                  │
                    │   ──────────────────────────────────────────────────        │
                    │   Google Ads → TikTok → Meta → LinkedIn → Reddit            │
                    │   + DV360 / SA360 audience suppression                      │
                    │                                                             │
                    │   → operator_action_log (BigQuery, full audit trail)        │
                    └─────────────────────────────────────────────────────────────┘
```

---

## Core Capabilities

| Capability | Engine | What It Does |
|---|---|---|
| **Data Integrity** | Task 37 Forensic State Machine | Catches Salesforce CRM attribution overwrites by fingerprinting `systemmodstamp` + `lead_source_updated_at` drift. Surfaces hidden pipeline attribution poisoning before it contaminates models. |
| **Causal Measurement** | Task 24 JAX-backed BSTS | Bayesian Structural Time Series counterfactual modeling for true incrementality. Isolates paid media lift from organic trends with credible-interval confidence bands. |
| **Portfolio Allocation** | Task 27 Google Meridian MMM | Bayesian Marketing Mix Model with pre-computed posterior summaries. Adjusted ROI means + confidence tiers (`high` / `medium` / `low`) feed directly into `task27.v1` budget recommendation packages. |
| **Programmatic Write** | Tasks 20 & 21 — All-or-nothing mutation gates | Sequential budget mutations across Google Ads, TikTok, Meta, LinkedIn, and Reddit. Zero mutations applied if any pre-flight guardrail check fails — schema version, approval flag, shift cap, and platform floors all validated before the first API call. |

---

## Security & Privacy Architecture

### The Open Core Isolation Pattern

Corporate strategy and sensitive client playbooks are never committed to version control and never flow through agent inference logs. The `SkillResolver` (`tools/skill_resolver.py`) enforces this boundary programmatically:

```
agents/analyst/skills/
├── private_market_intelligence.md   ← loaded at runtime, never logged
├── private_account_intent.md        ← ICP tiers, suppression lists, intent thresholds
├── private_meridian_priors.md       ← channel-specific MMM prior beliefs
├── private_saturation_rules.md      ← Hill function parameters + CPA tolerance corridors per channel
└── private_decay_pacing.md          ← adstock lambda values, cool-down gates, floor spend, pulse cadence
```

At inference time the resolver reads the relevant `.md` file and injects its contents into the agent's system prompt for that run only. The file contents are held in memory during the inference pass — they are never:

- Written to `operator_action_log` or any BigQuery table
- Included in structured log output (structlog fields are whitelisted)
- Echoed in agent tool responses or Markdown summaries

This decouples proprietary IP from public code commits — the repository is safe to open-source or share while competitive strategy remains private to each deployment.

### Data Governance Constraints

Enforced in code, not just documentation:

| Control | Enforcement |
|---|---|
| `OPERATOR_REQUIRE_APPROVAL=true` default | Write operations raise `ApprovalRequiredError` unless explicitly disabled |
| `MAX_BUDGET_SHIFT_PCT=10` | Pre-flight sweep rejects any recommendation where `abs(shift_pct) > policy` |
| No raw PII storage | Sessions store `/24` IP prefix + resolved `company_domain` only; raw IPs never persisted |
| Reddit handle hashing | `author_handle` stored as SHA-256 hash in `social_mentions_staging`; raw handle never written |
| Financial fields as NUMERIC | All currency columns use BigQuery `NUMERIC` type — never `FLOAT64` |
| CRM data isolation | Raw emails never logged; only domain-level counts exposed externally |
| Credential isolation | `google-ads.yaml`, `tiktok-ads.yaml`, `reddit-ads.yaml` are in `.gitignore` and `.claudeignore`; production credentials pulled from GCP Secret Manager |

---

## The Three Agents

### Watchdog — runs hourly

Monitors data quality across all platform signal namespaces: `gclid`, `fbclid`, `li_fat_id`, `ttclid`, `GA4 client_id`, and IP-resolved company signals. Runs the **Task 37 Forensic State Machine** — a multi-trap anomaly detector that identifies:

- **Trap A — Salesforce overwrite:** CRM leads where `systemmodstamp = lead_source_updated_at = created_at` with offline `lead_source` values on records that originated from paid clicks. The fingerprint of a bulk Salesforce import that silently rewrites digital attribution to "Trade Show" or "Content Syndication."
- **Trap B — Organic surge masking paid lift:** A 5× organic session multiplier with flat paid spend across a 5-day window — creates a clean BSTS counterfactual that exposes false incrementality claims.
- **Trap C — Vertical revenue concentration:** Logistics & Supply Chain closed-won deals accumulating to ≥45% of 30-day revenue — flags model contamination from segment over-indexing before it skews MMM priors.

Writes structured alerts to `watchdog_alerts` and capture rate history to `watchdog_capture_rate_log`. Sends Slack notifications on threshold breaches.

### Analyst — runs daily

Stitches identity signals into canonical entities, then computes multi-touch attribution across three models:

| Model | When | Description |
|---|---|---|
| **Full-Path** | Always (default) | Position-weighted: first/last touch 40% each, middle touches share 20% |
| **Shapley Value** | ≥ 1,000 unique paths | Game-theoretic, data-driven credit allocation across all channel combinations |
| **Markov Chain** | Many-channel programs | Removal-effect transition matrix; fast and channel-count-agnostic |

Also runs:

- **BSTS Causal Analysis** (`causal_analyst_engine.py`) — JAX-backed Bayesian Structural Time Series for incrementality modeling with counterfactual intervals
- **Meridian MMM** (`meridian_analyst_engine.py`, `mmm_optimizer_analyst.py`) — Google Meridian wrapper with posterior summary export and `task27.v1` budget recommendation packages
- **Channel Saturation Analysis** (`saturation_analyst.py`) — Hill function saturation scoring with 85% diminishing-return rule, CPA tolerance corridors per channel, and Operator reallocation vectors
- **Adstock Decay & Flighting** (`adstock_analyst.py`) — geometric adstock series per channel, cool-down gate eligibility, 14-day programmatic flighting schedule with PULSE_ON / COOL_DOWN / HOLD actions and floor enforcement
- **Account-Based Analytics** (`account_analytics_inspector.py`) — IP-resolved company-level attribution; `intent_score` from firmographic signals, session depth, and multi-channel footprint
- **Social Listening** (`social_listening_client.py`) — brand mention signals enriched into attribution context
- **Creative Intelligence** (`creative_insights_client.py`) — asset-level performance signals feeding creative strategy recommendations

Writes to: `attribution_results`, `attribution_channel_summary`, `analyst_insights`, `mmm_runs`, `mmm_channel_contributions`, `data_attribution_anomalies`.

### Operator — runs daily, after Analyst

Reads attribution results and acts. Two primary action classes:

**Budget Reallocation** (`execute_system_budget_reallocation`)
Ingests a `task27.v1` MMM optimization package from the Analyst and executes budget mutations across all five platforms. The **pre-flight** guardrail sweep is all-or-nothing — if any single check fails, zero mutations are applied and the full error list is returned for human review before any retry. The **execution loop** that follows is sequential per channel and is *not* transactional: platform APIs have no cross-platform rollback, so if channel 3 of 5 fails, channels 1–2 stay applied and 4–5 still execute. The action's terminal status in `operator_action_log` records this honestly — `executed` (all channels applied), `partial` (some applied, some failed — per-channel detail in the result payload), or `failed` (nothing applied). A `partial` outcome requires human review: re-running the package is not safe until the failed channels are reconciled.

**Audience Suppression** (`sync_evolving_lookalike_seeds`)
Pushes pipeline account lists to DV360, Meta, and LinkedIn to suppress in-flight accounts from top-of-funnel acquisition spend. Prevents wasted impressions on companies already in active sales cycles.

All actions logged to `operator_action_log`. Human approval required by default — review via `get_pending_approvals` in Claude Code.

---

## Platform Adapters

| Platform | Audience Suppression | Budget Reallocation | Module |
|---|---|---|---|
| **Google Ads** | — | ✓ | `tools/google_ads_operator.py` |
| **TikTok Ads** | — | ✓ | `tools/tiktok_ads_operator.py` |
| **Meta** | ✓ | ✓ | `tools/meta_client.py` |
| **LinkedIn** | ✓ | ✓ | `tools/linkedin_client.py` |
| **Reddit Ads** | — | ✓ | `tools/reddit_ads_client.py` |
| **DV360** | ✓ | ✓ | `tools/gmp_client.py` |
| **SA360** | — | ✓ | `tools/gmp_client.py` |

**Platform budget floors enforced in code:**

| Platform | Daily Floor | Lifetime Floor |
|---|---|---|
| Google Ads | $5.00/day | — |
| TikTok Ads | $20.00/day | $50.00 |
| Meta | $1.00/day | — |
| LinkedIn | $10.00/day | — |
| Reddit Ads | $5.00/day | — |

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/kenlim5656/paid-media-agent.git
cd paid-media-agent
pip install -e .
bash scripts/install-hooks.sh   # pre-commit guard against committing private assets

# 2. Configure credentials
cp .env.example .env
# PAID_MEDIA_GCP_PROJECT is required (legacy GCP_PROJECT_ID still accepted);
# all platform credentials are optional per adapter

# 3. Deploy BigQuery schema (first time only)
bq mk --dataset YOUR_PROJECT_ID:paid_media
bq query --use_legacy_sql=false < schema/bigquery/00_create_all.sql

# 4. Seed the sandbox dataset (90-day B2B synthetic data with forensic anomaly traps)
python tools/generate_sandbox_data.py --days 90

# 5. Run agents locally
python -m orchestrator.runner --agent watchdog
python -m orchestrator.runner --agent analyst

# 6. Review pending Operator actions before any live mutations
# In Claude Code:  get_pending_approvals()

# 7. Deploy to Cloud Run
```

→ [Full deployment guide](./SETUP.md)

---

## Repository Structure

```
paid-media-agent/
├── schema/                         # ← BigQuery DDL and identity contracts (was paid-media-schema)
│   ├── bigquery/                   # 17 SQL files — deploy in numbered order
│   │   ├── 00_create_all.sql       # Convenience: runs all files in sequence
│   │   ├── 01_identity.sql         # Identity graph tables
│   │   ├── 02_touchpoints.sql      # Sessions and touchpoint events
│   │   ├── 03_platform.sql         # Campaigns, ad groups, daily spend
│   │   ├── 04_attribution.sql      # Attribution paths, runs, results
│   │   ├── 05_agent_outputs.sql    # Watchdog alerts, analyst insights, operator log
│   │   ├── 06_reporting.sql        # Pre-built reporting views
│   │   ├── 07_account_analytics.sql # Account-based analytics (IP-resolved)
│   │   ├── 08_mmm.sql              # Meridian MMM runs + channel contributions
│   │   ├── 09_incrementality.sql   # BSTS incrementality tests
│   │   ├── 10_causal_impact.sql    # Causal impact analysis outputs
│   │   ├── 12_social_listening.sql # Social mention staging
│   │   ├── 13_reddit_ads.sql       # Reddit Ads spend + engagement
│   │   ├── 14_audience_mutation.sql # Audience suppression audit log
│   │   ├── 15_market_signals.sql   # Market intent signal staging
│   │   ├── 16_attribution_forensics.sql # Forensic anomaly detection output
│   │   ├── 17_unified_reporting.sql # Cross-channel unified reporting views
│   │   └── migrations/             # Schema migration files
│   ├── namespaces/
│   │   └── identity_namespaces.json  # Registry of 30+ identity signal types
│   └── json-files/
│       └── schema.json             # JSON schema for simple mode (no BigQuery)
├── agents/
│   ├── watchdog/                   # Hourly data integrity monitor + forensic audit
│   │   └── skills/                 # Private watchdog playbooks (Open Core)
│   ├── analyst/                    # Daily attribution + causal modeling
│   │   └── skills/                 # Private market intelligence + MMM priors (Open Core)
│   └── operator/                   # Daily programmatic execution layer
│       └── skills/                 # Private operator playbooks (Open Core)
├── tools/
│   ├── google_ads_operator.py      # Atomic Google Ads budget mutations
│   ├── tiktok_ads_operator.py      # Atomic TikTok budget mutations
│   ├── causal_analyst_engine.py    # JAX BSTS incrementality engine
│   ├── meridian_analyst_engine.py  # Google Meridian MMM wrapper
│   ├── mmm_optimizer_analyst.py    # Posterior → task27.v1 budget package
│   ├── saturation_analyst.py       # Hill function saturation + 85% rule + CPA corridors
│   ├── adstock_analyst.py          # Geometric adstock series + cool-down gates + flighting schedule
│   ├── account_analytics_inspector.py  # Account-based analytics
│   ├── attribution_models.py       # Shapley value + Markov chain attribution
│   ├── skill_resolver.py           # Open Core isolation — private .md loader
│   ├── generate_sandbox_data.py    # 90-day B2B synthetic dataset + anomaly traps
│   ├── bigquery_client.py          # BigQuery write layer (NUMERIC fields enforced)
│   ├── ip_intelligence_client.py   # IP → company resolution (no raw IP storage)
│   └── [platform clients]          # meta, linkedin, tiktok, reddit, gmp, google_ads
├── config/
│   └── settings.py                 # Pydantic BaseSettings — all config from .env
├── deploy/
│   ├── cloud_run/                   # Cloud Run service configurations
│   └── cloud_scheduler/             # Cron job definitions
├── orchestrator/                    # Agent runner and scheduling logic
├── SETUP.md                         # Full deployment guide
└── AGENT.md                         # Unified agent definition and capabilities
```

---

## The Three Repos

| Repo | Role |
|---|---|
| **[paid-media-agent](https://github.com/kenlim5656/paid-media-agent)** ← you are here | Autonomous agents + BigQuery schema DDL + deployment docs — the core |
| **[paid-media-mcp](https://github.com/kenlim5656/paid-media-mcp)** | Interactive data server — connects Claude Code to live campaign data and agent outputs |
| **[skills](https://github.com/kenlim5656/skills)** | Interactive skill library — 16+ paid-media skills for Claude Code |

---

## Schema Alignment

All BigQuery DDL lives in `schema/bigquery/` — 17 SQL files covering every table the agents read and write. The `paid-media-mcp` reads from the same tables — this is the integration point that closes the loop between autonomous agent outputs and interactive Claude Code skill sessions.

| Agent | Tables Written |
|---|---|
| Watchdog | `watchdog_alerts`, `watchdog_capture_rate_log` |
| Analyst | `identity_entities`, `identity_entity_signals`, `attribution_runs`, `attribution_results`, `attribution_channel_summary`, `analyst_insights`, `mmm_runs`, `mmm_channel_contributions`, `data_attribution_anomalies` |
| Operator | `operator_action_log`, `operator_pending_approvals`, `audience_mutation_logs` |
| Generator | `platform_campaigns`, `platform_daily_spend`, `sessions`, `crm_leads_staging`, `crm_opportunities_staging`, `company_profiles`, `company_engagement`, `target_account_activity` |

---

## License

Business Source License 1.1 (BSL 1.1). Persistent attribution required.
See [LICENSE](./LICENSE) and [NOTICE](./NOTICE) for terms.
© 2026 @kenlim5656
