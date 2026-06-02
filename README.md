# Paid Media Agent

Autonomous agents for paid media — Watchdog, Analyst, and Operator — running on Cloud Run.
Part of the [Paid Media AI Suite](https://github.com/arcticgreyy/paid-media-suite).

---

## Part of the Paid Media AI Suite

| Component | Role |
|-----------|------|
| **[paid-media-schema](https://github.com/arcticgreyy/paid-media-schema)** | Shared data contract — BigQuery DDL and identity namespace registry |
| **[paid-media-mcp](https://github.com/arcticgreyy/paid-media-mcp)** | Interactive data server — connects Claude to campaign data and agent outputs |
| **[paid-media-agent](https://github.com/arcticgreyy/paid-media-agent)** ← you are here | Autonomous agents — Watchdog, Analyst, Operator on Cloud Run |
| **[skills](https://github.com/arcticgreyy/skills)** | Interactive skill library — 16 paid-media skills for Claude Code |

→ [Full setup guide](https://github.com/arcticgreyy/paid-media-suite/blob/main/SETUP.md)

---

## The three agents

### Watchdog (runs hourly)
Monitors data quality across all platform signal namespaces — gclid, fbclid, li_fat_id, ttclid, GA4 client_id, and more. Checks for CRM null field spikes. Writes structured alerts to `watchdog_alerts` and capture rate history to `watchdog_capture_rate_log`. Sends Slack notifications on threshold breaches.

### Analyst (runs daily)
Stitches identity signals into canonical entities, then computes multi-touch attribution. Supports three models: Full-Path (default), Shapley Value (data-driven, ≥1K paths), and Markov Chain (fast, many channels). Writes results to `attribution_results` and `attribution_channel_summary`. Surfaces findings to `analyst_insights`.

### Operator (runs daily, after Analyst)
Reads attribution results and acts. Suppresses pipeline accounts from top-of-funnel ads (DV360, Meta, LinkedIn). Reallocates budget from underperforming campaigns to high-attribution channels. All actions logged to `operator_action_log`. Requires human approval by default — review via `get_pending_approvals` in Claude Code.

---

## Platform adapters

| Platform | Audience suppression | Budget reallocation |
|----------|---------------------|---------------------|
| DV360 | ✓ | ✓ |
| SA360 | — | ✓ |
| CM360 | — | — |
| Meta | ✓ | ✓ |
| LinkedIn | ✓ | ✓ |
| TikTok | Planned | Planned |
| Google Ads | Planned | Planned |

---

## Quick start

```bash
# 1. Clone and install
git clone https://github.com/arcticgreyy/paid-media-agent.git
cd paid-media-agent
pip install -e .

# 2. Configure credentials
cp .env.example .env
# Edit .env with your GCP, Salesforce, and platform credentials

# 3. Run the Watchdog locally to test
python -m orchestrator.runner --agent watchdog

# 4. Deploy to Cloud Run (see SETUP.md for full instructions)
```

For full deployment instructions including Cloud Run and Cloud Scheduler setup,
see the [suite setup guide](https://github.com/arcticgreyy/paid-media-suite/blob/main/SETUP.md).

---

## Schema alignment

This agent writes to tables defined in [paid-media-schema](https://github.com/arcticgreyy/paid-media-schema).
The `paid-media-mcp` reads from the same tables — this is the integration point that closes the loop
between autonomous agent outputs and interactive skill sessions.

Key tables written by each agent:
- Watchdog → `watchdog_alerts`, `watchdog_capture_rate_log`
- Analyst → `identity_entities`, `identity_entity_signals`, `attribution_runs`, `attribution_results`, `attribution_channel_summary`, `analyst_insights`
- Operator → `operator_action_log`, `operator_pending_approvals`
