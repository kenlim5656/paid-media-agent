# Setup Guide

This guide walks through setting up the Paid Media AI Suite from scratch.
Choose the path that fits your environment.

---

## Prerequisites

**Required for all modes:**
- [Claude Code](https://claude.ai/claude-code) installed
- An [Anthropic API key](https://console.anthropic.com/)
- Node.js 18+ (for paid-media-mcp)
- Git

**Required for full mode only:**
- A GCP project with BigQuery enabled
- A GCP service account with `BigQuery Data Editor` and `BigQuery Job User` roles
- Python 3.11+ (for paid-media-agent)

**Required for autonomous agents only:**
- Cloud Run enabled in your GCP project
- Cloud Scheduler enabled

---

## Path A — Simple mode (JSON files, no warehouse)

Estimated setup time: **30–60 minutes**

### Step 1: Install the skills

```bash
# Clone the skills repo
git clone https://github.com/arcticgreyy/skills.git

# Copy the paid-media skills to your Claude Code skills directory
cp -r skills/paid-media ~/.claude/skills/
cp -r skills/paid-media-mcp-setup ~/.claude/skills/
```

Verify in Claude Code: type `/paid-media/` — you should see the skill list autocomplete.

### Step 2: Set up paid-media-mcp

```bash
git clone https://github.com/arcticgreyy/paid-media-mcp.git
cd paid-media-mcp
npm install
npm run build
```

### Step 3: Configure Claude Code or Claude Desktop

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "paid-media": {
      "command": "node",
      "args": ["/path/to/paid-media-mcp/dist/index.js"],
      "env": {
        "PAID_MEDIA_DATA_DIR": "/path/to/paid-media-mcp/data"
      }
    }
  }
}
```

**Claude Code** (`.claude/settings.json` in your project):
```json
{
  "mcpServers": {
    "paid-media": {
      "command": "node",
      "args": ["/path/to/paid-media-mcp/dist/index.js"],
      "env": {
        "PAID_MEDIA_DATA_DIR": "/path/to/paid-media-mcp/data"
      }
    }
  }
}
```

Restart Claude Desktop / reload Claude Code after saving.

### Step 4: Populate your data

Run the setup wizard in Claude Code:

```
/paid-media-mcp-setup/setup
```

This walks through all 13 data domains (teams, campaigns, attribution models, measurement
setup, audiences, etc.) one at a time. Accept data from pasted spreadsheet content,
plain conversation answers, or BigQuery if available.

Minimum to set up first (in order):
1. `metadata` — company name, currency, fiscal year
2. `accounts` — your ad platform accounts
3. `teams` — team structure and KPIs
4. `campaigns` — active and recent campaign list

### Step 5: Activate the Paid Media Agent

Copy `AGENT.md` from this repo to your project as `CLAUDE.md`:

```bash
cp /path/to/paid-media-suite/AGENT.md /your/project/CLAUDE.md
```

Open Claude Code in your project directory. You now have the full Paid Media Agent.

### Verification

Test the setup with a few quick checks:
- `list_campaigns` → should return your campaigns from data/campaigns.json
- `get_team` → should return your team structure
- `/paid-media/analyze-performance` → should trigger the performance analysis skill

---

## Path B — Full mode (BigQuery + autonomous agents)

Estimated setup time: **2–4 hours** (most of it is GCP setup and data loading)

### Step 1: GCP project setup

```bash
# Enable required APIs
gcloud services enable bigquery.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com

# Create a service account
gcloud iam service-accounts create paid-media-agent \
  --display-name="Paid Media Agent"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download the key
gcloud iam service-accounts keys create service-account.json \
  --iam-account=paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### Step 2: Deploy the BigQuery schema

```bash
git clone https://github.com/arcticgreyy/paid-media-schema.git
cd paid-media-schema

# Create the dataset
bq mk --dataset YOUR_PROJECT_ID:paid_media

# Deploy all tables (replace placeholders first)
for f in bigquery/0*.sql bigquery/1*.sql; do
  sed "s/{project}/YOUR_PROJECT_ID/g; s/{dataset}/paid_media/g" "$f" \
    | bq query --use_legacy_sql=false
  echo "Deployed $f"
done
```

Verify: open BigQuery console → your project → `paid_media` dataset → you should see 23 tables and 9 views.

**Required external imports — populate before the views return data:**

The schema creates three staging tables as empty stubs, but **your ETL must
populate them** — the agents only read from them:

| Table | Created by | Populated by | Key columns |
|---|---|---|---|
| `sessions` | `02_touchpoints.sql` | GA4 BigQuery export ETL | `session_id`, `ga4_client_id`, `utm_campaign`, `gclid`/`fbclid`/`li_fat_id`/`ttclid`, `session_start_at` |
| `crm_leads_staging` | `18_external_staging.sql` | CRM (Salesforce/HubSpot) export | `lead_id`, `email`, `account_id`, `ga_client_id`, click IDs, `lead_source`, `created_at` |
| `crm_opportunities_staging` | `18_external_staging.sql` | CRM export | `account_id`, `company_domain`, `pipeline_stage`, `is_closed`, `amount`, `close_date` |

See the header of `schema/bigquery/18_external_staging.sql` for the full
column contract and which agent/view reads each column. Until these are
populated, `v_reporting_campaign_roi` (17_unified_reporting.sql) and the
audience-mutation views return no rows, and the Operator's open-pipeline
suppression falls back to the live Salesforce API.

### Step 3: Set up paid-media-mcp (BigQuery mode)

```bash
git clone https://github.com/arcticgreyy/paid-media-mcp.git
cd paid-media-mcp
npm install
npm run build
```

Configure Claude Desktop / Code (same as simple mode, but add BigQuery env vars):

```json
{
  "mcpServers": {
    "paid-media": {
      "command": "node",
      "args": ["/path/to/paid-media-mcp/dist/index.js"],
      "env": {
        "PAID_MEDIA_DATA_DIR": "/path/to/paid-media-mcp/data",
        "PAID_MEDIA_GCP_PROJECT": "YOUR_PROJECT_ID",
        "PAID_MEDIA_BQ_DATASET": "paid_media",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/service-account.json",
        "PAID_MEDIA_SCHEMA_DIR": "/path/to/paid-media-schema",
        "PAID_MEDIA_AGENT_URL": "https://YOUR_CLOUD_RUN_URL"
      }
    }
  }
}
```

### Step 4: Populate MCP data files

Even in BigQuery mode, the MCP reads org knowledge (teams, attribution models,
audiences, measurement setup) from local JSON files. Run the setup wizard:

```
/paid-media-mcp-setup/setup
```

For campaign and performance data: the wizard will ask for your BigQuery table details
and import directly from your existing tables.

### Step 5: Deploy paid-media-agent

```bash
git clone https://github.com/arcticgreyy/paid-media-agent.git
cd paid-media-agent

# Install dependencies
pip install -e .

# Copy and fill in your credentials
cp .env.example .env
# Edit .env with your actual values

# Test locally first
python -m orchestrator.runner --agent watchdog
```

**Deploy to Cloud Run:**

```bash
# Build and push the container
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/paid-media-agent

# Deploy
gcloud run deploy paid-media-agent \
  --image gcr.io/YOUR_PROJECT_ID/paid-media-agent \
  --platform managed \
  --region us-central1 \
  --service-account paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars="PAID_MEDIA_GCP_PROJECT=YOUR_PROJECT_ID,PAID_MEDIA_BQ_DATASET=paid_media" \
  --set-secrets="ANTHROPIC_API_KEY=anthropic-api-key:latest" \
  --no-allow-unauthenticated
```

**Set up Cloud Scheduler (from `deploy/cloud_scheduler/schedules.yaml`):**

```bash
# Get your Cloud Run URL
AGENT_URL=$(gcloud run services describe paid-media-agent \
  --platform managed --region us-central1 \
  --format='value(status.url)')

# Watchdog: every hour
gcloud scheduler jobs create http attribution-watchdog \
  --schedule="0 * * * *" \
  --uri="$AGENT_URL/run?agent=watchdog" \
  --http-method=POST \
  --oidc-service-account-email=paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --location=us-central1

# Analyst: 6am UTC daily
gcloud scheduler jobs create http attribution-analyst \
  --schedule="0 6 * * *" \
  --uri="$AGENT_URL/run?agent=analyst" \
  --http-method=POST \
  --oidc-service-account-email=paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --location=us-central1

# Operator: 8am UTC daily (after analyst)
gcloud scheduler jobs create http attribution-operator \
  --schedule="0 8 * * *" \
  --uri="$AGENT_URL/run?agent=operator" \
  --http-method=POST \
  --oidc-service-account-email=paid-media-agent@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --location=us-central1
```

**HTTP endpoints exposed by the Cloud Run service:**

| Route | Method | Caller | Purpose |
|---|---|---|---|
| `/run?agent=<name>` | POST | Cloud Scheduler / MCP `trigger_agent_run` | Run watchdog, analyst, or operator |
| `/query/account-journey` | POST | MCP `query_account_journey` | Read-only journey lookup for one account domain |
| `/action/audience-suppression` | POST | MCP `push_audience_suppression` | Operator-guarded audience exclusion push |
| `/action/reallocate-budget` | POST | MCP `reallocate_media_budget` | Operator-guarded budget reallocation |
| `/health` | GET | Cloud Run | Liveness probe |

The `/action/*` routes run through the Operator's `log_proposed_action` →
execution-tool path: `OPERATOR_REQUIRE_APPROVAL` queues the action in
`operator_pending_approvals` instead of executing, and the platform clients
enforce `MAX_BUDGET_SHIFT_PCT`. Set `PAID_MEDIA_AGENT_URL` in the MCP config
to the Cloud Run URL to enable them.

> The service is deployed with `--no-allow-unauthenticated`, so callers must
> present a Google-signed identity token. In-handler OIDC verification is
> tracked separately (Phase 2 of the review).

### Step 6: Install skills and activate agent

Same as Simple mode Steps 1 and 5.

---

## Platform credentials

### Google Marketing Platform (GMP)

Required for Operator agent to execute budget and audience actions on DV360/SA360/CM360.

1. Create a service account in the GCP project linked to your GMP account
2. Add the service account as a user in DV360 / CM360 with `Write` access
3. Set `CM360_PROFILE_ID`, `DV360_PARTNER_ID`, `SA360_AGENCY_ID` in `.env`

### Meta Ads

Required for Meta audience suppression and budget management.

1. Create a [System User](https://business.facebook.com) in Meta Business Manager
   → Business Settings → Users → System Users
2. Grant the System User `Advertiser` access to your ad accounts
3. Generate a long-lived access token with `ads_management` + `ads_read` permissions
4. Set `META_APP_ID`, `META_APP_SECRET`, `META_ACCESS_TOKEN`, `META_AD_ACCOUNT_ID` in `.env`

Note: `META_AD_ACCOUNT_ID` must include the `act_` prefix (e.g. `act_123456789`).

### LinkedIn Marketing API

Required for LinkedIn audience suppression and budget management.

1. Create an app at [developer.linkedin.com](https://developer.linkedin.com)
2. Add the `Marketing Developer Platform` product to your app
3. Request scopes: `r_ads`, `rw_ads`, `r_dmp_profile`, `rw_dmp_profile`
4. Complete the OAuth 2.0 authorization code flow to get an access token
5. Set `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, `LINKEDIN_ACCESS_TOKEN`,
   `LINKEDIN_PARTNER_ID` in `.env`

Token expiry: 60 days. You must refresh the token before expiry using the refresh flow.

### Salesforce

Required for Watchdog to check CRM null fields and Operator to get pipeline accounts.
Falls back to BigQuery staging table if not configured.

1. Create a Connected App in Salesforce Setup → App Manager
2. Enable OAuth with `api`, `offline_access` scopes
3. Set `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN` in `.env`

---

## Data source mapping

The suite expects certain data to exist. Here's where it comes from:

| Schema table | Source | How to populate |
|---|---|---|
| `sessions` | GA4 BigQuery export, sGTM logs | GA4 → BigQuery linking, or ETL |
| `touchpoint_events` | CM360 data transfer, platform APIs | ETL from your ad platform exports |
| `conversion_events` | CRM + analytics | ETL from Salesforce + GA4 goals |
| `platform_campaigns` | Platform APIs / manual import | `/paid-media-mcp-setup/import-data` |
| `platform_daily_spend` | Platform API exports | ETL or manual import |
| `crm_leads_staging` | Salesforce / HubSpot | ETL from CRM API |
| `crm_opportunities_staging` | Salesforce / HubSpot | ETL from CRM API |
| `sgtm_request_logs` | Cloud Logging → BigQuery sink | sGTM log routing |

For teams without an existing ETL pipeline: start with the JSON file mode for campaign
and performance data, and use the `/paid-media-mcp-setup/import-data` skill to load
data from pasted CSV exports.

---

## Guardrails and approval gates

By default, all Operator agent write actions (budget changes, audience modifications)
require human approval before executing. This is controlled by:

```bash
OPERATOR_REQUIRE_APPROVAL=true   # default — all writes require approval
```

When `true`: the Operator agent writes proposed actions to `operator_pending_approvals`
in BigQuery and returns a pending-approval payload. Use `get_pending_approvals` in
Claude Code to review and explicitly approve before anything executes.

When you're confident in the agent's judgment (after extended validation), set:

```bash
OPERATOR_REQUIRE_APPROVAL=false  # autonomous execution — use with caution
```

Budget shift guardrail (always enforced regardless of approval setting):
```bash
MAX_BUDGET_SHIFT_PCT=10   # max % of any budget to move in one agent run
```

---

## Verification checklist

After setup, run these checks to confirm everything is connected:

**Skills:**
- [ ] `/paid-media/analyze-performance` triggers without error
- [ ] `/paid-media/paid-social` triggers and asks for platform input
- [ ] `/paid-media-mcp-setup/setup` shows your data state correctly

**MCP (simple mode):**
- [ ] `list_campaigns` returns your campaigns
- [ ] `get_team` returns your team structure
- [ ] `list_identity_namespaces` returns the namespace registry

**MCP (BigQuery mode, additional):**
- [ ] `get_attribution_results` attempts a BigQuery query (may return empty if no runs yet)
- [ ] `get_watchdog_alerts` returns empty list (no alerts yet)
- [ ] `query_account_journey` returns a BigQuery error or empty result (confirms BQ connection)

**Autonomous agents (if deployed):**
- [ ] `python -m orchestrator.runner --agent watchdog` completes without error
- [ ] After first Watchdog run: `get_watchdog_alerts` returns capture rate data
- [ ] After first Analyst run: `get_attribution_results` returns channel summary

---

## Troubleshooting

**"MCP server not connected" in Claude Code**
→ Check that the MCP server path in settings.json is correct and the server is built
(`npm run build` in paid-media-mcp directory).

**"list_campaigns returns empty"**
→ Check that `PAID_MEDIA_DATA_DIR` points to a directory containing `campaigns.json`.
Run `/paid-media-mcp-setup/setup` to populate it.

**"BigQuery auth error"**
→ Verify `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account JSON file.
Test with: `bq ls YOUR_PROJECT_ID:paid_media`

**"Watchdog alerts not appearing in get_watchdog_alerts"**
→ The Watchdog has not run yet, or the `watchdog_alerts` table doesn't exist.
Run `python -m orchestrator.runner --agent watchdog` manually first.

**"Operator action not executing"**
→ Check `OPERATOR_REQUIRE_APPROVAL=true` — if true, actions require explicit approval
via `get_pending_approvals`. Set to `false` to enable autonomous execution.

**"Meta API error: Invalid OAuth access token"**
→ System user tokens expire. Generate a new long-lived token in Meta Business Manager.
