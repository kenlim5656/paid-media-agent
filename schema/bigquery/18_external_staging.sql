-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- EXTERNAL STAGING TABLES — CRM IMPORTS (18_external_staging.sql)
-- =============================================================================
-- These tables are POPULATED BY YOUR ETL, not by the agents. They are the raw
-- CRM exports the suite reads from. This file only creates empty stubs so a
-- fresh deployment doesn't fail at view creation (14_audience_mutation.sql,
-- 16_attribution_forensics.sql, and 17_unified_reporting.sql all reference
-- them) — you still need to load data into them.
--
-- NOTE: `sessions` (the GA4/analytics export) is NOT defined here — it is
-- created by 02_touchpoints.sql, but it likewise must be populated by your
-- GA4 BigQuery export ETL before the unified reporting views return rows.
--
-- Consumers:
--   crm_leads_staging:
--     • 17_unified_reporting.sql  — ga_client_id, account_id (revenue layer join)
--     • 14_audience_mutation.sql  — lead_id, email, created_at
--     • tools/crm_client.py       — email, is_active (Customer Match uploads)
--     • tools/attribution_verifier.py — lead_source, ga_client_id,
--         lead_source_updated_at / systemmodstamp / updated_at / last_modified_at
--     • agents/watchdog/agent.py + paid-media-mcp — gclid, fbclid, li_fat_id,
--         ttclid, ga_client_id, utm_source, created_at (null-field audits)
--   crm_opportunities_staging:
--     • 17_unified_reporting.sql  — account_id, pipeline_stage, amount, is_closed
--     • 14_audience_mutation.sql  — account_id, company_domain, pipeline_stage,
--         is_closed, amount, close_date
--     • agents/operator/agent.py  — account_id, company_domain, pipeline_stage,
--         is_closed (open-pipeline suppression domains)
--
-- Usage:
--   sed 's/{project}/my-project/g; s/{dataset}/paid_media/g' 18_external_staging.sql \
--     | bq query --use_legacy_sql=false
-- =============================================================================


-- =============================================================================
-- TABLE: crm_leads_staging
-- =============================================================================
-- One row per CRM lead/contact (standard Salesforce/HubSpot export).
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.crm_leads_staging`
(
    lead_id                 STRING,     -- CRM lead/contact ID
    account_id              STRING,     -- CRM account ID (joins crm_opportunities_staging)
    email                   STRING,     -- raw email; hashed in-process before any upload
    company_domain          STRING,     -- normalized company domain (or derived from email)

    -- Web linkage — required for the digital→revenue attribution path
    ga_client_id            STRING,     -- GA4 client ID captured on the lead form
    gclid                   STRING,     -- Google Ads click ID
    fbclid                  STRING,     -- Meta click ID
    li_fat_id               STRING,     -- LinkedIn first-party ad tracking ID
    ttclid                  STRING,     -- TikTok click ID
    utm_source              STRING,
    utm_medium              STRING,

    lead_source             STRING,     -- CRM lead source label
    is_active               BOOL,       -- optional; crm_client filters on it when present

    created_at              TIMESTAMP,  -- lead creation time (audits window on this)
    lead_source_updated_at  TIMESTAMP,  -- when lead_source last changed (overwrite forensics)
    systemmodstamp          TIMESTAMP,  -- Salesforce system modification stamp
    updated_at              TIMESTAMP,
    last_modified_at        TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY company_domain
OPTIONS (
    description = 'External CRM lead export — populated by your ETL, read by agents/views. See 18_external_staging.sql header for the column contract.'
);


-- =============================================================================
-- TABLE: crm_opportunities_staging
-- =============================================================================
-- One row per CRM opportunity (standard Salesforce/HubSpot export).
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.crm_opportunities_staging`
(
    opportunity_id  STRING,     -- CRM opportunity ID (optional)
    account_id      STRING,     -- CRM account ID (joins crm_leads_staging.account_id)
    company_domain  STRING,     -- normalized company domain
    industry        STRING,     -- firmographic; used by sandbox + seed cohorts

    pipeline_stage  STRING,     -- e.g. 'mql', 'sql', 'closed won', 'closed lost'
    is_closed       BOOL,       -- TRUE once the opportunity is closed (won or lost)

    amount          NUMERIC,    -- deal value / ARR — canonical money column (views read this)
    deal_value      NUMERIC,    -- legacy alias still written by tools/generate_sandbox_data.py

    created_at      TIMESTAMP,
    close_date      DATE,       -- canonical close date (views read this)
    closed_at       TIMESTAMP   -- legacy alias still written by tools/generate_sandbox_data.py
)
PARTITION BY DATE(created_at)
CLUSTER BY company_domain
OPTIONS (
    description = 'External CRM opportunity export — populated by your ETL, read by agents/views. See 18_external_staging.sql header for the column contract.'
);
