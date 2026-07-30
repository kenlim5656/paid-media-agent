-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — AUDIENCE MUTATION LAYER (Task 35)
-- =============================================================================
-- Closed-loop lookalike audience mutation: detects ICP drift in closed-won CRM
-- revenue and provides the hashed email seed list used to retrain ad platform
-- lookalike models on Meta, Google Ads, TikTok, and Reddit Ads.
--
-- Tables in this file:
--   audience_mutation_logs        — per-platform push ledger: one row per run × platform
--
-- Views in this file:
--   v_lookalike_mutation_seed     — rolling 60-day cohort: closed-won top-25% ARR accounts
--                                   with firmographic over-index scores and seed emails
--
-- Upstream dependencies:
--   crm_opportunities_staging     — source: closed-won opportunity records (CRM export)
--   crm_leads_staging             — source: contact/lead records (CRM export)
--   company_profiles              — 07_account_analytics.sql — firmographic enrichment
--
-- Expected columns in crm_opportunities_staging (standard Salesforce export):
--   account_id STRING, company_domain STRING, pipeline_stage STRING,
--   is_closed BOOL, amount NUMERIC (deal value / ARR), close_date DATE
--
-- Expected columns in crm_leads_staging (standard Salesforce export):
--   lead_id STRING, email STRING, created_at TIMESTAMP
--   (company_domain derived from SPLIT(email,'@')[SAFE_OFFSET(1)])
--
-- Privacy constraints (non-negotiable):
--   • v_lookalike_mutation_seed outputs raw emails — these are already present in
--     crm_leads_staging; the view merely reshapes them for the mutation engine.
--   • The mutation engine (tools/audience_mutation_engine.py) hashes all emails
--     to SHA-256 immediately after reading the view. Raw emails are never stored
--     in audience_mutation_logs or any other table.
--   • audience_mutation_logs stores only seed_count_after (integer) and
--     firmographic aggregates — zero individual-level PII is persisted.
--
-- Usage:
--   bq query --use_legacy_sql=false < 14_audience_mutation.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: audience_mutation_logs
-- =============================================================================
-- Execution ledger for lookalike seed mutation runs.
-- One row per platform × run (a single run that pushes to 4 platforms = 4 rows,
-- all sharing the same run_id). Consistent with the run-registry pattern used
-- in attribution_runs, mmm_runs, causal_impact_runs, and reddit_ads_runs.
--
-- status values:
--   "completed"        — upload to the platform audience succeeded
--   "partial"          — upload succeeded but < expected seed count was pushed
--   "failed"           — upload call returned an error; see error_message
--   "pending_approval" — OPERATOR_REQUIRE_APPROVAL gated the push
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.audience_mutation_logs`
(
    mutation_id                  STRING    NOT NULL,  -- UUID (primary key for this row)
    run_id                       STRING    NOT NULL,  -- UUID shared across all platforms in one run

    -- ── Target platform ───────────────────────────────────────────────────────
    platform                     STRING    NOT NULL,
    -- "meta" | "google_ads" | "tiktok" | "reddit_ads"

    audience_id                  STRING    NOT NULL,
    -- Platform-specific audience / list identifier:
    --   Meta:        Custom Audience ID (numeric string)
    --   Google Ads:  UserList resource_name (customers/{id}/userLists/{id})
    --   TikTok:      DMP Custom Audience ID (numeric string)
    --   Reddit Ads:  Audience ID returned by POST /api/v3/audiences

    advertiser_id                STRING,
    -- Platform-specific advertiser / account ID:
    --   Meta:        not required (reads from settings.meta_ad_account_id)
    --   Google Ads:  customer_id (digits only, no dashes)
    --   TikTok:      advertiser_id (numeric string)
    --   Reddit Ads:  ad account ID (t2_xxx or a2_xxx)

    -- ── Seed cohort metrics ───────────────────────────────────────────────────
    seed_count_before            INT64,
    -- Prior known seed count in the audience (NULL on first run or if platform
    -- does not expose an API to retrieve current audience size).

    seed_count_after             INT64,
    -- Number of SHA-256 hashed emails uploaded in this mutation run.
    -- Represents unique deduped email addresses from v_lookalike_mutation_seed.

    domains_in_seed              INT64,
    -- Number of unique company domains that contributed contacts to this run's seed.

    arr_threshold_usd            NUMERIC,
    -- The 75th-percentile ARR value used to gate the high-value cohort.
    -- Stored here so trend analysis can track ICP revenue threshold drift over time.

    -- ── Firmographic shift analysis ────────────────────────────────────────────
    dominant_firmographic_shift  STRING,
    -- Single highest over-indexing trait label, e.g. "Logistics Automation +34.2%"
    -- Derived from the top industry / employee_range / region over-index score.

    top_shifts_json              JSON,
    -- Full firmographic over-index report as JSON:
    -- {
    --   "top_industries":      [{"trait": "...", "over_index_pct": 34.2}, ...],
    --   "top_employee_ranges": [{"trait": "...", "over_index_pct": 18.5}, ...],
    --   "top_regions":         [{"trait": "...", "over_index_pct": 12.1}, ...],
    --   "dominant_shift":      "Logistics Automation (industry) +34.2%"
    -- }

    -- ── Execution metadata ────────────────────────────────────────────────────
    status                       STRING    NOT NULL,
    error_message                STRING,

    created_by                   STRING,
    created_at                   TIMESTAMP NOT NULL,
)
PARTITION BY DATE(created_at)
CLUSTER BY platform, status
OPTIONS (
    description = "Per-platform push ledger for lookalike audience mutation runs. "
                  "Written by tools/audience_mutation_engine.py. "
                  "One row per platform per run_id. Zero raw PII — all personal "
                  "data is hashed before upload and never persisted here."
);


-- =============================================================================
-- VIEW: v_lookalike_mutation_seed
-- =============================================================================
-- Rolling 60-day analytical cohort for lookalike seed mutation.
--
-- What this view computes (three analytical layers):
--
-- Layer 1 — High-value seed domains:
--   • Aggregates closed-won opportunities from the last 60 days.
--   • Computes the 75th-percentile ARR threshold via PERCENTILE_CONT analytic function.
--   • Qualifies any domain whose total_arr meets or exceeds p75_arr, OR that has
--     ≥ 2 closed-won deals (repeat-buyer signal even if sub-threshold ARR).
--
-- Layer 2 — Firmographic over-index scores:
--   • Compares trait distribution in the seed pool vs all leads in the same window.
--   • over_index_pct = (seed_trait_share / lead_trait_share − 1) × 100
--   • A score of +50 means that trait is 1.5× over-represented in closed-won accounts
--     relative to where all top-of-funnel leads are coming from.
--   • Computed independently for: industry, employee_range, headquarters_region.
--
-- Layer 3 — Seed email payload:
--   • Pulls all known email addresses for seed domains from crm_leads_staging.
--   • Deduplicates by normalized email (lowercase) via QUALIFY.
--   • Outputs RAW email strings — callers MUST hash via SHA-256 before upload.
--
-- Output grain: one row per unique email address in the high-value seed cohort.
-- =============================================================================
CREATE OR REPLACE VIEW `{project}.{dataset}.v_lookalike_mutation_seed` AS

WITH

-- ─── Layer 1a: Closed-won deals in rolling 60-day window ─────────────────────
-- Aggregate by domain. 'amount' = ARR / deal value (standard Salesforce export).
closed_won_60d AS (
    SELECT
        LOWER(TRIM(company_domain))                          AS company_domain,
        SUM(COALESCE(SAFE_CAST(amount AS NUMERIC), 0))       AS total_arr,
        COUNT(*)                                             AS deal_count,
        MAX(close_date)                                      AS latest_close_date
    FROM `{project}.{dataset}.crm_opportunities_staging`
    WHERE is_closed = TRUE
      AND LOWER(TRIM(pipeline_stage)) = 'closed won'
      AND close_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
      AND company_domain IS NOT NULL
      AND TRIM(company_domain) != ''
    GROUP BY 1
),

-- ─── Layer 1b: Add 75th-percentile ARR threshold via analytic window ─────────
-- PERCENTILE_CONT over the full closed_won_60d set — every row gets the same p75.
arr_stats AS (
    SELECT
        company_domain,
        total_arr,
        deal_count,
        latest_close_date,
        PERCENTILE_CONT(total_arr, 0.75) OVER ()             AS p75_arr
    FROM closed_won_60d
),

-- ─── Layer 1c: High-value seed domain pool ────────────────────────────────────
-- Top-25% ARR tier OR repeat buyer (≥ 2 deals in window) to ensure a meaningful
-- seed pool even when most accounts share similar ARR values.
high_value_domains AS (
    SELECT
        company_domain,
        total_arr,
        deal_count,
        latest_close_date,
        p75_arr
    FROM arr_stats
    WHERE total_arr >= p75_arr
       OR deal_count >= 2
),

-- ─── Layer 1d: Firmographic enrichment from company_profiles ──────────────────
-- company_profiles is populated by IP intelligence enrichment (07_account_analytics.sql).
-- LEFT JOIN — seed domains without a profile still appear; firmographic fields are 'Unknown'.
seed_with_firmographics AS (
    SELECT
        hvd.company_domain,
        hvd.total_arr,
        hvd.deal_count,
        hvd.latest_close_date,
        hvd.p75_arr,
        COALESCE(cp.industry,              'Unknown')          AS industry,
        COALESCE(cp.employee_range,        'Unknown')          AS employee_range,
        COALESCE(cp.headquarters_country,  'Unknown')          AS headquarters_country,
        COALESCE(cp.headquarters_region,   'Unknown')          AS headquarters_region,
        cp.icp_score
    FROM high_value_domains hvd
    LEFT JOIN `{project}.{dataset}.company_profiles` cp
           ON LOWER(TRIM(hvd.company_domain)) = LOWER(TRIM(cp.company_domain))
),

-- ─── Layer 2a: All leads in 60-day window (over-index denominator) ────────────
-- Derive company domain from email; exclude freemail providers.
all_leads_60d AS (
    SELECT
        LOWER(TRIM(SPLIT(email, '@')[SAFE_OFFSET(1)]))        AS domain,
        LOWER(TRIM(email))                                    AS normalized_email
    FROM `{project}.{dataset}.crm_leads_staging`
    WHERE email IS NOT NULL
      AND ARRAY_LENGTH(SPLIT(email, '@')) = 2
      AND LENGTH(TRIM(SPLIT(email, '@')[SAFE_OFFSET(1)])) > 3
      AND LOWER(TRIM(SPLIT(email, '@')[SAFE_OFFSET(1)])) NOT IN (
          'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com',
          'icloud.com', 'me.com', 'protonmail.com', 'mail.com'
      )
      AND CAST(created_at AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

all_leads_domain_agg AS (
    SELECT domain, COUNT(*) AS lead_count
    FROM all_leads_60d
    GROUP BY domain
),

total_leads AS (
    SELECT COUNT(*) AS n FROM all_leads_60d
),

total_seed_domains AS (
    SELECT COUNT(*) AS n FROM high_value_domains
),

-- ─── Layer 2b: Industry over-index ────────────────────────────────────────────
-- seed_industry_share  = fraction of seed domains with industry X
-- lead_industry_share  = fraction of all leads from domains with industry X
-- over_index_pct       = (seed_share / lead_share − 1) × 100

seed_industry_agg AS (
    SELECT industry, COUNT(*) AS seed_domain_count
    FROM seed_with_firmographics
    GROUP BY industry
),

lead_industry_agg AS (
    SELECT
        COALESCE(cp.industry, 'Unknown')                      AS industry,
        COUNT(*)                                              AS lead_count
    FROM all_leads_domain_agg ala
    LEFT JOIN `{project}.{dataset}.company_profiles` cp
           ON ala.domain = LOWER(TRIM(cp.company_domain))
    GROUP BY 1
),

industry_over_index AS (
    SELECT
        sia.industry,
        sia.seed_domain_count,
        COALESCE(lia.lead_count, 0)                           AS lead_count,
        ROUND(
            SAFE_DIVIDE(
                SAFE_DIVIDE(sia.seed_domain_count, tsd.n),
                NULLIF(SAFE_DIVIDE(COALESCE(lia.lead_count, 0), tl.n), 0)
            ) - 1.0,
            4
        ) * 100                                               AS industry_over_index_pct
    FROM seed_industry_agg sia
    CROSS JOIN total_seed_domains tsd
    CROSS JOIN total_leads tl
    LEFT JOIN lead_industry_agg lia USING (industry)
),

-- ─── Layer 2c: Employee-range over-index ──────────────────────────────────────
seed_emp_agg AS (
    SELECT employee_range, COUNT(*) AS seed_domain_count
    FROM seed_with_firmographics
    GROUP BY employee_range
),

lead_emp_agg AS (
    SELECT
        COALESCE(cp.employee_range, 'Unknown')                AS employee_range,
        COUNT(*)                                              AS lead_count
    FROM all_leads_domain_agg ala
    LEFT JOIN `{project}.{dataset}.company_profiles` cp
           ON ala.domain = LOWER(TRIM(cp.company_domain))
    GROUP BY 1
),

emp_over_index AS (
    SELECT
        sea.employee_range,
        ROUND(
            SAFE_DIVIDE(
                SAFE_DIVIDE(sea.seed_domain_count, tsd.n),
                NULLIF(SAFE_DIVIDE(COALESCE(lea.lead_count, 0), tl.n), 0)
            ) - 1.0,
            4
        ) * 100                                               AS emp_over_index_pct
    FROM seed_emp_agg sea
    CROSS JOIN total_seed_domains tsd
    CROSS JOIN total_leads tl
    LEFT JOIN lead_emp_agg lea USING (employee_range)
),

-- ─── Layer 2d: Region over-index ──────────────────────────────────────────────
seed_region_agg AS (
    SELECT headquarters_region, COUNT(*) AS seed_domain_count
    FROM seed_with_firmographics
    GROUP BY headquarters_region
),

lead_region_agg AS (
    SELECT
        COALESCE(cp.headquarters_region, 'Unknown')           AS headquarters_region,
        COUNT(*)                                              AS lead_count
    FROM all_leads_domain_agg ala
    LEFT JOIN `{project}.{dataset}.company_profiles` cp
           ON ala.domain = LOWER(TRIM(cp.company_domain))
    GROUP BY 1
),

region_over_index AS (
    SELECT
        sra.headquarters_region,
        ROUND(
            SAFE_DIVIDE(
                SAFE_DIVIDE(sra.seed_domain_count, tsd.n),
                NULLIF(SAFE_DIVIDE(COALESCE(lra.lead_count, 0), tl.n), 0)
            ) - 1.0,
            4
        ) * 100                                               AS region_over_index_pct
    FROM seed_region_agg sra
    CROSS JOIN total_seed_domains tsd
    CROSS JOIN total_leads tl
    LEFT JOIN lead_region_agg lra USING (headquarters_region)
),

-- ─── Layer 3: Seed email payload ──────────────────────────────────────────────
-- Pull all known emails for seed domains from crm_leads_staging.
-- Deduplicates by normalized email address (QUALIFY enforces one row per email).
-- IMPORTANT: outputs RAW email strings. Caller must SHA-256 hash before upload.
seed_emails AS (
    SELECT
        swf.company_domain,
        LOWER(TRIM(l.email))                                  AS email,   -- RAW — hash before upload
        swf.industry,
        swf.employee_range,
        swf.headquarters_country,
        swf.headquarters_region,
        swf.icp_score,
        swf.total_arr,
        swf.deal_count,
        swf.latest_close_date,
        swf.p75_arr,
        ioi.industry_over_index_pct,
        eoi.emp_over_index_pct,
        roi.region_over_index_pct,
        -- Human-readable ARR tier label for the Markdown report
        CASE
            WHEN swf.total_arr >= 100000 THEN 'Enterprise ($100k+ ARR)'
            WHEN swf.total_arr >= 50000  THEN 'Upper Mid-Market ($50k–$100k ARR)'
            WHEN swf.total_arr >= 10000  THEN 'Mid-Market ($10k–$50k ARR)'
            ELSE                              'SMB (sub-$10k ARR)'
        END                                                   AS arr_tier_label,
        -- Composite cohort label for downstream reporting
        CONCAT(
            'Closed-Won 60d | ',
            CASE
                WHEN swf.total_arr >= 100000 THEN 'Enterprise ($100k+)'
                WHEN swf.total_arr >= 50000  THEN 'Upper Mid-Market ($50k–$100k)'
                WHEN swf.total_arr >= 10000  THEN 'Mid-Market ($10k–$50k)'
                ELSE                              'SMB (sub-$10k)'
            END,
            ' | ', swf.industry
        )                                                     AS cohort_label,
        CURRENT_TIMESTAMP()                                   AS view_evaluated_at
    FROM seed_with_firmographics swf
    JOIN `{project}.{dataset}.crm_leads_staging` l
      ON LOWER(TRIM(SPLIT(l.email, '@')[SAFE_OFFSET(1)])) = LOWER(swf.company_domain)
    LEFT JOIN industry_over_index ioi USING (industry)
    LEFT JOIN emp_over_index      eoi USING (employee_range)
    LEFT JOIN region_over_index   roi USING (headquarters_region)
    WHERE l.email IS NOT NULL
      AND ARRAY_LENGTH(SPLIT(l.email, '@')) = 2
      AND TRIM(l.email) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(l.email))
        ORDER BY swf.total_arr DESC
    ) = 1
)

-- ─── Final output ─────────────────────────────────────────────────────────────
-- One row per unique email in the high-value seed cohort, with firmographic
-- over-index metadata attached for reporting in the mutation Markdown summary.
SELECT
    company_domain,
    email,                          -- RAW string — SHA-256 hash immediately on read
    industry,
    employee_range,
    headquarters_country,
    headquarters_region,
    icp_score,
    total_arr,
    deal_count,
    latest_close_date,
    p75_arr                         AS arr_p75_threshold,
    arr_tier_label,
    cohort_label,
    industry_over_index_pct,        -- % over-representation vs all leads; NULL if no firmographic data
    emp_over_index_pct,
    region_over_index_pct,
    view_evaluated_at
FROM seed_emails;
