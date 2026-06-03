-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — MARKET SIGNALS LAYER (Task 36)
-- =============================================================================
-- Competitive intelligence ingestion pipeline for market and competitor
-- monitoring. Captures raw scraped text from competitor web properties,
-- press releases, and text dumps, then stores AI-inferred strategic profiles
-- (positioning angles, value propositions, keyword arrays).
--
-- Tables in this file:
--   market_signals_runs           — run registry: one row per ingestion run
--   market_signals_staging        — raw normalized text blocks per URL
--   competitor_messaging_vectors  — AI-inferred strategic messaging profiles
--
-- Relationship to other schema layers:
--   Feeds Task 26 (copy assistant):
--     competitor_messaging_vectors.primary_keywords_detected
--     → injected as counter-positioning context in generate_creative_campaign_brief()
--     via tools/market_signals_client.get_competitor_context()
--
--   Feeds Task 35 (audience mutation):
--     competitor_messaging_vectors.inferred_target_audience
--     → can inform ICP definition updates
--
-- Privacy / compliance:
--   • Only publicly accessible pages are ingested (landing pages, press releases,
--     published blog posts). No authentication-gated or paywalled content.
--   • Raw scraped payloads in market_signals_staging.raw_text_payload are limited
--     to 50,000 characters and may be purged after inference is complete.
--   • No personally identifiable information (PII) is targeted or stored here;
--     the ingestion pipeline focuses on marketing copy, not individual user data.
--   • Private evaluation instructions (private_market_intelligence.md) are
--     never persisted to BigQuery — they exist only in memory during inference.
--
-- Usage:
--   bq query --use_legacy_sql=false < 15_market_signals.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: market_signals_runs
-- =============================================================================
-- Run registry — one row per ingestion session. Follows the established
-- run-registry pattern (attribution_runs, mmm_runs, reddit_ads_runs, etc.).
--
-- source_type values:
--   "url_batch"     — array of HTTP URLs scraped in this run
--   "text_dump"     — raw text passed directly (no HTTP fetch)
--   "press_release" — structured press release ingestion
--
-- status values:
--   "completed" — all URLs fetched and inference written successfully
--   "partial"   — at least one URL succeeded; at least one failed
--   "failed"    — all URLs failed or inference call errored
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.market_signals_runs`
(
    run_id                    STRING    NOT NULL,  -- UUID

    -- ── Scope ─────────────────────────────────────────────────────────────────
    source_type               STRING    NOT NULL,
    -- "url_batch" | "text_dump" | "press_release"

    competitor_name           STRING    NOT NULL,
    -- Canonical label for the competitor being analyzed, e.g. "Acme Corp".
    -- Used as the partition key when querying competitor_messaging_vectors.

    target_competitor_domain  STRING,
    -- Primary domain of the competitor being analyzed, e.g. "acme.com".
    -- Derived from source_urls when not explicitly provided.

    source_urls               JSON,
    -- Array of URL strings submitted to this run.
    -- Stored for reproducibility — allows re-running against the same URL set.

    category                  STRING,
    -- Optional product/market category flag, e.g. "B2B SaaS", "ecommerce",
    -- "marketing automation". Used to scope inference prompt context.

    evaluation_prompt_source  STRING,
    -- "private"         — skills/private_market_intelligence.md was found and used
    -- "public_fallback" — private file not found; generic prompt used

    -- ── Outputs ───────────────────────────────────────────────────────────────
    urls_submitted            INT64,
    urls_fetched_ok           INT64,
    urls_failed               INT64,
    signals_written           INT64,    -- rows written to market_signals_staging
    vectors_written           INT64,    -- rows written to competitor_messaging_vectors

    -- ── Execution metadata ────────────────────────────────────────────────────
    status                    STRING    NOT NULL,
    error_message             STRING,
    created_by                STRING    DEFAULT 'analyst_agent',
    created_at                TIMESTAMP NOT NULL,
)
PARTITION BY DATE(created_at)
CLUSTER BY competitor_name, status
OPTIONS (
    description = "Run registry for market signals ingestion. "
                  "Written by tools/market_signals_client.py MarketSignalsClient. "
                  "One row per ingestion session — links to market_signals_staging "
                  "and competitor_messaging_vectors via run_id."
);


-- =============================================================================
-- TABLE: market_signals_staging
-- =============================================================================
-- Raw normalized text blocks extracted from each source URL. Analogous to
-- social_mentions_staging (Task 25) but for competitor web properties.
--
-- raw_text_payload stores the full normalized text (post-normalize_text() pipeline)
-- capped at 50,000 characters to prevent runaway BigQuery storage costs.
--
-- These rows are the direct input to the inference step; after
-- competitor_messaging_vectors is written, the raw_text_payload may be
-- truncated or purged (set status = 'archived') per data retention policy.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.market_signals_staging`
(
    signal_id                 STRING    NOT NULL,  -- UUID
    run_id                    STRING    NOT NULL,  -- FK → market_signals_runs.run_id

    -- ── Source metadata ───────────────────────────────────────────────────────
    source_url                STRING    NOT NULL,
    source_type               STRING,              -- "landing_page" | "blog_post" | "press_release" | "text_dump"
    http_status_code          INT64,               -- 200, 301, 403, etc. (NULL for text_dump)
    content_type              STRING,              -- MIME type returned by server, e.g. "text/html"

    -- ── Text payload ──────────────────────────────────────────────────────────
    raw_text_payload          STRING,
    -- Normalized text output from normalize_text() — HTML stripped, tracking pixels
    -- removed, emojis stripped, whitespace collapsed. Max 50,000 characters.

    estimated_word_count      INT64,
    text_language_hint        STRING,              -- ISO 639-1 if detected, else NULL

    -- ── Derived analytics (computed during ingestion) ─────────────────────────
    top_keywords_json         JSON,
    -- Array of {keyword: str, count: int} for the top 20 most frequent content
    -- words (stop words excluded). Computed in Python before BQ write.

    -- ── Status ────────────────────────────────────────────────────────────────
    status                    STRING    DEFAULT 'raw',
    -- "raw"       — freshly ingested, awaiting inference
    -- "inferred"  — competitor_messaging_vectors row written from this signal
    -- "failed"    — fetch or normalize failed; see error_message
    -- "archived"  — raw_text_payload may have been truncated per retention policy

    error_message             STRING,
    capture_date              DATE      NOT NULL,
    capture_timestamp         TIMESTAMP NOT NULL,
)
PARTITION BY capture_date
CLUSTER BY run_id, source_type, status
OPTIONS (
    description = "Raw normalized text blocks from competitor web properties. "
                  "Written by tools/market_signals_client.py. "
                  "Input to the Claude inference step that produces "
                  "competitor_messaging_vectors. raw_text_payload capped at 50k chars."
);


-- =============================================================================
-- TABLE: competitor_messaging_vectors
-- =============================================================================
-- AI-inferred strategic messaging profiles extracted from competitor content.
-- One row per run (summarising all URLs in that run for the competitor).
-- Consumed by the Task 26 Copy Assistant for counter-positioning context.
--
-- All fields are either scalars (for BQ analytics / filtering) or JSON
-- (for rich structured data returned to the LLM copy generation prompt).
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.competitor_messaging_vectors`
(
    vector_id                 STRING    NOT NULL,  -- UUID
    run_id                    STRING    NOT NULL,  -- FK → market_signals_runs.run_id
    signal_ids                JSON,
    -- Array of signal_id strings from market_signals_staging that were used
    -- as input to this vector. Allows tracing a vector back to source URLs.

    -- ── Competitor identity ────────────────────────────────────────────────────
    competitor_name           STRING    NOT NULL,
    competitor_domain         STRING,
    category                  STRING,

    -- ── Core inferred profile (scalar — filterable in BQ) ─────────────────────
    core_value_prop           STRING,
    -- Primary value proposition extracted, in one sentence.
    -- Example: "Real-time intent data platform for B2B revenue teams."

    observed_positioning_angle STRING,
    -- The strategic positioning frame detected. Typical values:
    --   "price_leadership"       — "most affordable", "free tier", "save X%"
    --   "quality_premium"        — "enterprise-grade", "gold standard"
    --   "speed_efficiency"       — "fastest", "real-time", "instant"
    --   "ease_simplicity"        — "no-code", "5-minute setup", "plug-and-play"
    --   "category_creation"      — "the only platform that...", "first ever"
    --   "vertical_specialist"    — "built for [industry]"
    --   "trust_security"         — "SOC2", "GDPR-compliant", "bank-grade"
    --   "mixed"                  — multiple angles of roughly equal weight

    inferred_target_audience  STRING,
    -- Primary buyer persona inferred from the content, e.g.
    -- "VP of Marketing at B2B SaaS companies (201–1000 employees)".

    sentiment_tone            STRING,
    -- Overall tone of the competitor's messaging:
    -- "authoritative" | "conversational" | "aggressive" | "aspirational" |
    -- "technical" | "friendly" | "neutral"

    -- ── Keyword and theme arrays (JSON — for copy assistant context injection) ─
    primary_keywords_detected JSON,
    -- Array of high-frequency strategic terms from the content.
    -- Example: ["intent data", "pipeline velocity", "revenue intelligence", "GTM"]

    key_themes_json           JSON,
    -- Array of {theme: str, frequency: int, example_phrase: str} objects.
    -- Top 5 recurring narrative themes detected across all source content.

    messaging_pillars_json    JSON,
    -- Array of {pillar: str, supporting_claims: [str, ...]} objects.
    -- The top 3 messaging pillars (the repeated value claims) the competitor uses.

    cta_patterns_json         JSON,
    -- Array of detected call-to-action phrase patterns, e.g.
    -- ["Start free trial", "Book a demo", "See pricing"].

    counter_positioning_hooks_json JSON,
    -- AI-generated counter-positioning opportunities (not competitor data —
    -- this is the inference layer's output on how to differentiate).
    -- Array of {angle: str, hook: str, rationale: str}.

    -- ── Source stats ──────────────────────────────────────────────────────────
    source_url_count          INT64,
    total_word_count          INT64,
    evaluation_prompt_source  STRING,              -- "private" | "public_fallback"

    -- ── Raw inference output ──────────────────────────────────────────────────
    raw_inference_json        JSON,
    -- Full structured JSON returned by the Claude inference call.
    -- Preserves all fields from the model response for future re-parsing.

    capture_timestamp         TIMESTAMP NOT NULL,
)
PARTITION BY DATE(capture_timestamp)
CLUSTER BY competitor_name, observed_positioning_angle
OPTIONS (
    description = "AI-inferred strategic messaging profiles per competitor. "
                  "Written by tools/market_signals_client.py MarketSignalsClient. "
                  "Consumed by Task 26 Copy Assistant (generate_creative_campaign_brief) "
                  "via market_signals_client.get_competitor_context() for counter-positioning."
);
