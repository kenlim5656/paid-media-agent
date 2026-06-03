-- Copyright 2026 @arcticgreyy. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — SOCIAL LISTENING LAYER (Task 25)
-- =============================================================================
-- Landing tables for external trend signals and social mention data ingested
-- by tools/social_listening_client.py.
--
-- Tables in this file:
--   social_listening_runs      — run registry: one row per ingestion run
--   social_trend_signals       — normalised 0–100 interest-over-time weights
--                                (Google Trends, Reddit search velocity)
--   social_mentions_staging    — raw + normalised text from forum/news sources
--
-- Data sources:
--   Google Trends              — relative search interest via authenticated
--                                marketing data provider (DataForSEO or direct HTTP)
--   Reddit (PRAW)              — posts across targeted business subreddits,
--                                captured via official OAuth API (praw>=7.8.0)
--
-- Privacy notes:
--   • No author handles or user IDs are stored in raw form.
--     author_handle stores a one-way SHA-256 hash of the Reddit author name
--     (for deduplication only — never re-identifiable from schema alone).
--   • payload_text is a public post body; it must never contain inferred PII.
--   • IP addresses and device fingerprints are never ingested.
--
-- Relationship to other layers:
--   Feeds: analyst_insights (05_agent_outputs.sql) — market momentum summaries
--          platform_daily_spend (03_platform.sql) — spend correlated with trends
--          attribution_results  (04_attribution.sql) — conversion correlation
--
-- MoM velocity calculation (in tools/social_listening_client.py):
--   current_avg  = AVG(signal_weight_score) WHERE period_label = 'current'
--   prior_avg    = AVG(signal_weight_score) WHERE period_label = 'prior'
--   velocity_pct = (current_avg - prior_avg) / NULLIF(prior_avg, 0) * 100
--
-- Usage:
--   bq query --use_legacy_sql=false < 12_social_listening.sql
--   (Replace {project} and {dataset} before running)
-- =============================================================================


-- =============================================================================
-- TABLE: social_listening_runs
-- =============================================================================
-- One row per ingestion run. Tracks what was fetched, when, and whether it
-- succeeded. Consistent with attribution_runs / mmm_runs / causal_impact_runs.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.social_listening_runs`
(
    run_id               STRING    NOT NULL,   -- UUID

    -- ── Sources requested ──────────────────────────────────────────────────────
    sources_requested    JSON,
    -- Array of source strings included in this run.
    -- Example: ["google_trends", "reddit"]

    keywords             JSON,                 -- array of target keyword strings
    geo_codes            JSON,                 -- array of ISO country/region codes
    subreddits           JSON,                 -- array of subreddit names (may be null)
    lookback_days        INT64,                -- window length for "current" period

    -- ── Ingestion results ─────────────────────────────────────────────────────
    signals_written      INT64,                -- rows written to social_trend_signals
    mentions_written     INT64,                -- rows written to social_mentions_staging

    -- ── Status ────────────────────────────────────────────────────────────────
    status               STRING    NOT NULL,
    -- "completed"          — all requested sources returned data
    -- "partial"            — at least one source succeeded, at least one failed
    -- "failed"             — all sources failed
    error_message        STRING,               -- populated on partial / failed

    -- ── Audit ─────────────────────────────────────────────────────────────────
    created_by           STRING,               -- "analyst_agent" or caller identifier
    created_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY status
OPTIONS (
    description = "One row per social listening ingestion run. Tracks sources, keywords, geo scope, and row counts written to social_trend_signals and social_mentions_staging."
);


-- =============================================================================
-- TABLE: social_trend_signals
-- =============================================================================
-- Normalised, numeric trend signal data.
--
-- signal_weight_score is always normalised to a 0–100 relative index
-- within the keyword set for a given run:
--   100 = peak interest in the timeframe
--   0   = effectively zero interest
--
-- Multiple data points per keyword per run (one row per date/geo bucket).
-- period_label tags rows for MoM comparison:
--   "current"  — within the lookback_days window
--   "prior"    — the equivalent preceding window (used for velocity computation)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.social_trend_signals`
(
    signal_id            STRING    NOT NULL,   -- UUID

    -- ── Run linkage ───────────────────────────────────────────────────────────
    run_id               STRING    NOT NULL,   -- → social_listening_runs.run_id

    -- ── Signal identity ───────────────────────────────────────────────────────
    platform             STRING    NOT NULL,
    -- "google_trends"      — relative search interest from Google Trends
    -- "reddit_search"      — keyword post frequency normalised to 0–100

    keyword_string       STRING    NOT NULL,   -- the exact keyword or phrase queried
    geography_code       STRING,               -- ISO 3166-1 alpha-2 or region code (e.g. "US", "GB", "US-CA")

    -- ── Signal value ──────────────────────────────────────────────────────────
    signal_date          DATE      NOT NULL,   -- the date this data point represents
    signal_weight_score  FLOAT64   NOT NULL,   -- 0.0–100.0 normalised relative index
    period_label         STRING,
    -- "current"            — within the requested lookback_days window
    -- "prior"              — the preceding equivalent window (for MoM baseline)

    -- ── Context ───────────────────────────────────────────────────────────────
    related_topics       JSON,
    -- Google Trends "related topics" (rising or top) for this keyword snapshot.
    -- Example: [{"topic": "Artificial Intelligence", "type": "rising", "value": 810}]
    -- Null for Reddit-sourced signals.

    timeframe_label      STRING,
    -- Human-readable label for the query timeframe used, e.g. "today 1-m".

    -- ── Audit ─────────────────────────────────────────────────────────────────
    capture_timestamp    TIMESTAMP NOT NULL
)
PARTITION BY signal_date
CLUSTER BY keyword_string, platform, geography_code
OPTIONS (
    description = "Normalised 0–100 trend interest signal by keyword, date, platform, and geography. Supports MoM velocity analysis via period_label."
);


-- =============================================================================
-- TABLE: social_mentions_staging
-- =============================================================================
-- Raw and normalised text blocks from forum and news ingest.
--
-- This table holds one row per captured post/article. It is a staging table:
-- rows are never updated in-place, only appended. Downstream analytics views
-- should join against social_listening_runs to filter by keyword set or
-- geo scope.
--
-- Text pipeline:
--   payload_text    — original captured text (may contain HTML, emojis, tracking URLs)
--   normalized_text — output of normalize_text() in social_listening_client.py:
--                     HTML tags stripped, tracking pixels removed, emojis removed,
--                     whitespace normalised
--
-- Sentiment:
--   sentiment_polarity_score is computed by _score_sentiment() in the client.
--   Range: -1.0 (maximally negative) to +1.0 (maximally positive).
--   Neutral text scores near 0.0.
--
-- Privacy:
--   author_handle stores a SHA-256 hex digest of the platform author name.
--   The original handle is never stored. Used only for deduplication within
--   a run; the hash cannot be reversed to re-identify the author.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.social_mentions_staging`
(
    mention_id           STRING    NOT NULL,   -- UUID (generated at ingest time)

    -- ── Run linkage ───────────────────────────────────────────────────────────
    run_id               STRING    NOT NULL,   -- → social_listening_runs.run_id

    -- ── Source classification ─────────────────────────────────────────────────
    source_platform      STRING    NOT NULL,
    -- "reddit"             — Reddit post or comment via PRAW
    -- "news"               — news article (future adapter)
    -- "linkedin_feed"      — LinkedIn public content (future adapter)

    community_subsegment STRING,
    -- Platform-specific community identifier.
    -- Reddit: subreddit name (e.g. "marketing", "SEO", "sales")
    -- News: publication or category slug

    keyword_matched      STRING,               -- which keyword from the run triggered this mention

    -- ── Content ───────────────────────────────────────────────────────────────
    post_id              STRING,               -- platform-native post/submission ID
    payload_text         STRING,               -- original captured text (pre-normalisation)
    normalized_text      STRING,               -- after normalize_text() scrubbing
    post_title           STRING,               -- Reddit submission title or article headline
    url_reference        STRING,               -- canonical URL of the post/article

    -- ── Sentiment ─────────────────────────────────────────────────────────────
    sentiment_polarity_score FLOAT64,
    -- -1.0 to +1.0 from _score_sentiment().
    -- 0.0 when sentiment scoring is unavailable (e.g. textblob not installed).

    -- ── Engagement metrics ────────────────────────────────────────────────────
    -- Reddit-specific; null for non-Reddit sources.
    upvote_ratio         FLOAT64,              -- fraction of upvotes to total votes (0.0–1.0)
    score                INT64,                -- net vote score (upvotes - downvotes)
    num_comments         INT64,                -- comment count at capture time
    engagement_score     FLOAT64,
    -- Composite: score * upvote_ratio. Higher = more positively engaged community.
    -- Used for ranking mentions by impact within a run.

    -- ── Authorship (hashed) ───────────────────────────────────────────────────
    author_handle        STRING,
    -- SHA-256 hex of the platform author name.
    -- Never the raw handle — for deduplication only.

    -- ── Timing ────────────────────────────────────────────────────────────────
    published_at         TIMESTAMP,            -- when the post was originally published
    capture_timestamp    TIMESTAMP NOT NULL
)
PARTITION BY DATE(capture_timestamp)
CLUSTER BY source_platform, community_subsegment, keyword_matched
OPTIONS (
    description = "Staging table for raw and normalised social mentions (Reddit, news). One row per captured post. author_handle is SHA-256 hashed for privacy. Supports keyword frequency, sentiment, and engagement analysis."
);
