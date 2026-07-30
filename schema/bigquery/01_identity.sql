-- Copyright 2026 @kenlim5656. All rights reserved.
-- Licensed under the Business Source License 1.1 (BSL 1.1)
-- Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
-- Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

-- =============================================================================
-- PAID MEDIA SCHEMA — IDENTITY LAYER
-- =============================================================================
-- The identity layer is the foundation of attribution. It captures every
-- identifier signal observed, and stitches them into canonical entities
-- (persons for B2C, accounts for B2B) via the identity graph.
--
-- Tables in this file:
--   identity_signals        Raw signals captured per session/event
--   identity_entities       Canonical resolved entities
--   identity_entity_signals The graph: entity ↔ signal mappings
--   identity_stitching_log  Audit trail of every stitching decision
-- =============================================================================


-- -----------------------------------------------------------------------------
-- identity_signals
-- One row per identifier observed per session. This is the raw intake table —
-- signals land here before stitching resolves them to entities.
-- Never store raw PII (email, phone, IP). Store only hashed or non-PII values.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.identity_signals`
(
    signal_id           STRING    NOT NULL,  -- UUID, generated on insert
    session_id          STRING,              -- links to touchpoint_events.session_id
    event_id            STRING,              -- links to a specific event if sub-session

    -- The identifier itself
    namespace_id        STRING    NOT NULL,  -- e.g. "platform_click_id.google.gclid"
    identifier_value    STRING    NOT NULL,  -- the actual value (hashed where required)

    -- Provenance
    captured_at         TIMESTAMP NOT NULL,
    capture_source      STRING    NOT NULL,  -- "landing_page" | "analytics" | "crm" | "capi" | "sdk" | "manual"
    capture_platform    STRING,              -- e.g. "ga4", "server_side_gtm", "salesforce"

    -- Confidence
    confidence_score    FLOAT64,             -- 0.0–1.0; derived from namespace determinism + capture method
    is_deterministic    BOOL,                -- from namespace registry

    -- Lifecycle
    expires_at          TIMESTAMP,           -- based on namespace lifetime_days; null = indefinite
    is_active           BOOL      NOT NULL,

    -- Audit
    ingested_at         TIMESTAMP NOT NULL,
    source_system       STRING,              -- which component wrote this row

    -- Flexible extension: any additional context as JSON
    context             JSON
)
PARTITION BY DATE(captured_at)
CLUSTER BY namespace_id, identifier_value
OPTIONS (
    description = "Raw identity signals captured across all touchpoints. One row per identifier per session."
);


-- -----------------------------------------------------------------------------
-- identity_entities
-- Canonical resolved entities — one row per person (B2C) or account (B2B).
-- The entity_id is the stable anchor everything else joins on.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.identity_entities`
(
    entity_id           STRING    NOT NULL,  -- UUID, stable forever
    entity_type         STRING    NOT NULL,  -- "person" | "account" | "household"

    -- B2B enrichment (populated when entity_type = "account")
    company_domain      STRING,              -- e.g. "acme.com" (non-PII)
    company_name        STRING,              -- display name, from CRM or IP intelligence
    crm_account_id      STRING,              -- Salesforce/HubSpot account ID if matched

    -- B2C enrichment (populated when entity_type = "person")
    crm_contact_id      STRING,              -- Salesforce/HubSpot contact ID if matched

    -- Identity confidence
    signal_count        INT64,               -- total signals stitched to this entity
    deterministic_signal_count INT64,        -- count of high-confidence signals
    confidence_tier     STRING,              -- "high" | "medium" | "low"
    -- high: ≥1 deterministic signal (hashed email, CRM ID, authenticated user ID)
    -- medium: 2+ probabilistic signals that corroborate
    -- low: single probabilistic signal only

    -- Lifecycle
    first_seen_at       TIMESTAMP,
    last_seen_at        TIMESTAMP,
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL,

    -- Merge history (when two entities are merged into one)
    merged_into_entity_id STRING,           -- if this entity was merged, points to the survivor
    is_active           BOOL      NOT NULL,

    -- Flexible extension
    attributes          JSON                -- org-specific entity attributes
)
PARTITION BY DATE(created_at)
CLUSTER BY entity_type, company_domain
OPTIONS (
    description = "Canonical resolved entities. One row per person (B2C) or company account (B2B). The stable anchor for attribution."
);


-- -----------------------------------------------------------------------------
-- identity_entity_signals
-- The identity graph: maps every signal to its resolved entity.
-- This is the stitching table — one row per (entity, namespace, identifier).
-- Multiple signals pointing to the same entity = that entity's identity footprint.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.identity_entity_signals`
(
    entity_id           STRING    NOT NULL,  -- → identity_entities.entity_id
    namespace_id        STRING    NOT NULL,  -- → identity_namespaces registry
    identifier_value    STRING    NOT NULL,  -- the identifier value

    -- Stitching metadata
    match_method        STRING    NOT NULL,
    -- "deterministic"  — exact match on a high-confidence signal (email hash, CRM ID)
    -- "probabilistic"  — inferred from co-occurrence (same session, IP domain match)
    -- "declarative"    — manually asserted by a human or upstream system
    -- "inherited"      — derived from a parent entity merge

    confidence_score    FLOAT64   NOT NULL,  -- 0.0–1.0
    stitched_by         STRING,              -- "watchdog_agent" | "analyst_agent" | "import" | "manual"
    stitched_at         TIMESTAMP NOT NULL,

    -- Signal lifecycle on this entity
    first_observed_at   TIMESTAMP,
    last_observed_at    TIMESTAMP,
    observation_count   INT64, -- how many times this signal has been seen for this entity

    -- Audit
    is_active           BOOL      NOT NULL,
    superseded_at       TIMESTAMP            -- set when a higher-confidence signal replaces this one
)
PARTITION BY DATE(stitched_at)
CLUSTER BY entity_id, namespace_id
OPTIONS (
    description = "Identity graph: maps every observed signal to its canonical entity. The stitching layer."
);


-- -----------------------------------------------------------------------------
-- identity_stitching_log
-- Immutable audit trail of every stitching decision made.
-- Never update or delete rows here — append only.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.identity_stitching_log`
(
    log_id              STRING    NOT NULL,  -- UUID
    event_type          STRING    NOT NULL,
    -- "entity_created"       — new entity resolved from signals
    -- "signal_attached"      — signal linked to existing entity
    -- "entities_merged"      — two entities collapsed into one
    -- "signal_detached"      — signal removed from entity (correction)
    -- "confidence_updated"   — confidence tier changed

    entity_id           STRING,
    source_entity_id    STRING,              -- for merges: the entity that was absorbed
    target_entity_id    STRING,              -- for merges: the surviving entity
    namespace_id        STRING,
    identifier_value    STRING,

    reason              STRING,              -- human-readable explanation of the decision
    triggered_by        STRING,              -- "analyst_agent" | "watchdog_agent" | "import" | "manual"
    occurred_at         TIMESTAMP NOT NULL,

    -- Before/after snapshot for corrections
    previous_state      JSON,
    new_state           JSON
)
PARTITION BY DATE(occurred_at)
CLUSTER BY entity_id, event_type
OPTIONS (
    description = "Immutable audit trail of all identity stitching decisions. Append-only."
);
