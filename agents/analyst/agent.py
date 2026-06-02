# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
The Analyst — Attribution Modeling Agent.
Runs daily. Stitches identities and executes the MTA model.
Writes structured output to paid-media-schema tables so the paid-media-mcp
can surface results in interactive skill sessions.
"""
import uuid
import structlog
from datetime import datetime, timezone

from agents.base import BaseAgent
from tools import bigquery_client as bq

log = structlog.get_logger()

SYSTEM = """You are the Analyst, an attribution modeling agent for a paid media pipeline.

You are platform-agnostic. The identity graph holds signals from all ad platforms
(Google, Meta, LinkedIn, TikTok, etc.) and analytics tools (GA4, Adobe, Segment).

Your daily run sequence — follow it in order:
1. Call `enrich_sessions` — resolve anonymous sessions to company domains (account-based analytics).
2. Call `start_attribution_run` — registers the run in BigQuery, returns a run_id.
3. Call `stitch_identities` — merges new signals into the identity graph.
4. Call `run_mta_model` with the run_id — computes Full-Path attribution and writes results.
5. Call `build_channel_summary` with the run_id — aggregates to channel level for the MCP.
6. Call `write_analyst_insight` — surface the most important finding as a structured insight.
7. Call `complete_attribution_run` with the run_id — marks run as completed.

Default model: Full-Path (first touch 30%, last touch 30%, middle touches split 40%).
Data-driven models: use run_shapley_model or run_markov_model instead of run_mta_model
when data volume allows (> 1,000 converted paths). Shapley is more accurate but slower;
Markov is faster for many channels. Always compare results to the Full-Path baseline.
Write efficient SQL. Prefer incremental queries over full scans."""


class AnalystAgent(BaseAgent):
    name = "analyst"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "enrich_sessions",
            "description": (
                "Resolve anonymous web sessions to company domains using IP intelligence. "
                "Reads recent sessions from sgtm_request_logs that haven't been enriched yet, "
                "calls the IP intelligence provider (Clearbit / ipinfo.io) for each, and writes "
                "results to: ip_resolution_cache (/24 prefix only — never raw IPs), "
                "company_profiles (upsert), company_sessions (de-anonymized session), and "
                "company_engagement (rolling-30d aggregation). "
                "Run first in the daily sequence before identity stitching and attribution."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_hours": {
                        "type": "integer",
                        "description": "How many hours back to look for unenriched sessions. Default: 48.",
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Max sessions to process in this run. Default: 1000.",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "run_bigquery_query",
            "description": "Execute a SELECT query. Returns up to 200 rows.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "params": {"type": "object", "additionalProperties": True},
                },
                "required": ["sql"],
            },
        },
        {
            "name": "start_attribution_run",
            "description": (
                "Register a new attribution model run in BigQuery. "
                "Returns a run_id that must be passed to all subsequent tools."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "enum": ["full_path", "first_touch", "last_touch", "linear", "time_decay", "position_based"],
                        "description": "Attribution model to run",
                    },
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Conversion types to include, e.g. ['opportunity_created', 'purchase']",
                    },
                },
                "required": ["model_name", "period_start", "period_end"],
            },
        },
        {
            "name": "stitch_identities",
            "description": (
                "Run the identity stitching job: match session signals to canonical entities "
                "using email domain, IP routing, and deterministic click ID co-occurrence. "
                "Writes matches to identity_entity_signals and identity_entities. "
                "Platform-agnostic — works across all signal namespaces."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "lookback_days": {"type": "integer", "default": 1},
                },
                "required": [],
            },
        },
        {
            "name": "run_mta_model",
            "description": (
                "Compute Full-Path MTA weights for all conversion paths in the date range. "
                "Inserts touchpoint-level credit rows into attribution_results. "
                "Returns path count and conversion count."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string", "description": "From start_attribution_run"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "model_config": {
                        "type": "object",
                        "description": "Model parameters. For full_path: {first_touch_pct, last_touch_pct}",
                        "additionalProperties": True,
                    },
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "build_channel_summary",
            "description": (
                "Aggregate attribution_results into attribution_channel_summary — "
                "the pre-computed channel-level view read by the paid-media-mcp and skills. "
                "Joins with platform_daily_spend to compute attributed CPA/ROAS."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string"},
                    "period_end":   {"type": "string"},
                    "model_name":   {"type": "string"},
                },
                "required": ["run_id", "period_start", "period_end", "model_name"],
            },
        },
        {
            "name": "run_shapley_model",
            "description": (
                "Compute Shapley value attribution — the game-theoretic fair attribution model. "
                "Gives each channel credit equal to its average marginal contribution across all "
                "coalition orderings. Eliminates first-touch / last-touch bias. "
                "Recommended when path count > 1,000. "
                "Writes results to attribution_results and attribution_channel_summary."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "e.g. ['opportunity_created', 'purchase']",
                    },
                    "max_channels": {"type": "integer", "description": "Cap on channels (default from settings)"},
                    "max_paths":    {"type": "integer", "description": "Sample size cap (default from settings)"},
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "run_markov_model",
            "description": (
                "Compute Markov chain attribution — transition-matrix based model. "
                "Each channel's credit is proportional to its removal effect: "
                "the drop in overall conversion probability when that channel is removed. "
                "Faster than Shapley for many channels. Good for long B2B paths. "
                "Writes results to attribution_results and attribution_channel_summary."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":       {"type": "string"},
                    "period_start": {"type": "string", "description": "YYYY-MM-DD"},
                    "period_end":   {"type": "string", "description": "YYYY-MM-DD"},
                    "conversion_types": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "max_paths": {"type": "integer", "description": "Sample size cap (default from settings)"},
                },
                "required": ["run_id", "period_start", "period_end"],
            },
        },
        {
            "name": "write_analyst_insight",
            "description": (
                "Persist a structured insight or recommendation to the analyst_insights table. "
                "Call once per run with the most important finding."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":         {"type": "string"},
                    "insight_type":   {
                        "type": "string",
                        "enum": [
                            "attribution_summary", "channel_anomaly", "path_pattern",
                            "model_readiness", "stitching_quality", "budget_efficiency",
                            "incrementality_signal", "audience_overlap",
                        ],
                    },
                    "headline":       {"type": "string", "description": "One sentence summary"},
                    "detail":         {"type": "string", "description": "Full analysis narrative"},
                    "priority":       {"type": "string", "enum": ["high", "medium", "low"]},
                    "recommendation": {"type": "string"},
                    "estimated_impact": {"type": "string"},
                    "period_start":   {"type": "string"},
                    "period_end":     {"type": "string"},
                    "data_points":    {"type": "object", "additionalProperties": True},
                },
                "required": ["run_id", "insight_type", "headline", "priority"],
            },
        },
        {
            "name": "complete_attribution_run",
            "description": "Mark an attribution run as completed or failed in attribution_runs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "run_id":                  {"type": "string"},
                    "status":                  {"type": "string", "enum": ["completed", "failed"]},
                    "paths_modeled":           {"type": "integer"},
                    "conversions_attributed":  {"type": "integer"},
                    "identity_match_rate":     {"type": "number"},
                    "avg_path_length":         {"type": "number"},
                    "error_message":           {"type": "string"},
                },
                "required": ["run_id", "status"],
            },
        },
    ]

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_enrich_sessions(
        self,
        lookback_hours: int | None = None,
        batch_size: int | None = None,
    ) -> dict:
        """
        Resolve anonymous sessions to company domains and write account analytics records.
        Delegates to EnrichmentJob which handles IP intelligence + all DB writes.
        Raw IP addresses are processed entirely within EnrichmentJob.run() and never
        stored, logged, or returned from this method.
        """
        from agents.analyst.enrichment import EnrichmentJob
        job = EnrichmentJob()
        return job.run(
            lookback_hours=lookback_hours,
            batch_size=batch_size,
        )

    def _tool_run_bigquery_query(self, sql: str, params: dict | None = None) -> dict:
        rows = bq.run_query(sql, params)
        return {"rows": rows[:200], "total_rows": len(rows)}

    def _tool_start_attribution_run(
        self,
        model_name: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
    ) -> dict:
        run_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()
        model_config = {
            "full_path":      {"first_touch_pct": 0.30, "last_touch_pct": 0.30},
            "first_touch":    {},
            "last_touch":     {},
            "linear":         {},
            "time_decay":     {"half_life_days": 7},
            "position_based": {"first_touch_pct": 0.40, "last_touch_pct": 0.40},
        }.get(model_name, {})

        row = {
            "run_id":             run_id,
            "run_type":           "scheduled",
            "model_name":         model_name,
            "model_version":      "1.0.0",
            "model_config":       str(model_config),
            "period_start":       period_start,
            "period_end":         period_end,
            "conversion_types":   conversion_types or [],
            "entity_types":       ["person", "account"],
            "paths_modeled":      None,
            "conversions_attributed": None,
            "total_credit":       None,
            "total_conversion_value": None,
            "data_quality_score": None,
            "identity_match_rate": None,
            "avg_path_length":    None,
            "started_at":         now,
            "completed_at":       None,
            "duration_seconds":   None,
            "status":             "running",
            "error_message":      None,
            "triggered_by":       "analyst_agent",
        }
        errors = bq.insert_rows("attribution_runs", [row])
        if errors:
            log.warning("analyst.run_insert_error", errors=errors)
        log.info("analyst.run_started", run_id=run_id, model=model_name, period=f"{period_start}→{period_end}")
        return {"run_id": run_id, "model_name": model_name, "period_start": period_start, "period_end": period_end}

    def _tool_stitch_identities(self, lookback_days: int = 1) -> dict:
        """
        Identity stitching using the paid-media-schema tables.
        Phase 1: domain matching (sessions → CRM leads → entity)
        Phase 2: click ID co-occurrence (sessions with matching click IDs → same entity)
        Writes to identity_entities and identity_entity_signals.
        """
        now = datetime.now(timezone.utc).isoformat()

        # ── Phase 1: Email-domain → account stitching ─────────────────────────
        # Match sessions to CRM leads via email domain, then roll up to entity.
        domain_sql = f"""
        MERGE {bq.table_ref('identity_entity_signals')} AS target
        USING (
            SELECT
                COALESCE(e.entity_id, GENERATE_UUID())   AS entity_id,
                'analytics_cookie.google.ga4_client_id'  AS namespace_id,
                s.ga4_client_id                          AS identifier_value,
                'probabilistic'                          AS match_method,
                0.65                                     AS confidence_score,
                MIN(s.session_start_at)                  AS first_observed_at,
                MAX(s.session_start_at)                  AS last_observed_at,
                COUNT(*)                                 AS observation_count
            FROM {bq.table_ref('sessions')} s
            JOIN {bq.table_ref('crm_leads_staging')} l
              ON SPLIT(l.email, '@')[SAFE_OFFSET(1)] = s.entry_referrer  -- domain match
                 OR s.ga4_client_id = l.ga_client_id                     -- direct match
            LEFT JOIN {bq.table_ref('identity_entity_signals')} e
              ON e.namespace_id = 'crm_id.salesforce.account_id'
             AND e.identifier_value = l.account_id
            WHERE DATE(s.session_start_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
              AND s.ga4_client_id IS NOT NULL
              AND l.account_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        ) AS source
        ON target.namespace_id = source.namespace_id
           AND target.identifier_value = source.identifier_value
        WHEN MATCHED THEN UPDATE SET
            last_observed_at  = source.last_observed_at,
            observation_count = target.observation_count + source.observation_count
        WHEN NOT MATCHED THEN INSERT (
            entity_id, namespace_id, identifier_value, match_method,
            confidence_score, stitched_by, stitched_at,
            first_observed_at, last_observed_at, observation_count, is_active
        ) VALUES (
            source.entity_id, source.namespace_id, source.identifier_value,
            source.match_method, source.confidence_score, 'analyst_agent', '{now}',
            source.first_observed_at, source.last_observed_at, source.observation_count, TRUE
        )
        """
        domain_rows = bq.run_dml(domain_sql)

        # ── Phase 2: Click ID co-occurrence ───────────────────────────────────
        # Sessions sharing the same click ID in the same conversion path → same entity.
        # For each click ID present in sessions, ensure entity_signals has an entry.
        click_id_namespaces = [
            ("gclid",     "platform_click_id.google.gclid",       0.95),
            ("dclid",     "platform_click_id.google.dclid",       0.95),
            ("fbclid",    "platform_click_id.meta.fbclid",        0.90),
            ("li_fat_id", "platform_click_id.linkedin.li_fat_id", 0.92),
            ("ttclid",    "platform_click_id.tiktok.ttclid",      0.90),
            ("msclkid",   "platform_click_id.microsoft.msclkid",  0.95),
        ]
        click_rows = 0
        for col, ns_id, conf in click_id_namespaces:
            sql = f"""
            MERGE {bq.table_ref('identity_entity_signals')} AS target
            USING (
                SELECT
                    COALESCE(existing.entity_id, GENERATE_UUID()) AS entity_id,
                    '{ns_id}'   AS namespace_id,
                    s.{col}     AS identifier_value,
                    'deterministic' AS match_method,
                    {conf}      AS confidence_score,
                    MIN(s.session_start_at) AS first_observed_at,
                    MAX(s.session_start_at) AS last_observed_at,
                    COUNT(*)    AS observation_count
                FROM {bq.table_ref('sessions')} s
                LEFT JOIN {bq.table_ref('identity_entity_signals')} existing
                  ON existing.namespace_id = '{ns_id}'
                 AND existing.identifier_value = s.{col}
                WHERE DATE(s.session_start_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                  AND s.{col} IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5
            ) AS source
            ON target.namespace_id = source.namespace_id
               AND target.identifier_value = source.identifier_value
            WHEN MATCHED THEN UPDATE SET
                last_observed_at  = source.last_observed_at,
                observation_count = target.observation_count + source.observation_count
            WHEN NOT MATCHED THEN INSERT (
                entity_id, namespace_id, identifier_value, match_method,
                confidence_score, stitched_by, stitched_at,
                first_observed_at, last_observed_at, observation_count, is_active
            ) VALUES (
                source.entity_id, source.namespace_id, source.identifier_value,
                source.match_method, source.confidence_score, 'analyst_agent', '{now}',
                source.first_observed_at, source.last_observed_at, source.observation_count, TRUE
            )
            """
            try:
                click_rows += bq.run_dml(sql)
            except Exception as exc:
                log.warning("analyst.stitch_click_id_error", namespace=ns_id, error=str(exc))

        # ── Phase 3: Sync entity table ─────────────────────────────────────────
        # Ensure identity_entities has a row for every entity referenced in signals.
        entity_sync_sql = f"""
        MERGE {bq.table_ref('identity_entities')} AS target
        USING (
            SELECT
                entity_id,
                'account' AS entity_type,
                COUNT(*)  AS signal_count,
                COUNTIF(match_method = 'deterministic') AS deterministic_signal_count,
                MIN(first_observed_at) AS first_seen_at,
                MAX(last_observed_at)  AS last_seen_at
            FROM {bq.table_ref('identity_entity_signals')}
            WHERE is_active = TRUE
            GROUP BY entity_id
        ) AS source
        ON target.entity_id = source.entity_id
        WHEN MATCHED THEN UPDATE SET
            signal_count = source.signal_count,
            deterministic_signal_count = source.deterministic_signal_count,
            last_seen_at = source.last_seen_at,
            confidence_tier = CASE
                WHEN source.deterministic_signal_count >= 1 THEN 'high'
                WHEN source.signal_count >= 2 THEN 'medium'
                ELSE 'low'
            END,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            entity_id, entity_type, signal_count, deterministic_signal_count,
            confidence_tier, first_seen_at, last_seen_at, is_active, created_at, updated_at
        ) VALUES (
            source.entity_id, source.entity_type, source.signal_count,
            source.deterministic_signal_count,
            CASE WHEN source.deterministic_signal_count >= 1 THEN 'high'
                 WHEN source.signal_count >= 2 THEN 'medium' ELSE 'low' END,
            source.first_seen_at, source.last_seen_at, TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """
        entity_rows = bq.run_dml(entity_sync_sql)

        log.info("analyst.stitch_complete", domain_rows=domain_rows, click_rows=click_rows, entity_rows=entity_rows)
        return {
            "domain_stitched":  domain_rows,
            "click_id_stitched": click_rows,
            "entities_synced":  entity_rows,
            "lookback_days":    lookback_days,
        }

    def _tool_run_mta_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        model_config: dict | None = None,
    ) -> dict:
        cfg = model_config or {}
        first_pct = float(cfg.get("first_touch_pct", 0.30))
        last_pct  = float(cfg.get("last_touch_pct", 0.30))
        mid_pct   = round(1.0 - first_pct - last_pct, 4)

        now = datetime.now(timezone.utc).isoformat()

        # Delete any stale results for this run (idempotent re-runs)
        bq.run_dml(f"""
            DELETE FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
        """)

        # Compute and insert Full-Path weighted credit
        insert_sql = f"""
        INSERT INTO {bq.table_ref('attribution_results')} (
            result_id, run_id, path_id, touchpoint_id, conversion_id, entity_id,
            conversion_date, touchpoint_date, platform, channel, campaign_id,
            touchpoint_type, path_position, path_total_touches,
            conversion_type, conversion_value, deal_value,
            credit_weight, credit_conversions, credit_value, credit_deal_value,
            model_name, period_start, period_end, created_at
        )
        WITH paths AS (
            SELECT
                ies.entity_id,
                t.touchpoint_id,
                t.touchpoint_at,
                t.platform,
                t.channel,
                t.campaign_id,
                t.touchpoint_type,
                c.conversion_id,
                c.converted_at,
                c.conversion_type,
                c.conversion_value,
                c.deal_value,
                ROW_NUMBER() OVER (
                    PARTITION BY ies.entity_id, c.conversion_id
                    ORDER BY t.touchpoint_at ASC
                ) AS touch_seq,
                COUNT(*) OVER (
                    PARTITION BY ies.entity_id, c.conversion_id
                ) AS total_touches,
                -- Stable path_id per entity+conversion
                TO_HEX(MD5(CONCAT(ies.entity_id, c.conversion_id))) AS path_id
            FROM {bq.table_ref('touchpoint_events')} t
            JOIN {bq.table_ref('identity_entity_signals')} ies
              ON ies.namespace_id = 'analytics_cookie.google.ga4_client_id'
             AND ies.identifier_value = t.session_id
            JOIN {bq.table_ref('conversion_events')} c
              ON c.entity_id = ies.entity_id
             AND c.converted_at > t.touchpoint_at
             AND DATE(c.converted_at) BETWEEN '{period_start}' AND '{period_end}'
            WHERE DATE(t.touchpoint_at) BETWEEN
                  DATE_SUB(DATE '{period_start}', INTERVAL 90 DAY)
                  AND '{period_end}'
        )
        SELECT
            GENERATE_UUID()                         AS result_id,
            '{run_id}'                              AS run_id,
            path_id,
            touchpoint_id,
            conversion_id,
            entity_id,
            DATE(converted_at)                      AS conversion_date,
            DATE(touchpoint_at)                     AS touchpoint_date,
            platform,
            channel,
            campaign_id,
            touchpoint_type,
            touch_seq                               AS path_position,
            total_touches                           AS path_total_touches,
            conversion_type,
            COALESCE(conversion_value, 0)           AS conversion_value,
            COALESCE(deal_value, 0)                 AS deal_value,
            CASE
                WHEN total_touches = 1 THEN 1.0
                WHEN touch_seq = 1           THEN {first_pct}
                WHEN touch_seq = total_touches THEN {last_pct}
                ELSE {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_weight,
            CASE
                WHEN total_touches = 1 THEN 1.0
                WHEN touch_seq = 1           THEN {first_pct}
                WHEN touch_seq = total_touches THEN {last_pct}
                ELSE {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_conversions,
            CASE
                WHEN total_touches = 1 THEN COALESCE(conversion_value, 0)
                WHEN touch_seq = 1           THEN COALESCE(conversion_value, 0) * {first_pct}
                WHEN touch_seq = total_touches THEN COALESCE(conversion_value, 0) * {last_pct}
                ELSE COALESCE(conversion_value, 0) * {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_value,
            CASE
                WHEN total_touches = 1 THEN COALESCE(deal_value, 0)
                WHEN touch_seq = 1           THEN COALESCE(deal_value, 0) * {first_pct}
                WHEN touch_seq = total_touches THEN COALESCE(deal_value, 0) * {last_pct}
                ELSE COALESCE(deal_value, 0) * {mid_pct} / NULLIF(total_touches - 2, 0)
            END                                     AS credit_deal_value,
            'full_path'                             AS model_name,
            DATE '{period_start}'                   AS period_start,
            DATE '{period_end}'                     AS period_end,
            TIMESTAMP '{now}'                       AS created_at
        FROM paths
        """
        rows_inserted = bq.run_dml(insert_sql)

        # Quick summary stats
        stats = bq.run_query(f"""
            SELECT
                COUNT(DISTINCT path_id)     AS paths,
                COUNT(DISTINCT conversion_id) AS conversions,
                ROUND(SUM(credit_conversions), 1) AS total_credit
            FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
        """)

        log.info("analyst.mta_complete", run_id=run_id, rows=rows_inserted)
        return {
            "run_id":          run_id,
            "rows_inserted":   rows_inserted,
            "paths_modeled":   stats[0].get("paths", 0) if stats else 0,
            "conversions":     stats[0].get("conversions", 0) if stats else 0,
            "total_credit":    stats[0].get("total_credit", 0) if stats else 0,
        }

    def _tool_build_channel_summary(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        model_name: str,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()

        bq.run_dml(f"""
            DELETE FROM {bq.table_ref('attribution_channel_summary')}
            WHERE run_id = '{run_id}'
        """)

        insert_sql = f"""
        INSERT INTO {bq.table_ref('attribution_channel_summary')} (
            summary_id, run_id, model_name, period_start, period_end,
            platform, channel, conversion_type,
            total_touches, unique_entities,
            first_touch_count, last_touch_count,
            attributed_conversions, attributed_value, attributed_deal_value, credit_share_pct,
            total_spend,
            attributed_cpa, attributed_roas,
            platform_conversions, platform_cpa,
            attribution_vs_platform_delta_pct,
            generated_at
        )
        WITH base AS (
            SELECT
                platform, channel, conversion_type,
                COUNT(*)                        AS total_touches,
                COUNT(DISTINCT entity_id)       AS unique_entities,
                COUNTIF(path_position = 1)      AS first_touch_count,
                COUNTIF(path_position = path_total_touches) AS last_touch_count,
                SUM(credit_conversions)         AS attributed_conversions,
                SUM(credit_value)               AS attributed_value,
                SUM(credit_deal_value)          AS attributed_deal_value,
                SUM(SUM(credit_conversions)) OVER () AS grand_total_credit
            FROM {bq.table_ref('attribution_results')}
            WHERE run_id = '{run_id}'
            GROUP BY platform, channel, conversion_type
        ),
        with_spend AS (
            SELECT
                b.*,
                COALESCE(SUM(s.spend), 0) AS total_spend,
                COALESCE(SUM(s.platform_conversions), 0) AS platform_conversions
            FROM base b
            LEFT JOIN {bq.table_ref('platform_daily_spend')} s
              ON s.platform = b.platform
             AND s.date BETWEEN '{period_start}' AND '{period_end}'
            GROUP BY
                b.platform, b.channel, b.conversion_type,
                b.total_touches, b.unique_entities,
                b.first_touch_count, b.last_touch_count,
                b.attributed_conversions, b.attributed_value,
                b.attributed_deal_value, b.grand_total_credit
        )
        SELECT
            GENERATE_UUID()     AS summary_id,
            '{run_id}'          AS run_id,
            '{model_name}'      AS model_name,
            DATE '{period_start}' AS period_start,
            DATE '{period_end}'   AS period_end,
            platform, channel, conversion_type,
            total_touches, unique_entities,
            first_touch_count, last_touch_count,
            attributed_conversions,
            attributed_value,
            attributed_deal_value,
            SAFE_DIVIDE(attributed_conversions, grand_total_credit) * 100 AS credit_share_pct,
            total_spend,
            SAFE_DIVIDE(total_spend, NULLIF(attributed_conversions, 0)) AS attributed_cpa,
            SAFE_DIVIDE(attributed_value, NULLIF(total_spend, 0))       AS attributed_roas,
            platform_conversions,
            SAFE_DIVIDE(total_spend, NULLIF(platform_conversions, 0))   AS platform_cpa,
            SAFE_DIVIDE(attributed_conversions - platform_conversions,
                        NULLIF(platform_conversions, 0)) * 100          AS attribution_vs_platform_delta_pct,
            TIMESTAMP '{now}'   AS generated_at
        FROM with_spend
        """
        rows = bq.run_dml(insert_sql)

        top = bq.run_query(f"""
            SELECT channel, platform, attributed_conversions, credit_share_pct,
                   attributed_cpa, attributed_roas
            FROM {bq.table_ref('attribution_channel_summary')}
            WHERE run_id = '{run_id}'
            ORDER BY attributed_conversions DESC
            LIMIT 10
        """)

        log.info("analyst.summary_built", run_id=run_id, rows=rows)
        return {"rows_written": rows, "top_channels": top}

    def _tool_write_analyst_insight(
        self,
        run_id: str,
        insight_type: str,
        headline: str,
        priority: str,
        detail: str | None = None,
        recommendation: str | None = None,
        estimated_impact: str | None = None,
        period_start: str | None = None,
        period_end: str | None = None,
        data_points: dict | None = None,
    ) -> dict:
        insight_id = bq.new_uuid()
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "insight_id":        insight_id,
            "run_id":            run_id,
            "insight_type":      insight_type,
            "period_start":      period_start,
            "period_end":        period_end,
            "affected_platform": None,
            "affected_channel":  None,
            "headline":          headline,
            "detail":            detail,
            "data_points":       str(data_points) if data_points else None,
            "confidence":        "high",
            "has_recommendation": recommendation is not None,
            "recommendation":    recommendation,
            "estimated_impact":  estimated_impact,
            "priority":          priority,
            "status":            "new",
            "reviewed_at":       None,
            "actioned_by":       None,
            "generated_at":      now,
        }
        errors = bq.insert_rows("analyst_insights", [row])
        log.info("analyst.insight_written", insight_id=insight_id, type=insight_type)
        return {"insight_id": insight_id, "errors": len(errors)}

    def _tool_run_shapley_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
        max_channels: int | None = None,
        max_paths: int | None = None,
    ) -> dict:
        from tools.attribution_models import load_paths, compute_shapley, write_model_results
        max_ch  = max_channels or settings.shapley_max_channels
        max_p   = max_paths    or settings.shapley_max_paths

        log.info("analyst.shapley_start", run_id=run_id, period=f"{period_start}→{period_end}")
        paths = load_paths(period_start, period_end, conversion_types, max_p)
        if not paths:
            return {"error": "No conversion paths found for the specified period and conversion types."}

        weights = compute_shapley(paths, max_channels=max_ch)
        result  = write_model_results(run_id, "shapley_value", period_start, period_end, paths, weights)

        log.info("analyst.shapley_complete", run_id=run_id, paths=len(paths))
        return result

    def _tool_run_markov_model(
        self,
        run_id: str,
        period_start: str,
        period_end: str,
        conversion_types: list[str] | None = None,
        max_paths: int | None = None,
    ) -> dict:
        from tools.attribution_models import load_paths, compute_markov, write_model_results
        max_p = max_paths or settings.shapley_max_paths

        log.info("analyst.markov_start", run_id=run_id, period=f"{period_start}→{period_end}")
        paths = load_paths(period_start, period_end, conversion_types, max_p)
        if not paths:
            return {"error": "No conversion paths found for the specified period and conversion types."}

        weights = compute_markov(paths)
        result  = write_model_results(run_id, "markov_chain", period_start, period_end, paths, weights)

        log.info("analyst.markov_complete", run_id=run_id, paths=len(paths))
        return result

    def _tool_complete_attribution_run(
        self,
        run_id: str,
        status: str,
        paths_modeled: int | None = None,
        conversions_attributed: int | None = None,
        identity_match_rate: float | None = None,
        avg_path_length: float | None = None,
        error_message: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        set_clauses = [
            f"status = '{status}'",
            f"completed_at = TIMESTAMP '{now}'",
        ]
        if paths_modeled is not None:
            set_clauses.append(f"paths_modeled = {paths_modeled}")
        if conversions_attributed is not None:
            set_clauses.append(f"conversions_attributed = {conversions_attributed}")
        if identity_match_rate is not None:
            set_clauses.append(f"identity_match_rate = {identity_match_rate}")
        if avg_path_length is not None:
            set_clauses.append(f"avg_path_length = {avg_path_length}")
        if error_message:
            set_clauses.append(f"error_message = '{error_message.replace(chr(39), chr(39)*2)}'")

        bq.run_dml(f"""
            UPDATE {bq.table_ref('attribution_runs')}
            SET {', '.join(set_clauses)}
            WHERE run_id = '{run_id}'
        """)

        log.info("analyst.run_completed", run_id=run_id, status=status)
        return {"run_id": run_id, "status": status, "completed_at": now}
