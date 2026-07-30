# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Attribution Verifier — forensic engine for data integrity and attribution accuracy
(Task 37, Component 2).

Evaluates CRM and session data against three explicit integrity rules:

  1. Orphaned Token Test:
     Scans crm_leads_staging for leads where a paid click token (gclid, fbclid,
     msclkid, ttclid, li_fat_id) or a paid UTM medium is present on the associated
     analytics session (joined via ga4_client_id), but the CRM LeadSource is assigned
     to an offline, organic, or manually sourced label — indicating the paid channel's
     contribution has been overwritten or misattributed.

  2. Timestamp Divergence Test:
     Where a lead modification timestamp (systemmodstamp, lead_source_updated_at, or
     equivalent) is accessible, compares the creation time of the paid click token
     against the application time of the current LeadSource. If the offline label was
     applied after the ad click was captured, classifies the event as an explicit
     overwrite with confidence proportional to the overwrite lag in hours.

  3. Phantom Conversion Test:
     Cross-references platform_daily_spend (platform-reported conversions) against
     conversion_events (CRM-matched conversions). Flags days and platforms where the
     ad platform claims significantly more conversions than the CRM can structurally
     account for within the attribution window, indicating pixel misconfiguration,
     view-through inflation, or duplicate conversion event firing.

Output:
  All detected anomalies are streamed to data_attribution_anomalies in BigQuery.
  v_attribution_correction_weights (the diagnostic view) then aggregates these rows
  into per-channel / per-geo / per-week correction multiplier vectors.

Privacy constraints:
  - crm_lead_id stores the opaque CRM record ID (account_id) — NOT email or name.
  - session_id is an opaque analytics identifier — no IP, device fingerprint, or PII.
  - No raw email addresses, phone numbers, or personal identifiers are written or
    returned by this module.
  - crm_lead_source stores the LeadSource label string (e.g. "Webinar Ingestion")
    — this is org-level metadata, not personal data.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import structlog

from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Constants ──────────────────────────────────────────────────────────────────

# Default offline / non-paid lead source labels that should NOT appear alongside
# paid click tokens. Case-insensitive; matched after LOWER(TRIM()).
_DEFAULT_OFFLINE_SOURCES: frozenset[str] = frozenset({
    "content syndication",
    "webinar",
    "webinar ingestion",
    "webinar registration",
    "sdr cold outreach",
    "cold outreach",
    "sdr outreach",
    "sdr",
    "direct mail",
    "event",
    "trade show",
    "conference",
    "referral",
    "employee referral",
    "partner referral",
    "channel partner",
    "word of mouth",
    "organic",
    "organic search",
    "internal",
    "press release",
    "podcast",
    "pr",
    "earned media",
    "dark social",
})

# Severity weight per anomaly type — used to compute correction multiplier.
# Mirrors the weights in v_attribution_correction_weights for consistency.
_SEVERITY_WEIGHT: dict[str, float] = {
    "phantom_conversion":   1.00,
    "timestamp_divergence": 0.70,
    "orphaned_token":       0.50,
}

# BQ streaming insert batch size (API maximum is 10,000; keep conservative)
_BQ_BATCH_SIZE = 500

# Candidate column names to probe for lead modification timestamp in crm_leads_staging.
# Tried in order — first one that exists in the schema is used.
_TS_COLUMN_CANDIDATES = (
    "lead_source_updated_at",
    "systemmodstamp",       # Salesforce standard (System Modstamp)
    "updated_at",
    "last_modified_at",
    "modified_date",
)


class AttributionVerifier:
    """
    Forensic verification engine for attribution data integrity.

    Usage::

        verifier = AttributionVerifier()
        result = verifier.run_audit(
            lookback_days=90,
            attribution_window_days=7,
        )
        # result["cleanliness_score"]  → 0–100
        # result["anomaly_count"]      → total anomalies written to BQ
        # result["total_pipeline_at_risk"] → estimated ARR at risk

    All anomaly rows are written to ``data_attribution_anomalies`` via BigQuery
    streaming insert. The ``run_id`` groups all anomalies from this audit instance.
    """

    def __init__(self) -> None:
        self._run_id: str = bq.new_uuid()

    @property
    def run_id(self) -> str:
        return self._run_id

    # ── Public API ─────────────────────────────────────────────────────────────

    def run_audit(
        self,
        lookback_days: int = 90,
        attribution_window_days: int = 7,
        lead_source_offline_patterns: list[str] | None = None,
    ) -> dict:
        """
        Execute all three forensic tests and write anomaly rows to BigQuery.

        Parameters
        ----------
        lookback_days : int
            How far back to scan CRM and session data (default 90 days).
            Longer windows surface more historical overwrites but increase
            BQ query cost. 90 days is the recommended production default.

        attribution_window_days : int
            Grace window for the Phantom Conversion test (default 7 days).
            Allows for CRM data latency — platform conversions may precede
            CRM record creation by up to this many days before being flagged.

        lead_source_offline_patterns : list[str] | None
            Override the default offline source label set. Each string is
            lowercased and stripped before matching. Provide the full set of
            your org's offline/non-paid LeadSource values.

        Returns
        -------
        dict
            Backend summary dict:
              run_id, anomaly_count, anomalies_by_type, anomalies_by_platform,
              anomalies_by_lead_source, total_pipeline_at_risk,
              cleanliness_score (0–100), test_results, write_errors.
        """
        log.info(
            "attribution_verifier.start",
            run_id=self._run_id,
            lookback_days=lookback_days,
            attribution_window_days=attribution_window_days,
        )

        offline_sources: frozenset[str] = (
            frozenset(s.lower().strip() for s in lead_source_offline_patterns)
            if lead_source_offline_patterns
            else _DEFAULT_OFFLINE_SOURCES
        )

        all_anomalies: list[dict] = []
        test_results: dict[str, dict] = {}

        # ── Test 1: Orphaned Token ─────────────────────────────────────────────
        try:
            t1_rows = self._run_orphaned_token_test(lookback_days, offline_sources)
            test_results["orphaned_token"] = {
                "ok": True,
                "anomalies_detected": len(t1_rows),
            }
            all_anomalies.extend(t1_rows)
            log.info("attribution_verifier.test1_complete", anomalies=len(t1_rows))
        except Exception as exc:
            log.warning("attribution_verifier.test1_failed", error=str(exc))
            test_results["orphaned_token"] = {
                "ok": False,
                "error": str(exc),
                "anomalies_detected": 0,
            }

        # ── Test 2: Timestamp Divergence ──────────────────────────────────────
        try:
            t2_rows = self._run_timestamp_divergence_test(lookback_days, offline_sources)
            test_results["timestamp_divergence"] = {
                "ok": True,
                "anomalies_detected": len(t2_rows),
            }
            all_anomalies.extend(t2_rows)
            log.info("attribution_verifier.test2_complete", anomalies=len(t2_rows))
        except Exception as exc:
            log.warning("attribution_verifier.test2_failed", error=str(exc))
            test_results["timestamp_divergence"] = {
                "ok": False,
                "error": str(exc),
                "anomalies_detected": 0,
            }

        # ── Test 3: Phantom Conversion ────────────────────────────────────────
        try:
            t3_rows = self._run_phantom_conversion_test(
                lookback_days, attribution_window_days
            )
            test_results["phantom_conversion"] = {
                "ok": True,
                "anomalies_detected": len(t3_rows),
            }
            all_anomalies.extend(t3_rows)
            log.info("attribution_verifier.test3_complete", anomalies=len(t3_rows))
        except Exception as exc:
            log.warning("attribution_verifier.test3_failed", error=str(exc))
            test_results["phantom_conversion"] = {
                "ok": False,
                "error": str(exc),
                "anomalies_detected": 0,
            }

        # ── Write anomalies to BigQuery ────────────────────────────────────────
        write_errors: list[dict] = []
        if all_anomalies:
            write_errors = self._write_anomaly_batch(all_anomalies)

        # ── Compute summary statistics ─────────────────────────────────────────
        total_anomalies = len(all_anomalies)
        total_pipeline_at_risk = sum(
            float(a.get("estimated_pipeline_value") or 0.0) for a in all_anomalies
        )

        # Cleanliness score: 100 − (weighted_severity_sum × 2), floored at 0
        # Each fully-confident phantom_conversion anomaly costs 2 points.
        # 50 mixed anomalies → score ≈ 50 (contaminated). 0 → 100 (clean).
        weighted_sum = sum(
            float(a.get("confidence_score") or 0.75)
            * _SEVERITY_WEIGHT.get(a.get("anomaly_type_enum", ""), 0.50)
            for a in all_anomalies
        )
        cleanliness_score = max(0.0, min(100.0, 100.0 - weighted_sum * 2.0))

        # Per-dimension breakdown for Markdown report
        by_type: dict[str, int] = {}
        by_platform: dict[str, int] = {}
        by_source: dict[str, int] = {}

        for a in all_anomalies:
            atype    = a.get("anomaly_type_enum", "unknown")
            platform = a.get("flagged_platform", "unknown")
            source   = a.get("crm_lead_source") or "no_crm_source"
            by_type[atype]      = by_type.get(atype, 0) + 1
            by_platform[platform] = by_platform.get(platform, 0) + 1
            by_source[source]   = by_source.get(source, 0) + 1

        log.info(
            "attribution_verifier.complete",
            run_id=self._run_id,
            total_anomalies=total_anomalies,
            cleanliness_score=round(cleanliness_score, 1),
            pipeline_at_risk=round(total_pipeline_at_risk, 0),
        )

        return {
            "ok":                      True,
            "run_id":                  self._run_id,
            "lookback_days":           lookback_days,
            "attribution_window_days": attribution_window_days,
            "anomaly_count":           total_anomalies,
            "anomalies_by_type":       by_type,
            "anomalies_by_platform":   by_platform,
            "anomalies_by_lead_source": by_source,
            "total_pipeline_at_risk":  round(total_pipeline_at_risk, 2),
            "cleanliness_score":       round(cleanliness_score, 1),
            "test_results":            test_results,
            "write_errors":            len(write_errors),
            "bq_table_written":        "data_attribution_anomalies",
        }

    # ── Test 1: Orphaned Token Test ────────────────────────────────────────────

    def _run_orphaned_token_test(
        self,
        lookback_days: int,
        offline_sources: frozenset[str],
    ) -> list[dict]:
        """
        Find CRM leads where a paid click token exists on the associated analytics
        session (via ga4_client_id join) but the CRM LeadSource is an offline label.

        Uses QUALIFY ROW_NUMBER() to deduplicate: one anomaly row per account_id,
        selecting the most recent session with a click token.

        Falls back gracefully if crm_leads_staging is missing lead_source.
        """
        offline_literals = ", ".join(
            f"'{s.replace(chr(39), chr(39)*2)}'" for s in sorted(offline_sources)
        )

        sql = f"""
        SELECT
            l.account_id                                AS crm_lead_id,
            CASE
                WHEN s.gclid     IS NOT NULL THEN 'google_ads'
                WHEN s.fbclid    IS NOT NULL THEN 'meta'
                WHEN s.msclkid   IS NOT NULL THEN 'microsoft_ads'
                WHEN s.ttclid    IS NOT NULL THEN 'tiktok'
                WHEN s.li_fat_id IS NOT NULL THEN 'linkedin'
                WHEN LOWER(s.utm_medium) IN ('cpc','paid','paidsocial','ppc') THEN 'utm_paid'
                ELSE 'unknown'
            END                                         AS flagged_platform,
            CASE
                WHEN s.gclid     IS NOT NULL THEN 'gclid'
                WHEN s.fbclid    IS NOT NULL THEN 'fbclid'
                WHEN s.msclkid   IS NOT NULL THEN 'msclkid'
                WHEN s.ttclid    IS NOT NULL THEN 'ttclid'
                WHEN s.li_fat_id IS NOT NULL THEN 'li_fat_id'
                WHEN LOWER(s.utm_medium) IN ('cpc','paid','paidsocial','ppc') THEN 'utm_paid'
                ELSE NULL
            END                                         AS token_type,
            LOWER(TRIM(l.lead_source))                  AS crm_lead_source,
            s.session_id,
            COALESCE(s.country, 'XX')                   AS geo_country_code,
            CAST(COALESCE(
                CAST(o.amount AS FLOAT64), 0.0
            ) AS FLOAT64)                               AS estimated_pipeline_value,
            DATE(s.session_start_at)                    AS anomaly_week
        FROM {bq.table_ref('crm_leads_staging')} l
        JOIN {bq.table_ref('sessions')} s
          ON s.ga4_client_id = l.ga_client_id
        LEFT JOIN {bq.table_ref('crm_opportunities_staging')} o
          ON LOWER(TRIM(o.account_id)) = LOWER(TRIM(l.account_id))
         AND o.is_closed = FALSE
        WHERE (
            s.gclid     IS NOT NULL
            OR s.fbclid    IS NOT NULL
            OR s.msclkid   IS NOT NULL
            OR s.ttclid    IS NOT NULL
            OR s.li_fat_id IS NOT NULL
            OR LOWER(s.utm_medium) IN ('cpc', 'paid', 'paidsocial', 'ppc')
        )
        AND LOWER(TRIM(l.lead_source)) IN ({offline_literals})
        AND DATE(s.session_start_at) >=
            DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        AND l.ga_client_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY l.account_id
            ORDER BY s.session_start_at DESC
        ) = 1
        """

        try:
            rows = bq.run_query(sql)
        except Exception as exc:
            err_lower = str(exc).lower()
            # If crm_leads_staging is missing lead_source, skip gracefully
            if "lead_source" in err_lower or "unrecognized name" in err_lower:
                log.warning(
                    "attribution_verifier.orphaned_token.no_lead_source",
                    note=(
                        "crm_leads_staging is missing the lead_source column. "
                        "Orphaned Token test skipped. "
                        "Add a LeadSource field to your CRM staging integration to enable this test."
                    ),
                )
                return []
            raise

        now = datetime.now(timezone.utc).isoformat()
        return [
            {
                "anomaly_id":               bq.new_uuid(),
                "run_id":                   self._run_id,
                "crm_lead_id":              row.get("crm_lead_id"),
                "flagged_platform":         row.get("flagged_platform", "unknown"),
                "anomaly_type_enum":        "orphaned_token",
                "token_type":               row.get("token_type"),
                "crm_lead_source":          row.get("crm_lead_source"),
                "claimed_channel":          row.get("crm_lead_source"),
                "expected_channel":         row.get("flagged_platform", "unknown"),
                "estimated_pipeline_value": str(round(float(row.get("estimated_pipeline_value") or 0.0), 4)),
                "confidence_score":         0.85,
                "session_id":               row.get("session_id"),
                "detection_method":         "sql_orphan_scan",
                "anomaly_week":             _safe_date_str(row.get("anomaly_week")),
                "geo_country_code":         row.get("geo_country_code", "XX"),
                "detected_timestamp":       now,
            }
            for row in rows
        ]

    # ── Test 2: Timestamp Divergence Test ──────────────────────────────────────

    def _run_timestamp_divergence_test(
        self,
        lookback_days: int,
        offline_sources: frozenset[str],
    ) -> list[dict]:
        """
        Find cases where a lead's CRM source was reassigned to an offline label
        AFTER the paid click token was already captured on the session.

        Probes candidate timestamp columns in crm_leads_staging in order:
        lead_source_updated_at → systemmodstamp → updated_at → last_modified_at.
        Skips gracefully if none are found (test result = 0 anomalies).

        Confidence scales with the overwrite lag: longer lag = higher confidence
        that the overwrite was intentional rather than a data pipeline artefact.
        """
        offline_literals = ", ".join(
            f"'{s.replace(chr(39), chr(39)*2)}'" for s in sorted(offline_sources)
        )

        rows: list[dict] | None = None

        for ts_col in _TS_COLUMN_CANDIDATES:
            sql = f"""
            SELECT
                l.account_id                                AS crm_lead_id,
                CASE
                    WHEN s.gclid    IS NOT NULL THEN 'google_ads'
                    WHEN s.fbclid   IS NOT NULL THEN 'meta'
                    WHEN s.msclkid  IS NOT NULL THEN 'microsoft_ads'
                    WHEN s.ttclid   IS NOT NULL THEN 'tiktok'
                    WHEN s.li_fat_id IS NOT NULL THEN 'linkedin'
                    ELSE 'unknown'
                END                                         AS flagged_platform,
                CASE
                    WHEN s.gclid    IS NOT NULL THEN 'gclid'
                    WHEN s.fbclid   IS NOT NULL THEN 'fbclid'
                    WHEN s.msclkid  IS NOT NULL THEN 'msclkid'
                    WHEN s.ttclid   IS NOT NULL THEN 'ttclid'
                    WHEN s.li_fat_id IS NOT NULL THEN 'li_fat_id'
                    ELSE NULL
                END                                         AS token_type,
                LOWER(TRIM(l.lead_source))                  AS crm_lead_source,
                s.session_id,
                s.session_start_at                          AS click_token_captured_at,
                l.{ts_col}                                  AS source_assigned_at,
                TIMESTAMP_DIFF(l.{ts_col}, s.session_start_at, HOUR)
                                                            AS hours_overwrite_lag,
                COALESCE(s.country, 'XX')                   AS geo_country_code,
                CAST(COALESCE(
                    CAST(o.amount AS FLOAT64), 0.0
                ) AS FLOAT64)                               AS estimated_pipeline_value,
                DATE(l.{ts_col})                            AS anomaly_week
            FROM {bq.table_ref('crm_leads_staging')} l
            JOIN {bq.table_ref('sessions')} s
              ON s.ga4_client_id = l.ga_client_id
            LEFT JOIN {bq.table_ref('crm_opportunities_staging')} o
              ON LOWER(TRIM(o.account_id)) = LOWER(TRIM(l.account_id))
             AND o.is_closed = FALSE
            WHERE (
                s.gclid    IS NOT NULL
                OR s.fbclid   IS NOT NULL
                OR s.msclkid  IS NOT NULL
                OR s.ttclid   IS NOT NULL
                OR s.li_fat_id IS NOT NULL
            )
            AND LOWER(TRIM(l.lead_source)) IN ({offline_literals})
            AND l.{ts_col} > s.session_start_at
            AND DATE(s.session_start_at) >=
                DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
            AND l.ga_client_id IS NOT NULL
            AND l.{ts_col} IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY l.account_id
                ORDER BY TIMESTAMP_DIFF(l.{ts_col}, s.session_start_at, HOUR) DESC
            ) = 1
            """
            try:
                rows = bq.run_query(sql)
                log.info(
                    "attribution_verifier.timestamp_divergence.ts_col_found",
                    column=ts_col,
                    rows_found=len(rows),
                )
                break  # found a working column
            except Exception as exc:
                err_lower = str(exc).lower()
                if (
                    ts_col.lower() in err_lower
                    or "unrecognized name" in err_lower
                    or "not found" in err_lower
                    or "invalid field" in err_lower
                ):
                    log.info(
                        "attribution_verifier.timestamp_divergence.column_not_found",
                        tried=ts_col,
                    )
                    rows = None
                    continue
                raise  # unexpected error — re-raise

        if rows is None:
            log.warning(
                "attribution_verifier.timestamp_divergence.no_ts_column",
                note=(
                    "No modification timestamp column found in crm_leads_staging. "
                    "Timestamp Divergence test skipped. "
                    "Add systemmodstamp (Salesforce) or updated_at to the CRM staging "
                    "integration to enable this test."
                ),
            )
            return []

        now = datetime.now(timezone.utc).isoformat()
        return [
            {
                "anomaly_id":               bq.new_uuid(),
                "run_id":                   self._run_id,
                "crm_lead_id":              row.get("crm_lead_id"),
                "flagged_platform":         row.get("flagged_platform", "unknown"),
                "anomaly_type_enum":        "timestamp_divergence",
                "token_type":               row.get("token_type"),
                "crm_lead_source":          row.get("crm_lead_source"),
                "claimed_channel":          row.get("crm_lead_source"),
                "expected_channel":         row.get("flagged_platform", "unknown"),
                "estimated_pipeline_value": str(round(float(row.get("estimated_pipeline_value") or 0.0), 4)),
                # Confidence scales with overwrite lag: +0.01 per ~72-hour lag, capped at 0.95
                "confidence_score":         min(
                    0.95,
                    0.70 + min(float(row.get("hours_overwrite_lag") or 0), 720) / 7200.0,
                ),
                "session_id":               row.get("session_id"),
                "detection_method":         "timestamp_compare",
                "anomaly_week":             _safe_date_str(row.get("anomaly_week")),
                "geo_country_code":         row.get("geo_country_code", "XX"),
                "detected_timestamp":       now,
            }
            for row in rows
        ]

    # ── Test 3: Phantom Conversion Test ───────────────────────────────────────

    def _run_phantom_conversion_test(
        self,
        lookback_days: int,
        attribution_window_days: int,
    ) -> list[dict]:
        """
        Cross-reference platform_daily_spend (platform-reported conversions) against
        conversion_events (CRM-matched conversions) per platform per day.

        Flags platform/date combinations where the ad network claims significantly
        more conversions than the CRM holds within a rolling attribution window.
        A grace tolerance of ``attribution_window_days`` is subtracted from the gap
        to account for CRM data latency (leads may take days to be created after
        an ad click converts).

        Pipeline value at risk is estimated from the average deal value of CRM-matched
        conversions for the same platform, times the phantom conversion count.
        Returns at most 500 anomaly rows (ordered by gap descending) to bound cost.
        """
        sql = f"""
        WITH platform_daily AS (
            SELECT
                c.platform,
                COALESCE(s.geo_country_code, 'XX')              AS geo_country_code,
                s.date,
                SUM(CAST(s.platform_conversions AS FLOAT64))    AS platform_conversions
            FROM {bq.table_ref('platform_daily_spend')} s
            LEFT JOIN {bq.table_ref('platform_campaigns')} c
                   ON s.campaign_id = c.campaign_id
            WHERE s.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
              AND c.platform IS NOT NULL
              AND s.platform_conversions > 0
            GROUP BY 1, 2, 3
        ),
        crm_daily AS (
            SELECT
                platform_attributed_to                              AS platform,
                DATE(converted_at)                                  AS conv_date,
                COUNT(*)                                            AS crm_conversions,
                SUM(COALESCE(CAST(deal_value AS FLOAT64), 0.0))    AS crm_pipeline_value
            FROM {bq.table_ref('conversion_events')}
            WHERE DATE(converted_at) >= DATE_SUB(
                      CURRENT_DATE(),
                      INTERVAL {lookback_days + attribution_window_days} DAY
                  )
              AND platform_attributed_to IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT
            p.platform,
            p.geo_country_code,
            p.date                                          AS anomaly_date,
            p.platform_conversions                          AS reported_by_platform,
            COALESCE(c.crm_conversions, 0)                 AS matched_in_crm,
            COALESCE(c.crm_pipeline_value, 0.0)            AS crm_pipeline_value,
            GREATEST(0.0,
                p.platform_conversions
                - COALESCE(c.crm_conversions, 0)
                - {attribution_window_days}
            )                                               AS phantom_gap
        FROM platform_daily p
        LEFT JOIN crm_daily c
               ON c.platform = p.platform
              AND c.conv_date BETWEEN
                  DATE_SUB(p.date, INTERVAL {attribution_window_days} DAY)
                  AND DATE_ADD(p.date, INTERVAL {attribution_window_days} DAY)
        WHERE GREATEST(0.0,
                  p.platform_conversions
                  - COALESCE(c.crm_conversions, 0)
                  - {attribution_window_days}
              ) > 0
        ORDER BY phantom_gap DESC
        LIMIT 500
        """

        try:
            rows = bq.run_query(sql)
        except Exception as exc:
            log.warning("attribution_verifier.phantom_test_query_failed", error=str(exc))
            return []

        now = datetime.now(timezone.utc).isoformat()
        anomalies: list[dict] = []

        for row in rows:
            gap = float(row.get("phantom_gap") or 0.0)
            if gap <= 0:
                continue

            platform     = row.get("platform", "unknown")
            crm_matched  = max(float(row.get("matched_in_crm") or 1.0), 1.0)
            crm_pipeline = float(row.get("crm_pipeline_value") or 0.0)

            # Confidence: larger phantom gap = higher confidence of real anomaly
            # 60% baseline; +0.1 per 5-unit gap; capped at 0.95
            confidence = min(0.95, 0.60 + min(gap, 50.0) / 100.0)

            # Pipeline at risk: avg deal value × phantom count
            avg_deal = crm_pipeline / crm_matched
            pipeline_at_risk = gap * avg_deal

            anomaly_date = row.get("anomaly_date")
            anomalies.append({
                "anomaly_id":               bq.new_uuid(),
                "run_id":                   self._run_id,
                "crm_lead_id":              None,  # no CRM lead for phantom conversions
                "flagged_platform":         platform,
                "anomaly_type_enum":        "phantom_conversion",
                "token_type":               None,
                "crm_lead_source":          None,
                "claimed_channel":          platform,
                "expected_channel":         None,
                "estimated_pipeline_value": str(round(pipeline_at_risk, 4)),
                "confidence_score":         confidence,
                "session_id":               None,
                "detection_method":         "pixel_crm_mismatch",
                "anomaly_week":             _safe_date_str(anomaly_date),
                "geo_country_code":         row.get("geo_country_code", "XX"),
                "detected_timestamp":       now,
            })

        return anomalies

    # ── BigQuery write ─────────────────────────────────────────────────────────

    def _write_anomaly_batch(self, anomalies: list[dict]) -> list[dict]:
        """
        Stream all anomaly rows to data_attribution_anomalies in BigQuery.
        Batched at _BQ_BATCH_SIZE rows to respect API limits.
        Returns any insertion errors (empty list = success).
        """
        if not anomalies:
            return []

        all_errors: list[dict] = []
        for i in range(0, len(anomalies), _BQ_BATCH_SIZE):
            batch = anomalies[i : i + _BQ_BATCH_SIZE]
            errs = bq.insert_rows("data_attribution_anomalies", batch)
            all_errors.extend(errs)

        if all_errors:
            log.warning(
                "attribution_verifier.write_partial_errors",
                total_anomalies=len(anomalies),
                error_count=len(all_errors),
            )
        else:
            log.info(
                "attribution_verifier.anomalies_written",
                total=len(anomalies),
            )

        return all_errors


# ── Module-level helper ────────────────────────────────────────────────────────


def _safe_date_str(val: Any) -> str:
    """
    Convert a BQ DATE value (python date, datetime, or string) to ISO 'YYYY-MM-DD'.
    Falls back to today's date string if the value is None or unparseable.
    """
    if val is None:
        return date.today().isoformat()
    if hasattr(val, "isoformat"):
        return val.isoformat()[:10]
    s = str(val)
    return s[:10] if len(s) >= 10 else date.today().isoformat()
