# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Session enrichment job for the Analyst agent.

Resolves anonymous web sessions to company domains using IP intelligence,
then writes de-anonymized records to the account-based analytics tables:
  • ip_resolution_cache    — /24 prefix → company (privacy-safe cache)
  • company_profiles       — enriched firmographic record per domain
  • company_sessions       — de-anonymized session with company context
  • company_engagement     — aggregated 30-day engagement summary

PRIVACY INVARIANT: The raw IP address is read from sgtm_request_logs,
passed directly to IPIntelligenceClient.resolve(), and immediately discarded.
Only the /24 prefix and the resolved company_domain are ever persisted.

Enrichment run sequence (called by AnalystAgent.enrich_sessions tool):
  1. Query sgtm_request_logs for recent sessions not yet in company_sessions
  2. For each session: resolve IP → CompanyResolution
  3. Skip if excluded (VPN / datacenter / residential / bot / low confidence)
  4. Write cache entry (/24 prefix → domain) via BigQueryCacheReader
  5. Upsert company_profiles (MERGE — safe for concurrent runs)
  6. Insert company_sessions row
  7. Recompute rolling-30d company_engagement for all enriched domains
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import structlog

from config import settings
from tools import bigquery_client as bq
from tools.analytics_writer import (
    BigQueryAnalyticsWriter,
    BigQueryCacheReader,
)
from tools.ip_intelligence_client import (
    CompanyResolution,
    IPIntelligenceClient,
    build_provider,
)

if TYPE_CHECKING:
    pass

log = structlog.get_logger()


# ── Main enrichment job ───────────────────────────────────────────────────────

class EnrichmentJob:
    """
    Orchestrates the IP → company enrichment pipeline.

    Designed for use by the Analyst agent as a tool, and also callable
    directly from the orchestrator runner for testing.
    """

    def __init__(self) -> None:
        cache_reader = BigQueryCacheReader()
        provider = build_provider()
        self._client = IPIntelligenceClient(provider, cache_reader)
        self._writer = BigQueryAnalyticsWriter()

    def run(
        self,
        lookback_hours: int | None = None,
        batch_size: int | None = None,
    ) -> dict:
        """
        Main enrichment entry point. Fetches unenriched sessions, resolves
        each to a company, and writes results to the analytics tables.

        Args:
            lookback_hours: How far back to look for unenriched sessions.
                            Defaults to settings.ip_enrichment_lookback_hours (48h).
            batch_size:     Maximum sessions to process per run.
                            Defaults to settings.ip_enrichment_batch_size (1000).

        Returns:
            Summary dict suitable for returning from the Analyst agent tool.
        """
        hours = lookback_hours or settings.ip_enrichment_lookback_hours
        batch = batch_size or settings.ip_enrichment_batch_size

        log.info("enrichment.start", lookback_hours=hours, batch_size=batch)

        # Step 1: Fetch unenriched sessions (includes raw IP — read-only, never persisted)
        sessions = self._fetch_unenriched_sessions(hours, batch)
        if not sessions:
            log.info("enrichment.no_sessions")
            return {"sessions_found": 0, "sessions_enriched": 0, "domains_resolved": 0}

        log.info("enrichment.sessions_fetched", count=len(sessions))

        # Step 2: Resolve and write
        enriched: list[str] = []   # session_ids
        domains: set[str] = set()  # unique domains for engagement aggregation

        for session in sessions:
            domain = self._enrich_session(session)
            if domain:
                enriched.append(session["session_id"])
                domains.add(domain)

        # Step 3: Recompute rolling-30d engagement for all newly enriched domains
        engagement_rows = 0
        for domain in domains:
            try:
                rows = self._aggregate_engagement(domain)
                engagement_rows += rows
            except Exception as exc:
                log.warning("enrichment.engagement_error", domain=domain, error=str(exc))

        log.info(
            "enrichment.complete",
            sessions_found=len(sessions),
            sessions_enriched=len(enriched),
            domains_resolved=len(domains),
            engagement_rows=engagement_rows,
        )

        return {
            "sessions_found":      len(sessions),
            "sessions_enriched":   len(enriched),
            "domains_resolved":    len(domains),
            "engagement_rows":     engagement_rows,
            "sample_domains":      sorted(domains)[:10],  # first 10 for logging
        }

    def _fetch_unenriched_sessions(self, hours: int, limit: int) -> list[dict]:
        """
        Query sGTM request logs for sessions that haven't been enriched yet.

        The raw IP address field (ip_address) is selected here and passed
        through the enrichment pipeline — it is NEVER written to any table.
        It exists only as a Python string within this function's execution.
        """
        try:
            return bq.run_query(
                f"""
                SELECT
                    l.session_id,
                    l.ip_address,          -- raw IP: read-only, never persisted
                    l.captured_at,
                    l.ga4_client_id,
                    l.gclid,
                    l.fbclid,
                    l.li_fat_id,
                    l.ttclid,
                    l.utm_source,
                    l.utm_medium,
                    l.utm_campaign,
                    l.landing_page,
                    l.page_count,
                    l.channel_grouping
                FROM {bq.table_ref('sgtm_request_logs')} l
                WHERE l.captured_at >= TIMESTAMP_SUB(
                    CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR
                )
                  AND l.ip_address IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {bq.table_ref('company_sessions')} cs
                    WHERE cs.session_id = l.session_id
                  )
                ORDER BY l.captured_at DESC
                LIMIT {limit}
                """
            )
        except Exception as exc:
            log.error("enrichment.fetch_error", error=str(exc))
            return []

    def _enrich_session(self, session: dict) -> str | None:
        """
        Resolve a single session to a company domain and write all output records.
        Returns the resolved company_domain on success, None on failure/exclusion.

        The ip_address field is extracted from the session dict, used for
        resolution, and then removed before any writes.
        """
        raw_ip: str | None = session.get("ip_address")
        session_id: str = session.get("session_id", "")

        if not raw_ip or not session_id:
            return None

        # Resolve IP → company (raw IP only used here, immediately dropped)
        resolution: CompanyResolution | None = self._client.resolve(raw_ip)

        if resolution is None:
            # Excluded or unresolvable (VPN / residential / low confidence)
            return None

        domain = resolution.company_domain
        if not domain:
            return None

        # Look up or create company profile
        company_id = self._get_or_create_company_id(domain, resolution)

        # Write company_sessions record
        self._writer.insert_company_session(
            _build_company_session_row(session, company_id, domain, resolution)
        )

        # Upsert company_profiles (MERGE is idempotent)
        self._writer.upsert_company_profile(
            _build_company_profile_row(company_id, domain, resolution)
        )

        return domain

    def _get_or_create_company_id(self, domain: str, resolution: CompanyResolution) -> str:
        """
        Look up an existing company_id for this domain, or generate a new UUID.
        Uses a cheap SELECT before we attempt the MERGE write.
        """
        try:
            rows = bq.run_query(
                f"""
                SELECT company_id
                FROM {bq.table_ref('company_profiles')}
                WHERE company_domain = @domain
                  AND is_active = TRUE
                LIMIT 1
                """,
                params={"domain": domain},
            )
            if rows:
                return str(rows[0]["company_id"])
        except Exception:
            pass
        return bq.new_uuid()

    def _aggregate_engagement(self, domain: str) -> int:
        """
        Recompute the rolling-30d company_engagement row for a domain
        from the company_sessions table.
        """
        today = date.today()
        start_30d = (today - timedelta(days=30)).isoformat()

        rows = bq.run_query(
            f"""
            SELECT
                cp.company_id,
                cp.company_name,
                COUNT(*)                        AS total_sessions,
                SUM(cs.page_count)              AS total_page_views,
                COUNT(DISTINCT cs.session_date) AS unique_session_days,
                COUNTIF(cs.visited_pricing)     AS pricing_page_sessions,
                COUNTIF(cs.visited_demo)        AS demo_page_sessions,
                COUNTIF(cs.visited_contact)     AS contact_page_sessions,
                COUNTIF(cs.has_paid_touchpoint) AS paid_sessions
            FROM {bq.table_ref('company_sessions')} cs
            JOIN {bq.table_ref('company_profiles')} cp
              ON cp.company_domain = cs.company_domain AND cp.is_active = TRUE
            WHERE cs.company_domain = @domain
              AND cs.session_date >= DATE(@start_30d)
            GROUP BY cp.company_id, cp.company_name
            """,
            params={"domain": domain, "start_30d": start_30d},
        )

        if not rows:
            return 0

        r = rows[0]
        total_sessions = int(r.get("total_sessions", 0) or 0)
        pricing = int(r.get("pricing_page_sessions", 0) or 0)
        demo    = int(r.get("demo_page_sessions", 0) or 0)
        contact = int(r.get("contact_page_sessions", 0) or 0)
        paid    = int(r.get("paid_sessions", 0) or 0)

        # Simple intent scoring: recency (50%) + depth (30%) + content (20%)
        # Recency component — scale 0–50 based on total_sessions in period
        recency_score  = min(50.0, total_sessions * 5.0)
        # Depth component — key page visits signal intent
        depth_score    = min(30.0, (pricing * 10 + demo * 8 + contact * 6))
        # Paid component — paid-media driven sessions add confidence
        content_score  = min(20.0, paid * 2.0)
        intent_score   = round(recency_score + depth_score + content_score, 1)

        engagement_row = {
            "company_id":           str(r.get("company_id", "")),
            "company_domain":       domain,
            "company_name":         r.get("company_name"),
            "period_type":          "rolling_30d",
            "period_start":         start_30d,
            "period_end":           today.isoformat(),
            "total_sessions":       total_sessions,
            "total_page_views":     int(r.get("total_page_views", 0) or 0),
            "unique_session_days":  int(r.get("unique_session_days", 0) or 0),
            "pricing_page_sessions": pricing,
            "demo_page_sessions":   demo,
            "contact_page_sessions": contact,
            "paid_sessions":        paid,
            "intent_score":         intent_score,
        }

        return self._writer.upsert_company_engagement(engagement_row)

    def handle_rb2b_webhook(self, payload: dict) -> dict:
        """
        Handle a push notification from RB2B.
        RB2B identifies visitors and pushes company info to a webhook URL.
        This method processes the payload and writes it as a company_session.

        Expected payload fields (RB2B webhook format):
          session_id, domain, company_name, page_url, timestamp
        """
        domain: str | None = payload.get("domain")
        session_id: str | None = payload.get("session_id") or payload.get("visit_id")

        if not domain or not session_id:
            log.warning("enrichment.rb2b_invalid_payload", payload_keys=list(payload.keys()))
            return {"error": "Missing required fields: domain, session_id"}

        # Check if already enriched
        existing = bq.run_query(
            f"""
            SELECT 1 FROM {bq.table_ref('company_sessions')}
            WHERE session_id = '{session_id}' LIMIT 1
            """
        )
        if existing:
            return {"status": "already_enriched", "session_id": session_id}

        company_id = self._get_or_create_company_id(domain, CompanyResolution(
            resolution_type="corporate",
            confidence=0.85,    # RB2B identification is high confidence
            provider="rb2b",
            company_domain=domain,
            company_name=payload.get("company_name"),
        ))

        resolution = CompanyResolution(
            resolution_type="corporate",
            confidence=0.85,
            provider="rb2b",
            company_domain=domain,
            company_name=payload.get("company_name"),
        )

        # Build a minimal session row from the webhook payload
        session_stub = {
            "session_id":      session_id,
            "captured_at":     payload.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "landing_page":    payload.get("page_url", ""),
            "page_count":      1,
            "channel_grouping": None,
            "utm_source":      payload.get("utm_source"),
            "utm_medium":      payload.get("utm_medium"),
            "utm_campaign":    payload.get("utm_campaign"),
        }

        self._writer.insert_company_session(
            _build_company_session_row(session_stub, company_id, domain, resolution)
        )
        self._writer.upsert_company_profile(
            _build_company_profile_row(company_id, domain, resolution)
        )

        log.info("enrichment.rb2b_processed", domain=domain, session_id=session_id)
        return {"status": "enriched", "session_id": session_id, "domain": domain}


# ── Row builders ──────────────────────────────────────────────────────────────

def _build_company_session_row(
    session: dict,
    company_id: str,
    domain: str,
    resolution: CompanyResolution,
) -> dict:
    """
    Build a company_sessions row from a sGTM log session + resolution result.
    Does NOT include the raw IP address in any field.
    """
    captured_at = session.get("captured_at", datetime.now(timezone.utc).isoformat())
    if hasattr(captured_at, "isoformat"):
        session_date = captured_at.date().isoformat()
        session_start_at = captured_at.isoformat()
    else:
        ts_str = str(captured_at)
        session_date = ts_str[:10]
        session_start_at = ts_str

    landing = str(session.get("landing_page") or "")

    return {
        "company_session_id":       bq.new_uuid(),
        "session_id":               session.get("session_id", ""),
        "company_id":               company_id,
        "company_domain":           domain,
        "company_name":             resolution.company_name,
        "resolution_method":        "ip_intelligence",
        "resolution_confidence":    str(resolution.confidence),  # NUMERIC as string
        "resolution_provider":      resolution.provider,
        # /24 prefix from the session — NOT the full IP
        # We don't have the prefix here because it's encapsulated inside the client.
        # The ip_resolution_cache already has it; we leave this null on the session.
        "ip_prefix":                None,
        # Session context
        "session_date":             session_date,
        "session_start_at":         session_start_at,
        "session_duration_seconds": session.get("session_duration_seconds"),
        "page_count":               session.get("page_count"),
        "channel_grouping":         session.get("channel_grouping"),
        "entry_url":                session.get("entry_url"),
        "landing_page":             landing,
        "utm_source":               session.get("utm_source"),
        "utm_medium":               session.get("utm_medium"),
        "utm_campaign":             session.get("utm_campaign"),
        # Key page flags — infer from landing page when not explicitly set
        "visited_pricing":          _page_matches(landing, ["/pricing", "/plans"]),
        "visited_demo":             _page_matches(landing, ["/demo", "/request-demo", "/book-demo"]),
        "visited_contact":          _page_matches(landing, ["/contact", "/talk-to-sales", "/get-started"]),
        "visited_docs":             _page_matches(landing, ["/docs", "/documentation", "/api"]),
        "visited_case_study":       _page_matches(landing, ["/case-study", "/customers", "/success"]),
        "visited_blog":             _page_matches(landing, ["/blog", "/resources", "/insights"]),
        "visited_careers":          _page_matches(landing, ["/careers", "/jobs"]),
        "visited_login":            _page_matches(landing, ["/login", "/sign-in", "/signin", "/app"]),
        # Paid media context — detect from click IDs in session
        "has_paid_touchpoint":      bool(
            session.get("gclid") or session.get("fbclid") or
            session.get("li_fat_id") or session.get("ttclid")
        ),
        "paid_touchpoint_platform": _detect_platform(session),
        "paid_touchpoint_campaign_id": None,
        "paid_click_id_namespace":  _detect_namespace(session),
        "paid_click_id_value":      (
            session.get("gclid") or session.get("fbclid") or
            session.get("li_fat_id") or session.get("ttclid")
        ),
        # CRM context — not available at enrichment time; Analyst fills this later
        "crm_account_id":           None,
        "crm_pipeline_stage":       None,
        "crm_is_open_opportunity":  None,
        "is_target_account":        None,   # filled by company_profiles MERGE
        "account_tier":             None,
        "entity_id":                None,   # filled by identity stitching
        "resolved_at":              datetime.now(timezone.utc).isoformat(),
        "enriched_by":              "analyst_agent",
    }


def _build_company_profile_row(
    company_id: str,
    domain: str,
    resolution: CompanyResolution,
) -> dict:
    """Build a company_profiles row from a CompanyResolution."""
    raw = resolution.raw_provider_data
    return {
        "company_id":               company_id,
        "company_domain":           domain,
        "company_name":             resolution.company_name or domain,
        "headquarters_country":     resolution.country_code,
        "headquarters_country_name": resolution.country_name,
        "headquarters_state":       resolution.region,
        # Provider-supplied firmographics (Clearbit only; ipinfo may return None)
        "industry":                 raw.get("industry"),
        "employee_count":           raw.get("employee_count"),
        "company_type":             raw.get("type"),
        # Enrichment metadata
        "enrichment_source":        resolution.provider,
        "enrichment_confidence":    str(resolution.confidence),
        "enrichment_method":        "ip_intelligence",
        "enrichment_provider_id":   raw.get("provider_id"),
    }


# ── Page classification helpers ───────────────────────────────────────────────

def _page_matches(url: str, patterns: list[str]) -> bool:
    """Return True if the URL contains any of the given path fragments."""
    lower = url.lower()
    return any(p in lower for p in patterns)


def _detect_platform(session: dict) -> str | None:
    """Identify the paid platform from click ID presence."""
    if session.get("gclid"):
        return "google_ads"
    if session.get("fbclid"):
        return "meta"
    if session.get("li_fat_id"):
        return "linkedin"
    if session.get("ttclid"):
        return "tiktok"
    return None


def _detect_namespace(session: dict) -> str | None:
    """Return the identity namespace for the first click ID found."""
    if session.get("gclid"):
        return "platform_click_id.google.gclid"
    if session.get("fbclid"):
        return "platform_click_id.meta.fbclid"
    if session.get("li_fat_id"):
        return "platform_click_id.linkedin.li_fat_id"
    if session.get("ttclid"):
        return "platform_click_id.tiktok.ttclid"
    return None
