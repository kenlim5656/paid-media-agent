"""
Analytics writer — database-agnostic write interface for account-based analytics.

Defines the AnalyticsWriter Protocol so the enrichment pipeline is not
hard-wired to BigQuery. When Task 23 (Snowflake / Redshift adapters) is
implemented, create a new class that satisfies this Protocol.

Current implementations:
  BigQueryAnalyticsWriter  — writes to paid-media-schema BigQuery tables
                             (company_profiles, company_sessions, company_engagement,
                              target_account_activity, ip_resolution_cache)

Usage:
    from tools.analytics_writer import BigQueryAnalyticsWriter
    writer = BigQueryAnalyticsWriter()
    writer.upsert_company_profile(row)
    writer.insert_company_session(row)
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Protocol, runtime_checkable

import structlog

from tools import bigquery_client as bq
from tools.ip_intelligence_client import CompanyResolution, CacheReader
from config import settings

log = structlog.get_logger()


# ── Protocol definition (database-agnostic interface) ─────────────────────────

@runtime_checkable
class AnalyticsWriter(Protocol):
    """
    Write interface for account-based analytics tables.
    Any class that implements these methods can replace BigQueryAnalyticsWriter.
    """

    def upsert_company_profile(self, row: dict) -> int:
        """MERGE a company profile row. Returns affected row count."""
        ...

    def insert_company_session(self, row: dict) -> None:
        """Stream-insert a de-anonymized session row."""
        ...

    def upsert_company_engagement(self, row: dict) -> int:
        """MERGE a company engagement summary row. Returns affected row count."""
        ...

    def insert_target_account_activity(self, row: dict) -> None:
        """Stream-insert a daily target account snapshot."""
        ...


# ── BigQuery cache reader/writer ───────────────────────────────────────────────

class BigQueryCacheReader(CacheReader):
    """
    Implements ip_resolution_cache reads/writes using BigQuery.
    Stores /24 prefix → company resolution with TTL.
    """

    def read(self, prefix: str) -> dict | None:
        """
        Return the most recent non-expired cache row for this /24 prefix.
        Returns None on cache miss or expiry.
        """
        try:
            rows = bq.run_query(
                f"""
                SELECT *
                FROM {bq.table_ref('ip_resolution_cache')}
                WHERE ip_prefix = @prefix
                  AND expires_at > CURRENT_TIMESTAMP()
                ORDER BY resolved_at DESC
                LIMIT 1
                """,
                {"prefix": prefix},
            )
            return rows[0] if rows else None
        except Exception as exc:
            log.warning("cache.read_error", prefix=prefix, error=str(exc))
            return None

    def write(self, prefix: str, result: CompanyResolution) -> None:
        """
        Write a new cache entry. Uses the /24 prefix — never the full IP.
        TTL is set to ip_resolution_cache_ttl_hours from settings.
        """
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=settings.ip_resolution_cache_ttl_hours)
        row = {
            "cache_id":               bq.new_uuid(),
            "ip_prefix":              prefix,
            "network_prefix_bits":    24,
            "resolved_company_domain": result.company_domain,
            "resolved_company_name":  result.company_name,
            "resolution_type":        result.resolution_type,
            "resolution_confidence":  str(result.confidence),   # NUMERIC as string for streaming
            "resolution_provider":    result.provider,
            "provider_response_ms":   result.provider_response_ms,
            "country_code":           result.country_code,
            "country_name":           result.country_name,
            "region":                 result.region,
            "resolved_at":            now.isoformat(),
            "expires_at":             expires.isoformat(),
            "hit_count":              0,
            "last_hit_at":            None,
            "is_vpn":                 result.is_vpn,
            "is_datacenter":          result.is_datacenter,
            "is_residential":         result.is_residential,
            "is_bot_suspected":       result.is_bot_suspected,
            "should_exclude_from_analytics": result.should_exclude,
        }
        errors = bq.insert_rows("ip_resolution_cache", [row])
        if errors:
            log.warning("cache.write_error", prefix=prefix, errors=errors)

    def increment_hit(self, prefix: str) -> None:
        """Increment hit_count for the most recent cache entry for this prefix."""
        try:
            bq.run_dml(
                f"""
                UPDATE {bq.table_ref('ip_resolution_cache')}
                SET hit_count = hit_count + 1,
                    last_hit_at = CURRENT_TIMESTAMP()
                WHERE ip_prefix = @prefix
                  AND expires_at > CURRENT_TIMESTAMP()
                """.replace("@prefix", f"'{prefix}'")
            )
        except Exception as exc:
            log.debug("cache.increment_error", prefix=prefix, error=str(exc))


# ── BigQuery analytics writer ─────────────────────────────────────────────────

class BigQueryAnalyticsWriter:
    """
    Implements AnalyticsWriter using BigQuery streaming inserts and MERGE DML.
    Satisfies the AnalyticsWriter Protocol — swap this class for a SQLAlchemy
    implementation in Task 23 to support Snowflake / Redshift.
    """

    def upsert_company_profile(self, row: dict) -> int:
        """
        MERGE company_profiles: insert new company or update firmographics.
        Preserves is_target_account, account_tier, and CRM fields set by humans.
        """
        domain = row.get("company_domain", "")
        if not domain:
            return 0

        now_str = datetime.now(timezone.utc).isoformat()

        # Build SET clause for fields we always refresh from enrichment provider
        set_pairs = {
            "company_name":         _sql_str(row.get("company_name")),
            "enrichment_source":    _sql_str(row.get("enrichment_source")),
            "enrichment_confidence": _sql_num(row.get("enrichment_confidence")),
            "enrichment_method":    _sql_str(row.get("enrichment_method")),
            "last_enriched_at":     f"TIMESTAMP '{now_str}'",
            "last_seen_at":         f"TIMESTAMP '{now_str}'",
            "total_session_count":  "target.total_session_count + 1",
            "updated_at":           f"TIMESTAMP '{now_str}'",
        }
        # Optional enrichment fields (only update when provider returns them)
        optional = [
            "industry", "industry_group", "sub_industry",
            "headquarters_country", "headquarters_country_name",
            "headquarters_state", "headquarters_city",
            "enrichment_provider_id",
        ]
        for k in optional:
            if row.get(k) is not None:
                set_pairs[k] = _sql_str(row[k])

        set_clause = ",\n            ".join(f"{k} = {v}" for k, v in set_pairs.items())

        # INSERT values for new companies
        company_id = row.get("company_id") or bq.new_uuid()

        sql = f"""
        MERGE {bq.table_ref('company_profiles')} AS target
        USING (
            SELECT
                '{company_id}' AS company_id,
                '{domain}'     AS company_domain
        ) AS source
        ON target.company_domain = source.company_domain
           AND target.is_active = TRUE
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN INSERT (
            company_id, company_domain, company_name,
            enrichment_source, enrichment_confidence, enrichment_method,
            last_enriched_at, first_seen_at, last_seen_at,
            total_session_count, is_active, created_at, updated_at
        ) VALUES (
            source.company_id, source.company_domain,
            {_sql_str(row.get('company_name'))},
            {_sql_str(row.get('enrichment_source'))},
            {_sql_num(row.get('enrichment_confidence'))},
            {_sql_str(row.get('enrichment_method'))},
            TIMESTAMP '{now_str}',
            TIMESTAMP '{now_str}',
            TIMESTAMP '{now_str}',
            1, TRUE,
            TIMESTAMP '{now_str}',
            TIMESTAMP '{now_str}'
        )
        """
        return bq.run_dml(sql)

    def insert_company_session(self, row: dict) -> None:
        """Stream-insert a de-anonymized session into company_sessions."""
        errors = bq.insert_rows("company_sessions", [row])
        if errors:
            log.warning(
                "writer.company_session_error",
                session_id=row.get("session_id"),
                errors=errors,
            )

    def upsert_company_engagement(self, row: dict) -> int:
        """
        MERGE company_engagement: rebuild the rolling-30d summary for a company.
        The Analyst agent calls this at the end of each enrichment batch.
        """
        domain = row.get("company_domain", "")
        period_type = row.get("period_type", "rolling_30d")
        period_start = row.get("period_start", "")
        if not domain or not period_start:
            return 0

        now_str = datetime.now(timezone.utc).isoformat()

        sql = f"""
        MERGE {bq.table_ref('company_engagement')} AS target
        USING (
            SELECT
                '{domain}'      AS company_domain,
                '{period_type}' AS period_type,
                DATE '{period_start}' AS period_start
        ) AS source
        ON target.company_domain = source.company_domain
           AND target.period_type = source.period_type
           AND target.period_start = source.period_start
        WHEN MATCHED THEN UPDATE SET
            total_sessions       = {row.get('total_sessions', 0)},
            total_page_views     = {row.get('total_page_views', 0)},
            unique_session_days  = {row.get('unique_session_days', 0)},
            pricing_page_sessions = {row.get('pricing_page_sessions', 0)},
            demo_page_sessions   = {row.get('demo_page_sessions', 0)},
            contact_page_sessions = {row.get('contact_page_sessions', 0)},
            paid_sessions        = {row.get('paid_sessions', 0)},
            intent_score         = {_sql_num(row.get('intent_score'))},
            generated_at         = TIMESTAMP '{now_str}',
            generated_by         = 'analyst_agent'
        WHEN NOT MATCHED THEN INSERT (
            engagement_id, company_id, company_domain, company_name,
            period_type, period_start, period_end,
            total_sessions, total_page_views, unique_session_days,
            pricing_page_sessions, demo_page_sessions, contact_page_sessions,
            paid_sessions, intent_score, generated_at, generated_by
        ) VALUES (
            GENERATE_UUID(),
            {_sql_str(row.get('company_id'))},
            '{domain}',
            {_sql_str(row.get('company_name'))},
            '{period_type}',
            DATE '{period_start}',
            DATE '{row.get("period_end", period_start)}',
            {row.get('total_sessions', 0)},
            {row.get('total_page_views', 0)},
            {row.get('unique_session_days', 0)},
            {row.get('pricing_page_sessions', 0)},
            {row.get('demo_page_sessions', 0)},
            {row.get('contact_page_sessions', 0)},
            {row.get('paid_sessions', 0)},
            {_sql_num(row.get('intent_score'))},
            TIMESTAMP '{now_str}',
            'analyst_agent'
        )
        """
        return bq.run_dml(sql)

    def insert_target_account_activity(self, row: dict) -> None:
        """Stream-insert a daily target account activity snapshot."""
        errors = bq.insert_rows("target_account_activity", [row])
        if errors:
            log.warning(
                "writer.taa_error",
                company_domain=row.get("company_domain"),
                errors=errors,
            )


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _sql_str(v: str | None) -> str:
    """Safely quote a string value for inline SQL. Returns NULL for None."""
    if v is None:
        return "NULL"
    escaped = str(v).replace("'", "''")
    return f"'{escaped}'"


def _sql_num(v: float | None) -> str:
    """Format a numeric value for inline SQL. Returns NULL for None."""
    if v is None:
        return "NULL"
    return str(float(v))
