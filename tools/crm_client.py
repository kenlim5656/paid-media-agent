# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
CRM email lookup for domain-based audience suppression (Task 22).

Queries crm_leads_staging to resolve company domains → raw email addresses,
which push_domain_suppression() in tiktok_ads_client.py and google_ads_client.py
then hash (SHA-256) and upload as Customer Match / Custom Audience exclusion lists.

Privacy contract (strictly enforced):
  - Raw email addresses are returned to the immediate caller ONLY and used
    solely to compute SHA-256 hashes before any external upload.
  - This module NEVER logs raw email addresses — only domain-level counts.
  - Emails are NOT written to any new tables or intermediate BigQuery storage
    by this module. The caller (push_domain_suppression) is responsible for
    hashing before any network transmission.
  - Domain validation uses an allowlist regex — no free-form strings are
    interpolated into SQL without validation first.

Usage:
    from tools.crm_client import get_crm_emails_by_domain

    emails_by_domain = get_crm_emails_by_domain(["acme.com", "bigcorp.com"])
    # Returns: {"acme.com": ["alice@acme.com", "bob@acme.com"], ...}
    # Caller hashes these before upload; this module never sees post-hash data.
"""
from __future__ import annotations

import re

import structlog

from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Domain allowlist validation ────────────────────────────────────────────────
# Domains are interpolated into SQL via string formatting (BigQuery's parameterized
# query API does not support IN-list parameters). Validate strictly to prevent injection.

_DOMAIN_RE = re.compile(r'^[a-z0-9][a-z0-9\-\.]{0,252}[a-z0-9]$')


def _validate_domain(domain: str) -> str:
    """
    Validate and normalize a domain string for safe SQL interpolation.
    Raises ValueError for malformed or suspiciously long domains.
    """
    normalized = domain.lower().strip()
    if not normalized:
        raise ValueError("Domain cannot be empty.")
    if len(normalized) > 255:
        raise ValueError(f"Domain too long (max 255 chars): {normalized!r}")
    if not _DOMAIN_RE.match(normalized):
        raise ValueError(
            f"Invalid domain format: {normalized!r}. "
            "Must contain only letters, digits, hyphens, and dots."
        )
    return normalized


def get_crm_emails_by_domain(
    domains: list[str] | None = None,
    active_only: bool = True,
    max_emails_per_domain: int = 10_000,
) -> dict[str, list[str]]:
    """
    Resolve company domains to lists of known CRM email addresses.

    Queries crm_leads_staging, groups records by email domain, and returns a dict
    mapping domain → [email, email, ...]. The caller must hash all emails
    before any upload to ad platforms.

    Args:
        domains: Optional list of domain strings to look up, e.g. ["acme.com", "bigcorp.com"].
                 If None, returns ALL domains present in crm_leads_staging (may be large).
                 Providing an explicit list is strongly recommended for efficiency.
        active_only: If True, filters to leads with is_active = TRUE (if that column
                     exists in crm_leads_staging). Default True.
        max_emails_per_domain: Safety cap on returned emails per domain.
                               Prevents accidentally uploading huge lists.

    Returns:
        Dict mapping domain → list of raw email strings.
        Empty dict if no matches found or CRM data is not available.

    Privacy note:
        This function returns raw email addresses to the caller.
        The caller is responsible for hashing before any external transmission.
        Do NOT log, print, or persist the return value.

    Example:
        emails = get_crm_emails_by_domain(["acme.com", "bigcorp.io"])
        # → {"acme.com": ["alice@acme.com", "bob@acme.com"],
        #     "bigcorp.io": ["carol@bigcorp.io"]}
    """
    domain_filter_clause = ""
    if domains:
        # Validate each domain before interpolation
        validated = []
        for d in domains:
            try:
                validated.append(_validate_domain(d))
            except ValueError as exc:
                log.warning("crm_client.invalid_domain", error=str(exc))
                # Skip malformed domains rather than aborting the whole call
                continue

        if not validated:
            log.warning("crm_client.all_domains_invalid", count=len(domains))
            return {}

        # Single-quote each validated domain — safe because _validate_domain
        # allows only [a-z0-9\-\.] characters (no quotes possible after validation)
        domain_literals = ", ".join(f"'{d}'" for d in validated)
        domain_filter_clause = (
            f"AND LOWER(SPLIT(email, '@')[SAFE_OFFSET(1)]) IN ({domain_literals})"
        )

    # Optional is_active filter — handled defensively in case the column doesn't exist
    active_clause = ""
    if active_only:
        # INFORMATION_SCHEMA check would be heavyweight; use a safer approach:
        # include the filter wrapped in a CASE so BQ ignores it if column is missing.
        # BigQuery raises schema errors at query time, so we catch and retry without.
        active_clause = "AND (is_active IS NULL OR is_active = TRUE)"

    sql = f"""
    SELECT
        LOWER(SPLIT(email, '@')[SAFE_OFFSET(1)]) AS domain,
        email
    FROM {bq.table_ref('crm_leads_staging')}
    WHERE email IS NOT NULL
      AND ARRAY_LENGTH(SPLIT(email, '@')) = 2
      AND LENGTH(SPLIT(email, '@')[SAFE_OFFSET(1)]) > 1
      {domain_filter_clause}
      {active_clause}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY LOWER(email)) = 1  -- deduplicate exact email
        AND ROW_NUMBER() OVER (PARTITION BY LOWER(SPLIT(email, '@')[SAFE_OFFSET(1)])) <= {max_emails_per_domain}
    """

    try:
        rows = bq.run_query(sql)
    except Exception as exc:
        # Retry without is_active filter if column doesn't exist
        if active_only and "is_active" in str(exc):
            log.warning(
                "crm_client.no_is_active_column",
                note="Retrying without is_active filter.",
            )
            return get_crm_emails_by_domain(
                domains=domains,
                active_only=False,
                max_emails_per_domain=max_emails_per_domain,
            )
        log.error("crm_client.query_failed", error=str(exc))
        return {}

    result: dict[str, list[str]] = {}
    total_emails = 0

    for row in rows:
        domain = str(row.get("domain", "")).lower()
        email = str(row.get("email", ""))
        if not domain or not email or "@" not in email:
            continue
        if domain not in result:
            result[domain] = []
        result[domain].append(email)
        total_emails += 1

    # Log only domain counts — never raw email addresses
    log.info(
        "crm_client.emails_fetched",
        domains_requested=len(domains) if domains else "all",
        domains_matched=len(result),
        total_email_count=total_emails,
    )

    return result


def summarize_domain_coverage(domains: list[str]) -> dict:
    """
    Report how many CRM emails are available for each domain without returning emails.

    Useful for pre-flight checks before running a suppression job.
    Safe to log — only counts are returned, never raw email data.

    Returns:
        Dict with 'total_domains', 'matched_domains', 'total_emails',
        and 'per_domain' (dict mapping domain → count).
    """
    emails_by_domain = get_crm_emails_by_domain(domains=domains)
    per_domain_counts = {domain: len(emails) for domain, emails in emails_by_domain.items()}
    total_emails = sum(per_domain_counts.values())
    matched = len(per_domain_counts)

    log.info(
        "crm_client.domain_coverage",
        domains_checked=len(domains),
        domains_matched=matched,
        total_emails=total_emails,
    )

    return {
        "total_domains_checked": len(domains),
        "matched_domains": matched,
        "unmatched_domains": [d for d in domains if d not in per_domain_counts],
        "total_emails": total_emails,
        "per_domain": per_domain_counts,
        "coverage_pct": round(matched / max(len(domains), 1) * 100, 1),
    }
