# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
IP Intelligence Client — multi-provider B2B IP-to-company resolution.

Resolves corporate IP addresses to company domains for account-based analytics.
Supports Clearbit Reveal, ipinfo.io, and RB2B with a pluggable provider pattern.

PRIVACY CONSTRAINTS (strictly enforced throughout this module):
  • Raw IP addresses are NEVER written to any log, file, or database
  • Only the /24 prefix (first 3 octets, e.g., "203.0.113") is used as a cache key
  • Resolution results store company_domain only — a non-PII, public identifier
  • VPN, datacenter, and residential IPs are classified and excluded from analytics
  • The full IP address exists only as a local variable inside resolve() — it is
    passed to the provider API and immediately discarded

Provider selection (set IP_INTELLIGENCE_PROVIDER in .env):
  "ipinfo"    — ipinfo.io Company API (affordable, free tier, good coverage)
  "clearbit"  — Clearbit Reveal (premium B2B data, best firmographic enrichment)
  "composite" — tries Clearbit first, falls back to ipinfo.io
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog

from config import settings

log = structlog.get_logger()


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class CompanyResolution:
    """
    Result of resolving a visitor IP to a company identity.
    All fields here are non-PII — the raw IP is never stored.
    """
    resolution_type: str          # "corporate" | "residential" | "vpn" | "datacenter" | "bot" | "unknown"
    confidence: float             # 0.0–1.0
    provider: str                 # which provider produced this result

    # Company identity (non-PII public identifiers)
    company_domain: str | None = None
    company_name: str | None = None

    # Geographic context (country / region only — not city or postal code)
    country_code: str | None = None
    country_name: str | None = None
    region: str | None = None     # state or province

    # Exclusion flags
    is_vpn: bool = False
    is_datacenter: bool = False
    is_residential: bool = False
    is_bot_suspected: bool = False

    # Diagnostics
    provider_response_ms: int = 0
    raw_provider_data: dict = field(default_factory=dict, repr=False)

    @property
    def should_exclude(self) -> bool:
        """True if this IP should be excluded from company analytics."""
        return (
            self.is_vpn
            or self.is_datacenter
            or self.is_bot_suspected
            or self.resolution_type in ("residential", "bot", "datacenter", "vpn")
        )

    @property
    def is_resolved(self) -> bool:
        """True if a company domain was successfully identified."""
        return bool(self.company_domain and self.confidence > 0)


# ── Privacy utility ───────────────────────────────────────────────────────────

def extract_prefix(ip_address: str) -> str:
    """
    Extract the /24 prefix from an IPv4 address.
    This is the ONLY string derived from the raw IP that we ever store.

    "203.0.113.42" → "203.0.113"
    "10.20.30.0"   → "10.20.30"

    For IPv6 or malformed addresses, returns the address unchanged —
    these will produce a cache miss and trigger an API call that returns
    resolution_type="unknown".
    """
    parts = ip_address.strip().split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.{parts[2]}"
    return ip_address  # IPv6 or malformed — treated as opaque key


# ── Provider ABC ──────────────────────────────────────────────────────────────

class IPIntelligenceProvider(ABC):
    """
    Abstract base class for IP intelligence providers.

    Each provider receives the full IP address in resolve() and returns a
    CompanyResolution. Providers MUST NOT log, store, or transmit the raw IP.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Short identifier used in resolution metadata and logs."""
        ...

    @abstractmethod
    def resolve(self, ip_address: str) -> CompanyResolution:
        """
        Call the provider API and return a resolution result.

        The full ip_address is used only for the API call. It must not be
        stored, logged, or returned in the result object.
        """
        ...


# ── ipinfo.io provider ────────────────────────────────────────────────────────

class IPInfoProvider(IPIntelligenceProvider):
    """
    ipinfo.io Company API.

    Recommended starting point: generous free tier (50k lookups/month),
    reliable B2B domain coverage, returns ASN + company info.
    Requires: IPINFO_ACCESS_TOKEN in .env

    API docs: https://ipinfo.io/developers
    """

    _BASE = "https://ipinfo.io"

    def __init__(self, access_token: str) -> None:
        self._token = access_token
        self._http = httpx.Client(
            timeout=httpx.Timeout(5.0),
            headers={"Authorization": f"Bearer {access_token}"},
        )

    @property
    def provider_name(self) -> str:
        return "ipinfo"

    def resolve(self, ip_address: str) -> CompanyResolution:
        t0 = time.monotonic()
        try:
            resp = self._http.get(f"{self._BASE}/{ip_address}/json")
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
        except Exception as exc:
            log.warning("ip_intel.ipinfo_error", error=str(exc))
            return CompanyResolution(
                resolution_type="unknown", confidence=0.0,
                provider=self.provider_name,
                provider_response_ms=int((time.monotonic() - t0) * 1000),
            )

        ms = int((time.monotonic() - t0) * 1000)
        privacy = data.get("privacy") or {}
        company = data.get("company") or {}
        org = data.get("org", "")  # e.g. "AS15169 Google LLC"

        is_vpn = bool(privacy.get("vpn"))
        is_dc  = bool(privacy.get("hosting"))
        is_bot = bool(privacy.get("abuse"))

        if is_vpn or is_dc or is_bot:
            return CompanyResolution(
                resolution_type="vpn" if is_vpn else ("bot" if is_bot else "datacenter"),
                confidence=0.0, provider=self.provider_name,
                country_code=data.get("country"),
                region=data.get("region"),
                is_vpn=is_vpn, is_datacenter=is_dc, is_bot_suspected=is_bot,
                provider_response_ms=ms,
            )

        domain = company.get("domain")
        name   = company.get("name") or (org.split(" ", 1)[1] if " " in org else None)

        if not domain:
            return CompanyResolution(
                resolution_type="residential" if not org else "unknown",
                confidence=0.2, provider=self.provider_name,
                company_name=name,
                country_code=data.get("country"),
                region=data.get("region"),
                is_residential=not bool(org),
                provider_response_ms=ms,
            )

        return CompanyResolution(
            resolution_type="corporate",
            confidence=0.75,
            provider=self.provider_name,
            company_domain=domain.lower().strip(),
            company_name=name,
            country_code=data.get("country"),
            region=data.get("region"),
            provider_response_ms=ms,
        )


# ── Clearbit provider ─────────────────────────────────────────────────────────

class ClearbitProvider(IPIntelligenceProvider):
    """
    Clearbit Reveal API (now part of HubSpot).

    Best firmographic depth for B2B — returns industry, employee count,
    tech stack, and company domain. Higher confidence than ipinfo.io.
    Requires: CLEARBIT_API_KEY in .env

    API docs: https://clearbit.com/docs#reveal-api
    """

    _BASE = "https://reveal.clearbit.com/v1/companies/find"

    def __init__(self, api_key: str) -> None:
        self._http = httpx.Client(
            timeout=httpx.Timeout(5.0),
            auth=(api_key, ""),
        )

    @property
    def provider_name(self) -> str:
        return "clearbit"

    def resolve(self, ip_address: str) -> CompanyResolution:
        t0 = time.monotonic()
        try:
            resp = self._http.get(self._BASE, params={"ip": ip_address})
            ms = int((time.monotonic() - t0) * 1000)

            if resp.status_code == 404:
                return CompanyResolution(
                    resolution_type="unknown", confidence=0.0,
                    provider=self.provider_name, provider_response_ms=ms,
                )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("ip_intel.clearbit_error", error=str(exc))
            return CompanyResolution(
                resolution_type="unknown", confidence=0.0,
                provider=self.provider_name,
                provider_response_ms=int((time.monotonic() - t0) * 1000),
            )

        ms = int((time.monotonic() - t0) * 1000)
        company = data.get("company") or data
        geo = company.get("geo") or {}

        domain = company.get("domain")
        name   = company.get("name")

        if not domain:
            return CompanyResolution(
                resolution_type="unknown", confidence=0.1,
                provider=self.provider_name,
                company_name=name,
                country_code=geo.get("countryCode"),
                region=geo.get("state"),
                provider_response_ms=ms,
            )

        return CompanyResolution(
            resolution_type="corporate",
            confidence=0.90,  # Clearbit has higher precision than ipinfo
            provider=self.provider_name,
            company_domain=domain.lower().strip(),
            company_name=name,
            country_code=geo.get("countryCode"),
            country_name=geo.get("country"),
            region=geo.get("state"),
            provider_response_ms=ms,
            raw_provider_data={
                "industry":       company.get("category", {}).get("industry"),
                "employee_count": company.get("metrics", {}).get("employees"),
                "tech":           company.get("tech", []),
                "type":           company.get("type"),
            },
        )


# ── RB2B stub provider (webhook push model) ───────────────────────────────────

class RB2BProvider(IPIntelligenceProvider):
    """
    RB2B identification service (https://rb2b.com).

    RB2B operates as a push webhook — it identifies visitors and sends
    notifications to your endpoint rather than accepting pull lookups.
    This stub represents the "resolution" step after RB2B delivers a match
    via webhook to the paid-media-agent's inbound endpoint.

    For webhook handling, see: agents/analyst/enrichment.py → handle_rb2b_webhook()
    This provider is a no-op for pull-mode resolution — configure RB2B via webhook.
    """

    @property
    def provider_name(self) -> str:
        return "rb2b"

    def resolve(self, ip_address: str) -> CompanyResolution:
        # RB2B is push-only — cannot do pull resolution
        return CompanyResolution(
            resolution_type="unknown",
            confidence=0.0,
            provider=self.provider_name,
        )


# ── Composite provider (tries in order) ──────────────────────────────────────

class CompositeProvider(IPIntelligenceProvider):
    """
    Tries each provider in order. Returns the first result that meets
    the confidence threshold. Falls back to the last provider's result
    if none meet the threshold.

    Default order: Clearbit → ipinfo.io
    Clearbit's higher confidence (0.9) clears the threshold (0.7) immediately,
    so it short-circuits. ipinfo.io is the fallback at 0.75 confidence.
    """

    def __init__(
        self,
        providers: list[IPIntelligenceProvider],
        confidence_threshold: float = 0.7,
    ) -> None:
        self._providers = providers
        self._threshold = confidence_threshold

    @property
    def provider_name(self) -> str:
        return "composite:" + "+".join(p.provider_name for p in self._providers)

    def resolve(self, ip_address: str) -> CompanyResolution:
        last: CompanyResolution | None = None
        for provider in self._providers:
            result = provider.resolve(ip_address)
            last = result
            if result.company_domain and result.confidence >= self._threshold:
                return result
        return last or CompanyResolution(
            resolution_type="unknown", confidence=0.0, provider="none"
        )


# ── Main client ───────────────────────────────────────────────────────────────

class IPIntelligenceClient:
    """
    Main IP-to-company resolution client.

    Manages a two-level cache:
      L1 — in-process dict (fast, lost on restart)
      L2 — BigQuery ip_resolution_cache table (persistent, survives restarts)

    The full IP address is used only to call the provider API.
    The /24 prefix (only) is stored in the cache and logs.
    """

    def __init__(
        self,
        provider: IPIntelligenceProvider,
        cache_reader: "CacheReader",
    ) -> None:
        self._provider = provider
        self._cache = cache_reader
        self._l1: dict[str, tuple[CompanyResolution, float]] = {}
        self._ttl = settings.ip_resolution_cache_ttl_hours * 3600
        self._min_confidence = settings.ip_resolution_confidence_threshold

    def resolve(self, ip_address: str) -> CompanyResolution | None:
        """
        Resolve an IP to a company.

        Returns None when:
          - The IP is residential, VPN, datacenter, or bot traffic
          - The resolution confidence is below the configured threshold
          - The provider returns an error

        The raw IP address is passed only to the provider API call and is
        never stored, logged, or returned.
        """
        # Step 1: Extract /24 prefix — the only key we ever store
        prefix = extract_prefix(ip_address)

        # Step 2: L1 in-process cache
        cached = self._l1.get(prefix)
        if cached:
            result, ts = cached
            if time.monotonic() - ts < self._ttl:
                return None if result.should_exclude else result

        # Step 3: L2 persistent cache (BigQuery)
        cached_row = self._cache.read(prefix)
        if cached_row:
            result = _row_to_resolution(cached_row)
            self._l1[prefix] = (result, time.monotonic())
            self._cache.increment_hit(prefix)
            return None if result.should_exclude else result

        # Step 4: Live API call (ip_address used here only — not stored)
        result = self._provider.resolve(ip_address)

        # Step 5: Write /24 prefix + result to L2 cache (not the full IP)
        self._cache.write(prefix, result)
        self._l1[prefix] = (result, time.monotonic())

        log.info(
            "ip_intel.resolved",
            prefix=prefix,           # /24 prefix — safe to log
            domain=result.company_domain,
            type=result.resolution_type,
            confidence=result.confidence,
            provider=result.provider,
        )

        # Step 6: Filter out non-corporate or low-confidence results
        if result.should_exclude:
            return None
        if result.confidence < self._min_confidence:
            return None

        return result


# ── Cache reader/writer protocol ──────────────────────────────────────────────

class CacheReader:
    """
    Thin interface over the ip_resolution_cache BigQuery table.
    Separated so the BigQuery implementation can be swapped for Snowflake/Redshift
    when Task 23 (database agnosticism) is implemented.
    """

    def read(self, prefix: str) -> dict | None:
        """Return a cached row for this /24 prefix, or None if not cached / expired."""
        raise NotImplementedError

    def write(self, prefix: str, result: CompanyResolution) -> None:
        """Write a new resolution result to the cache."""
        raise NotImplementedError

    def increment_hit(self, prefix: str) -> None:
        """Increment hit_count for cache analytics."""
        raise NotImplementedError


def _row_to_resolution(row: dict) -> CompanyResolution:
    """Reconstruct a CompanyResolution from a cached BigQuery row."""
    return CompanyResolution(
        resolution_type=row.get("resolution_type", "unknown"),
        confidence=float(row.get("resolution_confidence", 0)),
        provider=row.get("resolution_provider", "cache"),
        company_domain=row.get("resolved_company_domain"),
        company_name=row.get("resolved_company_name"),
        country_code=row.get("country_code"),
        country_name=row.get("country_name"),
        region=row.get("region"),
        is_vpn=bool(row.get("is_vpn")),
        is_datacenter=bool(row.get("is_datacenter")),
        is_residential=bool(row.get("is_residential")),
        is_bot_suspected=bool(row.get("is_bot_suspected")),
    )


# ── Factory function ──────────────────────────────────────────────────────────

def build_provider() -> IPIntelligenceProvider:
    """
    Build the configured IP intelligence provider from settings.
    Called once at agent startup.

    Provider is selected by IP_INTELLIGENCE_PROVIDER env var:
      "ipinfo"    — ipinfo.io (default, free tier)
      "clearbit"  — Clearbit Reveal (premium)
      "composite" — Clearbit → ipinfo fallback
    """
    choice = settings.ip_intelligence_provider.lower()

    if choice == "clearbit":
        if not settings.clearbit_api_key:
            raise ValueError("CLEARBIT_API_KEY is required when IP_INTELLIGENCE_PROVIDER=clearbit")
        return ClearbitProvider(settings.clearbit_api_key)

    if choice == "ipinfo":
        if not settings.ipinfo_access_token:
            raise ValueError("IPINFO_ACCESS_TOKEN is required when IP_INTELLIGENCE_PROVIDER=ipinfo")
        return IPInfoProvider(settings.ipinfo_access_token)

    if choice == "composite":
        providers: list[IPIntelligenceProvider] = []
        if settings.clearbit_api_key:
            providers.append(ClearbitProvider(settings.clearbit_api_key))
        if settings.ipinfo_access_token:
            providers.append(IPInfoProvider(settings.ipinfo_access_token))
        if not providers:
            raise ValueError(
                "At least one of CLEARBIT_API_KEY or IPINFO_ACCESS_TOKEN is required "
                "when IP_INTELLIGENCE_PROVIDER=composite"
            )
        return CompositeProvider(providers, settings.ip_resolution_confidence_threshold)

    raise ValueError(
        f"Unknown IP_INTELLIGENCE_PROVIDER: '{choice}'. "
        "Valid values: 'clearbit', 'ipinfo', 'composite'"
    )
