# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Lookalike Audience Mutation Engine — Task 35.

Reads the v_lookalike_mutation_seed analytical cohort view from BigQuery,
hashes seed emails to SHA-256, and hydrates lookalike seed audiences on
Meta, Google Ads, TikTok, and Reddit Ads in a single synchronous runtime block.

Architecture
────────────
  BigQuery v_lookalike_mutation_seed
          │
          ▼
  AudienceMutationEngine.run_mutation(platform_configs)
          │
          ├── _hash_sha256()        (SHA-256, deduped, raw emails never stored)
          │
          ├── _push_meta()          → meta_client.add_hashed_emails_to_exclusion_audience()
          ├── _push_google_ads()    → google_ads_client.add_emails_to_customer_match()
          ├── _push_tiktok()        → tiktok_ads_client.add_hashed_emails_to_audience()
          └── _push_reddit()        → reddit_ads_client.upload_hashed_emails_to_audience()
                    │
                    ▼
          audience_mutation_logs (BQ)

Privacy constraints (non-negotiable):
  • Raw email addresses are read from v_lookalike_mutation_seed (which queries
    crm_leads_staging), hashed immediately via SHA-256, and discarded.
  • No raw email is ever written to BigQuery, logs, or any persistent store.
  • audience_mutation_logs stores only seed_count_after (int), domain count,
    and firmographic aggregates — zero individual-level PII.
  • Log lines reference email counts only, never addresses.

Platform compatibility matrix:
  ┌─────────────┬────────────────────────────────────────────────────┬─────────────────┐
  │ Platform    │ Upload function                                    │ Hash standard   │
  ├─────────────┼────────────────────────────────────────────────────┼─────────────────┤
  │ Meta        │ add_hashed_emails_to_exclusion_audience()          │ SHA-256, lower  │
  │ Google Ads  │ add_emails_to_customer_match()                     │ SHA-256, lower  │
  │ TikTok      │ add_hashed_emails_to_audience()                    │ SHA-256, lower  │
  │ Reddit Ads  │ upload_hashed_emails_to_audience()                 │ SHA-256, lower  │
  └─────────────┴────────────────────────────────────────────────────┴─────────────────┘

Usage (from Operator agent):
    from tools.audience_mutation_engine import AudienceMutationEngine

    engine = AudienceMutationEngine()
    result = engine.run_mutation(
        platform_configs=[
            {"platform": "meta",       "advertiser_id": "act_123456789", "audience_id": "987654321"},
            {"platform": "google_ads", "advertiser_id": "1234567890",    "audience_id": "customers/123/userLists/456"},
            {"platform": "tiktok",     "advertiser_id": "1234567890",    "audience_id": "123456789"},
            {"platform": "reddit_ads", "advertiser_id": "t2_abc123",     "audience_id": "reddit_aud_id"},
        ],
        seed_limit=10_000,
    )
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone

import structlog

from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Constants ──────────────────────────────────────────────────────────────────

# Hard cap applied when caller doesn't specify seed_limit.
# Most platforms have soft limits of 100k–1M rows, but 10k is a safe default
# that avoids accidental bulk-overwrite of audience seeds on the first run.
_DEFAULT_SEED_CAP = 10_000

_SUPPORTED_PLATFORMS = frozenset({"meta", "google_ads", "tiktok", "reddit_ads"})


# ── Core engine ───────────────────────────────────────────────────────────────

class AudienceMutationEngine:
    """
    Orchestrates one complete mutation cycle:
      extract → hash → push (all platforms) → log.

    Instantiate once per Operator tool call. Each instance carries its own
    run_id, which links all platform push rows in audience_mutation_logs.
    """

    def __init__(self) -> None:
        self._run_id: str = bq.new_uuid()
        self._arr_p75_threshold: float | None = None

    # ── Public API ─────────────────────────────────────────────────────────────

    def run_mutation(
        self,
        platform_configs: list[dict],
        seed_limit: int | None = _DEFAULT_SEED_CAP,
    ) -> dict:
        """
        Execute a full mutation cycle.

        Args:
            platform_configs:
                List of dicts, one per target platform audience.
                Each dict must contain:
                    platform      — "meta" | "google_ads" | "tiktok" | "reddit_ads"
                    audience_id   — platform audience / list ID
                    advertiser_id — platform advertiser / account ID
                        Meta:        not required (reads from settings); pass "" or omit.
                        Google Ads:  customer_id (digits only, no dashes).
                        TikTok:      advertiser_id (numeric string).
                        Reddit Ads:  ad account ID (t2_xxx or a2_xxx).

            seed_limit:
                Maximum unique emails to push per platform.
                Default: 10,000. Pass None to disable (use full seed cohort).

        Returns:
            dict with keys:
                ok                  — True if at least one platform push succeeded
                run_id              — UUID shared across all log rows for this run
                seed_count          — unique hashed emails extracted from BQ view
                unique_domains      — unique company domains in the seed cohort
                arr_p75_threshold   — ARR floor used to gate the high-value cohort
                platforms_pushed    — count of platforms with status == "ok"
                platforms_total     — count of platforms in platform_configs
                platform_results    — dict keyed by "platform:audience_id"
                firmographic_summary — over-index analysis dict
        """
        log.info("audience_mutation.run_started", run_id=self._run_id)

        # ── Step 1: Extract seed cohort from BigQuery ────────────────────────
        try:
            seed_rows, firmographic_summary = self._extract_seed_cohort(seed_limit=seed_limit)
        except Exception as exc:
            log.error("audience_mutation.seed_fetch_failed", run_id=self._run_id, error=str(exc))
            return {
                "ok":     False,
                "run_id": self._run_id,
                "error":  f"Failed to read v_lookalike_mutation_seed: {exc}",
            }

        if not seed_rows:
            log.warning("audience_mutation.empty_seed_cohort", run_id=self._run_id)
            return {
                "ok":     False,
                "run_id": self._run_id,
                "error": (
                    "v_lookalike_mutation_seed returned zero records. "
                    "Verify that crm_opportunities_staging contains closed-won opportunities "
                    "with close_date within the last 60 days and a non-null amount value."
                ),
                "firmographic_summary": {},
            }

        unique_domains = len({
            r["company_domain"] for r in seed_rows if r.get("company_domain")
        })
        arr_threshold = seed_rows[0].get("arr_p75_threshold") if seed_rows else None
        self._arr_p75_threshold = float(arr_threshold) if arr_threshold is not None else None

        # ── Step 2: Hash emails (raw emails are discarded after this block) ──
        hashed_emails = self._deduplicated_hashes(seed_rows)
        seed_count = len(hashed_emails)
        log.info(
            "audience_mutation.seed_hashed",
            run_id=self._run_id,
            seed_count=seed_count,
            unique_domains=unique_domains,
            arr_p75_threshold=self._arr_p75_threshold,
        )

        if not hashed_emails:
            return {
                "ok":     False,
                "run_id": self._run_id,
                "error":  "Seed cohort contained rows but no valid email addresses. "
                          "Check that crm_leads_staging.email column is populated.",
                "firmographic_summary": firmographic_summary,
            }

        # ── Step 3: Push to each configured platform ─────────────────────────
        platform_results: dict[str, dict] = {}
        ok_count = 0

        for cfg in platform_configs:
            platform     = (cfg.get("platform") or "").strip().lower()
            audience_id  = (cfg.get("audience_id") or "").strip()
            advertiser_id = (cfg.get("advertiser_id") or "").strip()
            key          = f"{platform}:{audience_id}"

            if platform not in _SUPPORTED_PLATFORMS:
                platform_results[key] = {
                    "status":       "unsupported",
                    "platform":     platform,
                    "advertiser_id": advertiser_id,
                    "audience_id":  audience_id,
                    "error": (
                        f"Platform '{platform}' is not supported by the mutation engine. "
                        f"Supported: {sorted(_SUPPORTED_PLATFORMS)}."
                    ),
                }
                continue

            if not audience_id:
                platform_results[key] = {
                    "status":  "error",
                    "platform": platform,
                    "error":   "audience_id is required.",
                }
                continue

            log.info(
                "audience_mutation.pushing",
                run_id=self._run_id,
                platform=platform,
                audience_id=audience_id,
                email_count=seed_count,
            )
            result = self._dispatch(platform, advertiser_id, audience_id, hashed_emails)
            result["platform"]     = platform
            result["advertiser_id"] = advertiser_id
            result["audience_id"]  = audience_id
            platform_results[key] = result

            if result.get("status") == "ok":
                ok_count += 1

            # ── Step 4: Log to audience_mutation_logs ──────────────────────
            self._log_mutation_row(
                platform=platform,
                audience_id=audience_id,
                advertiser_id=advertiser_id,
                seed_count_after=seed_count,
                unique_domains=unique_domains,
                firmographic_summary=firmographic_summary,
                status="completed" if result.get("status") == "ok" else "failed",
                error_message=result.get("error"),
            )

        log.info(
            "audience_mutation.run_complete",
            run_id=self._run_id,
            ok_count=ok_count,
            total=len(platform_configs),
        )

        return {
            "ok":                  ok_count > 0,
            "run_id":              self._run_id,
            "seed_count":          seed_count,
            "unique_domains":      unique_domains,
            "arr_p75_threshold":   self._arr_p75_threshold,
            "platforms_pushed":    ok_count,
            "platforms_total":     len(platform_configs),
            "platform_results":    platform_results,
            "firmographic_summary": firmographic_summary,
        }

    # ── Seed extraction ────────────────────────────────────────────────────────

    def _extract_seed_cohort(
        self,
        seed_limit: int | None,
    ) -> tuple[list[dict], dict]:
        """
        Query v_lookalike_mutation_seed from BigQuery.

        Returns:
            (seed_rows, firmographic_summary)
            seed_rows: list of raw BQ row dicts (contains raw email — hash immediately)
            firmographic_summary: aggregated over-index report (no PII)
        """
        limit_clause = f"LIMIT {int(seed_limit)}" if seed_limit else ""
        sql = f"""
            SELECT
                company_domain,
                email,
                industry,
                employee_range,
                headquarters_country,
                headquarters_region,
                icp_score,
                total_arr,
                deal_count,
                latest_close_date,
                arr_p75_threshold,
                arr_tier_label,
                cohort_label,
                industry_over_index_pct,
                emp_over_index_pct,
                region_over_index_pct
            FROM {bq.table_ref('v_lookalike_mutation_seed')}
            WHERE email IS NOT NULL
              AND TRIM(email) != ''
            {limit_clause}
        """
        rows = bq.run_query(sql)
        summary = _compute_firmographic_summary(rows)
        return rows, summary

    # ── Email hashing ──────────────────────────────────────────────────────────

    @staticmethod
    def _hash_sha256(email: str) -> str:
        """SHA-256(email.lower().strip()). Universal across all four platforms."""
        return hashlib.sha256(email.lower().strip().encode()).hexdigest()

    def _deduplicated_hashes(self, seed_rows: list[dict]) -> list[str]:
        """
        Extract, normalize, and SHA-256 hash all emails from seed rows.
        Deduplicates by hash to guarantee unique upload set.
        Raw email strings exist only within this method's stack frame.
        """
        seen: set[str] = set()
        hashed: list[str] = []
        for row in seed_rows:
            raw = (row.get("email") or "").strip()
            if not raw or "@" not in raw:
                continue
            digest = self._hash_sha256(raw)
            if digest not in seen:
                seen.add(digest)
                hashed.append(digest)
        return hashed

    # ── Platform dispatch ──────────────────────────────────────────────────────

    def _dispatch(
        self,
        platform: str,
        advertiser_id: str,
        audience_id: str,
        hashed_emails: list[str],
    ) -> dict:
        """Route to the correct platform push method."""
        if platform == "meta":
            return self._push_meta(audience_id, hashed_emails)
        if platform == "google_ads":
            return self._push_google_ads(advertiser_id, audience_id, hashed_emails)
        if platform == "tiktok":
            return self._push_tiktok(advertiser_id, audience_id, hashed_emails)
        if platform == "reddit_ads":
            return self._push_reddit(advertiser_id, audience_id, hashed_emails)
        # Should not reach here — caller validates platform before dispatch.
        return {"status": "error", "error": f"Unknown platform: {platform!r}"}

    def _push_meta(self, audience_id: str, hashed_emails: list[str]) -> dict:
        """
        Push SHA-256 hashed emails to a Meta Custom Audience seed list.

        Uses add_hashed_emails_to_exclusion_audience() — which accepts any custom
        audience type (not just exclusion lists), so it works for lookalike seed
        hydration as well.

        Meta's account_id is read from settings.meta_ad_account_id by the client;
        no advertiser_id parameter is needed here.
        """
        try:
            from tools.meta_client import (
                add_hashed_emails_to_exclusion_audience,
            )
            result = add_hashed_emails_to_exclusion_audience(
                audience_id=audience_id,
                hashed_emails=hashed_emails,
            )
            log.info("audience_mutation.meta.ok", audience_id=audience_id, count=len(hashed_emails))
            return {
                "status":        "ok",
                "emails_pushed": len(hashed_emails),
                "platform_response": result,
            }
        except Exception as exc:
            log.warning("audience_mutation.meta.failed", audience_id=audience_id, error=str(exc))
            return {"status": "error", "error": str(exc)}

    def _push_google_ads(
        self,
        customer_id: str,
        user_list_resource_name: str,
        hashed_emails: list[str],
    ) -> dict:
        """
        Push SHA-256 hashed emails to a Google Ads Customer Match user list.

        customer_id:             Google Ads customer ID (digits only, no dashes).
        user_list_resource_name: e.g. "customers/123456789/userLists/987654321".
        hashed_emails:           Pre-hashed via SHA-256(email.lower().strip()).

        Internally uses add_emails_to_customer_match() which enqueues an offline
        user data job — match rate results are visible in Google Ads UI after
        processing (typically 6–24 hours).
        """
        try:
            from tools.google_ads_client import (
                add_emails_to_customer_match,
            )
            result = add_emails_to_customer_match(
                customer_id=customer_id,
                user_list_resource_name=user_list_resource_name,
                hashed_emails=hashed_emails,
            )
            log.info(
                "audience_mutation.google_ads.ok",
                customer_id=customer_id,
                user_list=user_list_resource_name,
                count=len(hashed_emails),
            )
            return {
                "status":        "ok",
                "emails_pushed": len(hashed_emails),
                "platform_response": result,
            }
        except Exception as exc:
            log.warning(
                "audience_mutation.google_ads.failed",
                customer_id=customer_id,
                user_list=user_list_resource_name,
                error=str(exc),
            )
            return {"status": "error", "error": str(exc)}

    def _push_tiktok(
        self,
        advertiser_id: str,
        audience_id: str,
        hashed_emails: list[str],
    ) -> dict:
        """
        Push SHA-256 hashed emails to a TikTok DMP Custom Audience.

        TikTok's file-upload endpoint accepts pre-hashed email identifiers
        (calculate_type = "SHA256"). Allow 24–48 hours for population and
        match rate to appear in TikTok Ads Manager.
        """
        try:
            from tools.tiktok_ads_client import (
                add_hashed_emails_to_audience,
            )
            result = add_hashed_emails_to_audience(
                advertiser_id=advertiser_id,
                audience_id=audience_id,
                hashed_emails=hashed_emails,
            )
            log.info(
                "audience_mutation.tiktok.ok",
                advertiser_id=advertiser_id,
                audience_id=audience_id,
                count=len(hashed_emails),
            )
            return {
                "status":        "ok",
                "emails_pushed": len(hashed_emails),
                "platform_response": result,
            }
        except Exception as exc:
            log.warning(
                "audience_mutation.tiktok.failed",
                advertiser_id=advertiser_id,
                audience_id=audience_id,
                error=str(exc),
            )
            return {"status": "error", "error": str(exc)}

    def _push_reddit(
        self,
        account_id: str,
        audience_id: str,
        hashed_emails: list[str],
    ) -> dict:
        """
        Push SHA-256 hashed emails to a Reddit Ads Customer List audience.

        account_id must begin with t2_ or a2_ — validated inside
        upload_hashed_emails_to_audience() via _validate_account_id().
        Allow 24–48 hours for match rate to appear in Reddit Ads Manager.
        """
        try:
            from tools.reddit_ads_client import (
                upload_hashed_emails_to_audience,
            )
            result = upload_hashed_emails_to_audience(
                account_id=account_id,
                audience_id=audience_id,
                hashed_emails=hashed_emails,
            )
            log.info(
                "audience_mutation.reddit.ok",
                account_id=account_id,
                audience_id=audience_id,
                count=len(hashed_emails),
            )
            return {
                "status":        "ok",
                "emails_pushed": len(hashed_emails),
                "platform_response": result,
            }
        except Exception as exc:
            log.warning(
                "audience_mutation.reddit.failed",
                account_id=account_id,
                audience_id=audience_id,
                error=str(exc),
            )
            return {"status": "error", "error": str(exc)}

    # ── BigQuery logging ───────────────────────────────────────────────────────

    def _log_mutation_row(
        self,
        platform: str,
        audience_id: str,
        advertiser_id: str,
        seed_count_after: int,
        unique_domains: int,
        firmographic_summary: dict,
        status: str,
        error_message: str | None,
    ) -> None:
        """Write one row to audience_mutation_logs. Zero PII — counts and aggregates only."""
        row = {
            "mutation_id":                bq.new_uuid(),
            "run_id":                     self._run_id,
            "platform":                   platform,
            "audience_id":                audience_id,
            "advertiser_id":              advertiser_id or None,
            "seed_count_before":          None,   # not exposed by platform APIs without extra call
            "seed_count_after":           seed_count_after,
            "domains_in_seed":            unique_domains,
            "arr_threshold_usd":          self._arr_p75_threshold,
            "dominant_firmographic_shift": firmographic_summary.get("dominant_shift"),
            "top_shifts_json":            json.dumps(firmographic_summary),
            "status":                     status,
            "error_message":              error_message,
            "created_by":                 "operator_agent",
            "created_at":                 datetime.now(timezone.utc).isoformat(),
        }
        try:
            bq.insert_rows("audience_mutation_logs", [row])
        except Exception as exc:
            log.warning("audience_mutation.log_write_failed", error=str(exc))


# ── Firmographic summary (module-level, pure analytics) ──────────────────────

def _compute_firmographic_summary(seed_rows: list[dict]) -> dict:
    """
    Aggregate firmographic over-index scores from seed rows.

    Averages the pre-computed over_index_pct values (which came from the BQ view)
    across all rows for each trait. Returns the top 3 traits in each dimension
    plus a single 'dominant_shift' string for the Markdown headline.

    No PII involved — works only on firmographic labels and numeric scores.
    """
    industry_scores: dict[str, list[float]] = defaultdict(list)
    emp_scores:      dict[str, list[float]] = defaultdict(list)
    region_scores:   dict[str, list[float]] = defaultdict(list)

    for row in seed_rows:
        ind = row.get("industry") or "Unknown"
        emp = row.get("employee_range") or "Unknown"
        reg = row.get("headquarters_region") or "Unknown"

        if row.get("industry_over_index_pct") is not None:
            industry_scores[ind].append(float(row["industry_over_index_pct"]))
        if row.get("emp_over_index_pct") is not None:
            emp_scores[emp].append(float(row["emp_over_index_pct"]))
        if row.get("region_over_index_pct") is not None:
            region_scores[reg].append(float(row["region_over_index_pct"]))

    def _top_n(score_dict: dict[str, list[float]], n: int = 3) -> list[dict]:
        averaged = {k: sum(v) / len(v) for k, v in score_dict.items() if v}
        ranked = sorted(averaged.items(), key=lambda x: x[1], reverse=True)[:n]
        return [{"trait": k, "over_index_pct": round(v, 1)} for k, v in ranked]

    top_industries = _top_n(industry_scores)
    top_emp        = _top_n(emp_scores)
    top_regions    = _top_n(region_scores)

    # Dominant shift: single highest over-index label across all dimensions
    all_candidates: list[tuple[str, float]] = []
    for item in top_industries:
        if item["over_index_pct"] > 0:
            all_candidates.append((f"{item['trait']} (industry)", item["over_index_pct"]))
    for item in top_emp:
        if item["over_index_pct"] > 0:
            all_candidates.append((f"{item['trait']} headcount", item["over_index_pct"]))
    for item in top_regions:
        if item["over_index_pct"] > 0:
            all_candidates.append((f"{item['trait']} (region)", item["over_index_pct"]))

    if all_candidates:
        dom_label, dom_pct = max(all_candidates, key=lambda x: x[1])
        dominant_shift = f"{dom_label} +{dom_pct:.1f}%"
    else:
        dominant_shift = "No over-index data (company_profiles may be sparsely populated)"

    return {
        "top_industries":      top_industries,
        "top_employee_ranges": top_emp,
        "top_regions":         top_regions,
        "dominant_shift":      dominant_shift,
    }
