# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Account Analytics Inspector — ABM intent signal detection engine (Task 32).

Queries de-anonymized account engagement from the 07_account_analytics.sql
schema layer and surfaces actionable intent signals stratified into three
intelligence tiers for ABM activation.

Architecture
────────────
  company_engagement (rolling_30d)  ←  primary signal source
  company_profiles                  ←  firmographics + ICP classification
  target_account_activity           ←  recent-day spike / spiking flag
         │
         ▼
  AccountAnalyticsInspector.get_intent_signals()
  ┌────────────────────────────────────────────┐
  │  1. SQL query — surge score computation    │
  │  2. Noise filter — bots, residential, ISP  │
  │  3. Account tier classification            │
  │     🚀 High Intent Surge                   │
  │     🔍 Early Discovery                     │
  │     ⚠️  ICP Boundary Breakers              │
  │  4. SkillResolver prompt resolution         │
  │  5. Markdown intelligence brief            │
  └────────────────────────────────────────────┘
         │
         ▼
  {markdown_brief, accounts, tier_summary, evaluation_context, prompt_source}

Surge Score (0–100)
───────────────────
  40%  Base intent score  (pre-computed in company_engagement)
  20%  Session velocity   (growth_pct vs prior period, capped at 100%)
  25%  High-intent page depth  (pricing × 5, demo × 5, contact × 4, docs × 2)
  15%  Multi-channel paid exposure  (5 pts per distinct paid platform, max 3)

Intelligence Tiers
──────────────────
  🚀 High Intent Surge   — surge_score ≥ 65 AND bottom-funnel pages visited
  🔍 Early Discovery     — target/ICP-fit account with surge_score 30–64
  ⚠️  ICP Boundary Breakers — surge_score ≥ 45, NOT in current target account list

Private skills file
───────────────────
  Path:    agents/analyst/skills/private_account_intent.md
  Purpose: Extended ABM heuristics — Account Inversion analysis, Buying
           Committee Signature detection, ICP Miss identification.
  Git:     gitignored, never committed.  Falls back to public_fallback_prompt.

Privacy constraints (inherited from 07_account_analytics.sql):
  • No raw IP addresses returned — only company_domain
  • No individual-level PII — aggregate session counts only
  • Residential, VPN, datacenter, and bot traffic pre-filtered upstream
"""

from __future__ import annotations

import structlog

from tools import bigquery_client as bq
from tools.skill_resolver import SkillResolver

log = structlog.get_logger()

# ── Skill resolver singleton ───────────────────────────────────────────────────
_skill_resolver = SkillResolver()

# ── Noise exclusion — consumer / ISP domains ──────────────────────────────────
# These should not appear in IP-intelligence-resolved corporate traffic,
# but are excluded defensively in case of any enrichment edge cases.
_FREEMAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.fr", "yahoo.de",
    "hotmail.com", "hotmail.co.uk", "hotmail.fr",
    "outlook.com", "live.com", "msn.com",
    "aol.com", "aol.co.uk",
    "protonmail.com", "proton.me",
    "icloud.com", "me.com", "mac.com",
    "comcast.net", "att.net", "verizon.net",
    "sbcglobal.net", "bellsouth.net", "cox.net",
    "earthlink.net", "charter.net",
})

# Enrichment confidence floor — exclude low-confidence IP resolutions
_MIN_CONFIDENCE = 0.60

# ── Public fallback evaluation prompt ─────────────────────────────────────────
# Functional baseline for open-source deployments.
# For extended ABM heuristics (Account Inversion, Buying Committee Signatures,
# Static ICP Miss Detection), place private_account_intent.md at:
#   agents/analyst/skills/private_account_intent.md
_PUBLIC_FALLBACK_PROMPT = """
You are a B2B account-based marketing analyst. Given the account engagement signals
below, provide a structured intelligence assessment. Your output should:

1. Identify the top 3–5 accounts showing the strongest purchase intent signals
   and explain WHY their behavior pattern suggests genuine buying intent.

2. Flag any accounts that appear in the high-engagement tier but are NOT currently
   in the active target account / ABM list — these are ICP misses the sales team
   should evaluate for inclusion.

3. For High Intent Surge accounts: assess what stage of the buying journey the
   visiting behavior suggests (awareness, evaluation, decision) and recommend
   the appropriate next activation (retargeting, SDR outreach, personalized
   nurture sequence, suppression from top-of-funnel).

4. For Early Discovery accounts: recommend what content or ad creative would
   deepen engagement — e.g., case studies for their vertical, product comparison
   content if they are visiting competitor pages.

5. Note any accounts where the corporate domain appears on multiple platforms
   (paid social + paid search + organic) in the same window — this multi-channel
   convergence is a strong buying committee signal.

Stick to what the data shows. Do not fabricate signals not present in the dataset.
Format your response as a brief executive summary (3–5 sentences) followed by
per-account bullets for the top flagged accounts.
""".strip()


def _resolve_prompt() -> tuple[str, str]:
    """Resolve ABM evaluation prompt via SkillResolver."""
    return _skill_resolver.resolve_skill_prompt(
        public_fallback_string=_PUBLIC_FALLBACK_PROMPT,
        private_filename="account_intent",
    )


# ── Noise filtering helpers ───────────────────────────────────────────────────

def _is_noise(row: dict) -> bool:
    """
    Return True if the account row should be excluded from the intelligence brief.

    Filters:
      - Consumer / ISP email domains
      - Low-confidence enrichments
      - Suppressed accounts (competitors, opted-out)
    """
    domain = (row.get("company_domain") or "").lower()
    if domain in _FREEMAIL_DOMAINS:
        return True
    confidence = row.get("enrichment_confidence") or 0.0
    if confidence < _MIN_CONFIDENCE and confidence > 0.0:
        # Allow 0.0 in case the field is NULL for CRM-imported accounts
        return True
    account_tier = (row.get("account_tier") or "").lower()
    if account_tier == "excluded":
        return True
    return False


# ── Surge score ───────────────────────────────────────────────────────────────

def _compute_surge_score(row: dict) -> float:
    """
    Compute a 0–100 Account Surge Score from engagement fields.

    Components:
      40%  Base intent score  (intent_score from company_engagement)
      20%  Session velocity   (session_growth_pct, capped at 100%)
      25%  High-intent page depth
      15%  Multi-channel paid exposure
    """
    # Base intent (0–40)
    intent_base = min(float(row.get("intent_score") or 0.0), 100.0) * 0.40

    # Velocity (0–20): 100% growth → 20 pts, negative → 0
    growth_pct = float(row.get("session_growth_pct") or 0.0)
    velocity = min(max(growth_pct / 5.0, 0.0), 20.0)

    # Page depth (0–25): pricing 5pts, demo 5pts, contact 4pts, docs 2pts each
    pricing  = min(int(row.get("pricing_page_sessions")  or 0), 5) * 5.0   # ≤25
    demo     = min(int(row.get("demo_page_sessions")     or 0), 5) * 5.0
    contact  = min(int(row.get("contact_page_sessions")  or 0), 5) * 4.0
    docs     = min(int(row.get("docs_sessions")          or 0), 5) * 2.0
    depth = min(pricing + demo + contact + docs, 25.0)

    # Multi-channel (0–15): 5 pts per paid platform, max 3 platforms
    paid_platforms = row.get("paid_platforms_seen") or []
    multi_channel = min(len(paid_platforms) * 5.0, 15.0)

    return round(min(intent_base + velocity + depth + multi_channel, 100.0), 1)


# ── Account tier classification ───────────────────────────────────────────────

_TIER_HIGH_INTENT   = "high_intent_surge"
_TIER_DISCOVERY     = "early_discovery"
_TIER_ICP_MISS      = "icp_boundary_breaker"


def _classify_tier(row: dict, surge_score: float) -> str:
    """
    Assign one of three intelligence tiers to an account.

    Priority order: high_intent_surge → icp_boundary_breaker → early_discovery.

    High Intent Surge:
      surge_score ≥ 65 AND has bottom-funnel page visits (pricing/demo/contact).
      Signals multiple cross-channel visits on conversion-critical pages.

    ICP Boundary Breakers:
      surge_score ≥ 45 AND account is NOT in the current target account list.
      Reveals engaged accounts outside existing ABM targeting — ICP misses.

    Early Discovery:
      All remaining accounts meeting the minimum session threshold.
      Consistent top-of-funnel activity from a qualified domain.
    """
    pricing  = int(row.get("pricing_page_sessions")  or 0)
    demo     = int(row.get("demo_page_sessions")     or 0)
    contact  = int(row.get("contact_page_sessions")  or 0)
    is_target = bool(row.get("is_target_account") or False)
    bottom_funnel_visit = (pricing + demo + contact) > 0

    if surge_score >= 65 and bottom_funnel_visit:
        return _TIER_HIGH_INTENT
    if surge_score >= 45 and not is_target:
        return _TIER_ICP_MISS
    return _TIER_DISCOVERY


# ── SQL builder ───────────────────────────────────────────────────────────────

def _build_intent_sql(
    lookback_days: int,
    min_page_views: int,
    target_industry: str | None,
) -> str:
    """
    Build the BigQuery SQL to retrieve account engagement signals.

    Uses company_engagement (rolling_30d period) joined to company_profiles
    and the most-recent target_account_activity snapshot for spike detection.

    Privacy guarantee: no raw IP addresses are selected; only company_domain
    and aggregated engagement counts are returned.
    """
    industry_filter = ""
    if target_industry:
        # Defensive: strip quotes to prevent SQL injection via the string param
        safe_industry = target_industry.replace("'", "").replace('"', "")[:100]
        industry_filter = f"AND LOWER(cp.industry) LIKE LOWER('%{safe_industry}%')"

    ce = bq.table_ref("company_engagement")
    cp = bq.table_ref("company_profiles")
    ta = bq.table_ref("target_account_activity")

    return f"""
WITH latest_engagement AS (
    -- Most recent rolling_30d window per company
    SELECT
        ce.company_id,
        ce.company_domain,
        ce.company_name,
        ce.period_start,
        ce.is_target_account,
        ce.account_tier,
        ce.crm_pipeline_stage,
        ce.crm_is_open_opportunity,
        ce.total_sessions,
        ce.total_page_views,
        ce.unique_session_days,
        ce.avg_pages_per_session,
        ce.pricing_page_sessions,
        ce.demo_page_sessions,
        ce.contact_page_sessions,
        ce.docs_sessions,
        ce.case_study_sessions,
        ce.blog_sessions,
        ce.paid_sessions,
        ce.paid_platforms_seen,
        ce.paid_campaigns_seen,
        ce.intent_score,
        ce.recency_score,
        ce.frequency_score,
        ce.depth_score,
        COALESCE(ce.session_growth_pct, 0.0) AS session_growth_pct,
        ce.is_suppressed_tofu,
        ce.suppression_reason
    FROM {ce} ce
    WHERE ce.period_type = 'rolling_30d'
      AND ce.period_start >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
      AND ce.total_sessions >= {min_page_views}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ce.company_id ORDER BY ce.period_start DESC) = 1
),

latest_spike AS (
    -- Most recent daily snapshot for intent spike flag
    SELECT
        company_id,
        intent_spiking,
        web_sessions_7d,
        visited_pricing_today,
        visited_demo_today,
        visited_contact_today,
        paid_touchpoints_30d,
        last_paid_touchpoint_at,
        last_paid_touchpoint_platform,
        coverage_completeness_score
    FROM {ta}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY date DESC) = 1
)

SELECT
    -- Identity
    ce.company_domain,
    ce.company_name,
    cp.industry,
    cp.sub_industry,
    cp.employee_range,
    cp.headquarters_country,
    cp.company_type,
    cp.enrichment_confidence,
    cp.icp_score,
    cp.crm_account_owner,

    -- ABM status
    ce.is_target_account,
    ce.account_tier,
    ce.crm_pipeline_stage,
    ce.crm_is_open_opportunity,
    ce.is_suppressed_tofu,
    ce.suppression_reason,

    -- Session engagement
    ce.total_sessions,
    ce.total_page_views,
    ce.unique_session_days,
    ce.avg_pages_per_session,
    ce.session_growth_pct,

    -- High-intent page signals
    ce.pricing_page_sessions,
    ce.demo_page_sessions,
    ce.contact_page_sessions,
    ce.docs_sessions,
    ce.case_study_sessions,
    ce.blog_sessions,

    -- Paid media
    ce.paid_sessions,
    ce.paid_platforms_seen,
    ce.paid_campaigns_seen,

    -- Pre-computed intent components
    ce.intent_score,
    ce.recency_score,
    ce.frequency_score,
    ce.depth_score,

    -- Spike signals from latest day snapshot
    COALESCE(ts.intent_spiking, FALSE)        AS intent_spiking,
    COALESCE(ts.web_sessions_7d, 0)           AS web_sessions_7d,
    COALESCE(ts.visited_pricing_today, FALSE) AS visited_pricing_today,
    COALESCE(ts.visited_demo_today, FALSE)    AS visited_demo_today,
    COALESCE(ts.visited_contact_today, FALSE) AS visited_contact_today,
    ts.paid_touchpoints_30d,
    ts.last_paid_touchpoint_at,
    ts.last_paid_touchpoint_platform,
    COALESCE(ts.coverage_completeness_score, 0.0) AS coverage_completeness_score

FROM latest_engagement ce
JOIN {cp} cp ON ce.company_id = cp.company_id
LEFT JOIN latest_spike ts ON ce.company_id = ts.company_id

-- Exclude noise
WHERE cp.account_tier != 'excluded'
  AND cp.is_active = TRUE
  {industry_filter}

ORDER BY ce.intent_score DESC, ce.total_sessions DESC
""".strip()


# ── Markdown brief formatter ──────────────────────────────────────────────────

_TIER_LABELS = {
    _TIER_HIGH_INTENT: "🚀 High Intent Surge",
    _TIER_DISCOVERY:   "🔍 Early Discovery",
    _TIER_ICP_MISS:    "⚠️ ICP Boundary Breakers",
}

_TIER_DESCRIPTIONS = {
    _TIER_HIGH_INTENT: (
        "Multiple cross-channel visits hitting critical conversion pages "
        "within a tight 72-hour window — high purchase intent signal."
    ),
    _TIER_DISCOVERY: (
        "Consistent top-of-funnel exploration activity from a qualified domain — "
        "early buyer journey, warming toward evaluation."
    ),
    _TIER_ICP_MISS: (
        "Highly engaged accounts that do NOT match current firmographic filters "
        "but exhibit genuine buying indicators — ICP miss candidates."
    ),
}


def _account_row_to_markdown(row: dict, surge_score: float) -> str:
    """Format a single account as a compact Markdown blockquote row."""
    domain = row.get("company_domain", "unknown")
    name   = row.get("company_name") or domain
    industry = row.get("industry") or "—"
    employees = row.get("employee_range") or "—"
    crm_stage = row.get("crm_pipeline_stage") or "not in CRM"
    sessions = int(row.get("total_sessions") or 0)
    growth   = row.get("session_growth_pct") or 0.0
    pricing  = int(row.get("pricing_page_sessions") or 0)
    demo     = int(row.get("demo_page_sessions") or 0)
    contact  = int(row.get("contact_page_sessions") or 0)
    docs     = int(row.get("docs_sessions") or 0)
    platforms = row.get("paid_platforms_seen") or []
    spiking  = bool(row.get("intent_spiking") or False)
    icp      = row.get("icp_score")
    owner    = row.get("crm_account_owner") or "—"

    # Build key signals line
    signals = []
    if pricing > 0:  signals.append(f"pricing ×{pricing}")
    if demo > 0:     signals.append(f"demo ×{demo}")
    if contact > 0:  signals.append(f"contact ×{contact}")
    if docs > 0:     signals.append(f"docs ×{docs}")
    if spiking:      signals.append("⚡ spiking")
    if platforms:    signals.append(f"paid: {', '.join(platforms)}")
    signals_str = " · ".join(signals) if signals else "top-of-funnel only"

    icp_str = f"{icp:.0f}" if icp is not None else "—"
    growth_str = f"+{growth:.0f}%" if growth >= 0 else f"{growth:.0f}%"

    lines = [
        f"**{name}** (`{domain}`)",
        f"> {industry} | {employees} employees | ICP score: {icp_str}",
        f"> Surge score: **{surge_score}** | Sessions: {sessions} ({growth_str} vs prior period)",
        f"> Key page signals: {signals_str}",
        f"> CRM stage: {crm_stage} | AE: {owner}",
        "",
    ]
    return "\n".join(lines)


def _build_markdown_brief(
    tier_accounts: dict[str, list[tuple[dict, float]]],
    params: dict,
    prompt_source: str,
) -> str:
    """
    Assemble the full Markdown intelligence brief.

    Structure:
      Header → tier totals → per-tier blockquote sections (top 10 per tier)
      Footer → filter params + prompt source tag
    """
    total = sum(len(v) for v in tier_accounts.values())
    high_n   = len(tier_accounts.get(_TIER_HIGH_INTENT, []))
    disc_n   = len(tier_accounts.get(_TIER_DISCOVERY, []))
    miss_n   = len(tier_accounts.get(_TIER_ICP_MISS, []))

    lines: list[str] = [
        "## ABM Intent Signal Intelligence Brief",
        "",
        f"**{total} accounts detected** across {params['lookback_days']}-day lookback "
        f"(min {params['min_page_views']} sessions)"
        + (f" · industry filter: `{params['target_industry']}`" if params.get('target_industry') else ""),
        "",
        "| Tier | Count | Description |",
        "|------|-------|-------------|",
        f"| 🚀 High Intent Surge | {high_n} | Bottom-funnel page visits + multi-channel exposure |",
        f"| 🔍 Early Discovery   | {disc_n} | Consistent TOFU activity from qualified domain |",
        f"| ⚠️  ICP Boundary Breakers | {miss_n} | High engagement outside current target list |",
        "",
    ]

    for tier_key in [_TIER_HIGH_INTENT, _TIER_ICP_MISS, _TIER_DISCOVERY]:
        accounts = tier_accounts.get(tier_key, [])
        if not accounts:
            continue

        label = _TIER_LABELS[tier_key]
        desc  = _TIER_DESCRIPTIONS[tier_key]

        lines += [
            "---",
            "",
            f"### {label}",
            "",
            f"> {desc}",
            "",
        ]

        # Show top 10 per tier, sorted by surge score desc
        for row, surge in sorted(accounts, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(_account_row_to_markdown(row, surge))

        if len(accounts) > 10:
            lines.append(
                f"> *…and {len(accounts) - 10} additional accounts in this tier. "
                f"Narrow `target_industry` or reduce `lookback_days` to focus the result set.*"
            )
            lines.append("")

    lines += [
        "---",
        "",
        "*Evaluation framework: "
        + ("`Extended Secure Framework` (private heuristics active)" if prompt_source == "private"
           else "`Standard Open Core Engine` (public fallback)")
        + " | Tables: `company_engagement`, `company_profiles`, `target_account_activity`*",
    ]

    return "\n".join(lines)


# ── Main inspector class ──────────────────────────────────────────────────────

class AccountAnalyticsInspector:
    """
    ABM intent signal detection engine.

    Queries the account analytics schema layer, scores accounts by engagement
    velocity and page depth, classifies them into intelligence tiers, and returns
    a Markdown intelligence brief ready for the Analyst agent's response.

    Usage:
        inspector = AccountAnalyticsInspector()
        result = inspector.get_intent_signals(
            lookback_days=30,
            min_page_views=3,
            target_industry="Software",
        )
        # result["markdown_brief"]    → formatted Markdown brief
        # result["accounts"]          → raw account rows
        # result["tier_summary"]      → {tier_name: count}
        # result["evaluation_context"] → resolved SkillResolver prompt
        # result["prompt_source"]      → "private" | "public_fallback"
    """

    def get_intent_signals(
        self,
        lookback_days: int = 30,
        min_page_views: int = 3,
        target_industry: str | None = None,
    ) -> dict:
        """
        Execute the full ABM intent signal pipeline.

        Args:
            lookback_days:   Days of engagement data to include (default 30).
                             Longer windows surface accounts earlier in the journey;
                             shorter windows surface only recent surges.
            min_page_views:  Minimum total sessions to include an account
                             (default 3 — filters single-visit bot-like traffic).
            target_industry: Optional industry filter substring, e.g. "Software",
                             "Financial Services". Case-insensitive LIKE match.

        Returns:
            dict with keys:
                markdown_brief     — formatted Markdown intelligence brief
                accounts           — list of raw account dicts with surge_score + tier
                tier_summary       — {tier_name: int count}
                evaluation_context — resolved SkillResolver prompt text
                prompt_source      — "private" | "public_fallback"
                params             — echo of input parameters
                status             — "ok" | "no_data" | "bq_error"
        """
        params = {
            "lookback_days":   lookback_days,
            "min_page_views":  min_page_views,
            "target_industry": target_industry,
        }

        # ── Resolve evaluation prompt ─────────────────────────────────────────
        evaluation_context, prompt_source = _resolve_prompt()

        # ── Build and run query ───────────────────────────────────────────────
        try:
            sql = _build_intent_sql(lookback_days, min_page_views, target_industry)
            rows = bq.run_query(sql)
        except Exception as exc:
            log.error("account_analytics.query_failed", error=str(exc))
            return {
                "status":        "bq_error",
                "error":         str(exc),
                "markdown_brief": (
                    "**BigQuery error** — account analytics tables may not be populated yet.\n\n"
                    "Run `enrich_sessions` to populate `company_sessions` and `company_engagement`, "
                    "then retry.\n\n"
                    f"```\n{exc}\n```"
                ),
                "accounts":          [],
                "tier_summary":      {},
                "evaluation_context": evaluation_context,
                "prompt_source":      prompt_source,
                "params":             params,
            }

        if not rows:
            return {
                "status":        "no_data",
                "markdown_brief": (
                    "## ABM Intent Signal Intelligence Brief\n\n"
                    f"_No accounts found matching your filters (lookback: {lookback_days}d, "
                    f"min sessions: {min_page_views}"
                    + (f", industry: {target_industry}" if target_industry else "")
                    + ")._\n\n"
                    "**To populate this data:** run the `enrich_sessions` tool to resolve "
                    "IP addresses to company domains and build the engagement aggregation tables."
                ),
                "accounts":          [],
                "tier_summary":      {},
                "evaluation_context": evaluation_context,
                "prompt_source":      prompt_source,
                "params":             params,
            }

        # ── Filter noise, score, classify ─────────────────────────────────────
        tier_accounts: dict[str, list[tuple[dict, float]]] = {
            _TIER_HIGH_INTENT: [],
            _TIER_DISCOVERY:   [],
            _TIER_ICP_MISS:    [],
        }
        enriched_accounts: list[dict] = []

        for row in rows:
            if _is_noise(row):
                continue

            surge = _compute_surge_score(row)
            tier  = _classify_tier(row, surge)

            enriched_row = dict(row)
            enriched_row["surge_score"] = surge
            enriched_row["intelligence_tier"] = tier

            tier_accounts[tier].append((enriched_row, surge))
            enriched_accounts.append(enriched_row)

        tier_summary = {tier: len(accts) for tier, accts in tier_accounts.items()}

        log.info(
            "account_analytics.signals_computed",
            total=len(enriched_accounts),
            high_intent=tier_summary[_TIER_HIGH_INTENT],
            discovery=tier_summary[_TIER_DISCOVERY],
            icp_miss=tier_summary[_TIER_ICP_MISS],
            prompt_source=prompt_source,
        )

        # ── Format Markdown brief ─────────────────────────────────────────────
        markdown_brief = _build_markdown_brief(tier_accounts, params, prompt_source)

        return {
            "status":             "ok",
            "markdown_brief":     markdown_brief,
            "accounts":           enriched_accounts,
            "tier_summary":       tier_summary,
            "evaluation_context": evaluation_context,
            "prompt_source":      prompt_source,
            "params":             params,
        }
