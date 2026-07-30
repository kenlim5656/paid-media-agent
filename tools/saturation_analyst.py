# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Saturation Analyst — Channel Spend Saturation Engine.

Computes per-channel saturation levels from Meridian MMM posterior outputs,
applies Hill function diminishing return curves, and generates structured
briefings for the Analyst agent and machine-readable recommendation vectors
for the Operator agent.

Architecture
────────────
    SaturationAnalyst
        ├── resolve_prompt()          — SkillResolver: private rules or public fallback
        ├── fetch_mmm_data()          — pulls mmm_channel_contributions (latest run)
        ├── fetch_current_spend()     — pulls last-30-day spend from platform_daily_spend
        ├── load_channel_params()     — parses JSON config block from private skill file
        ├── compute_saturation()      — Hill function: sat_pct, marginal_roi per channel
        ├── render_markdown_brief()   — scannable table + health badges
        └── build_operator_vector()   — machine-readable JSON for Operator agent

Open Core Isolation
────────────────────
PUBLIC_FALLBACK_PROMPT contains only general diminishing-return heuristics.
Private scaling thresholds (EC50, alpha, CPA corridors, 85% rule) live exclusively
in agents/analyst/skills/private_saturation_rules.md and are loaded at runtime
via SkillResolver — never committed to the open repo.

Hill Function Reference
───────────────────────
  Response:  r(x) = r_max * x^alpha / (EC50^alpha + x^alpha)
  Saturation %: sat_pct = 100 * x^alpha / (EC50^alpha + x^alpha)
  Marginal ROI: ≈ roi_mean * alpha * (1 - sat_pct/100) * (sat_pct/100 if alpha>1 else 1)
  Inflection:  x_inf = EC50 * ((alpha-1)/(alpha+1))^(1/alpha)  [only when alpha > 1]

Usage
─────
  from tools.saturation_analyst import SaturationAnalyst
  analyst = SaturationAnalyst()
  result = analyst.run(target_channels=["google_ads", "linkedin"])
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import structlog

from tools import bigquery_client as bq
from tools.skill_resolver import SkillResolver

log = structlog.get_logger()

# ── Public fallback prompt ────────────────────────────────────────────────────
# Fully functional for open-source deployments. Does not expose proprietary
# channel thresholds, EC50 values, or CPA corridors.

PUBLIC_FALLBACK_PROMPT = """
You are a paid media saturation analyst. Evaluate channel spend levels using the
following general diminishing return framework.

## Saturation Analysis Framework (Standard)

### Hill Function Interpretation
Spend efficiency follows a Hill saturation curve:
  r(x) = r_max * x^alpha / (EC50^alpha + x^alpha)

Where:
- x = current spend level
- EC50 = spend at which 50% of maximum response is achieved
- alpha = curve steepness (higher = faster diminishing returns)
- r_max = theoretical maximum response at infinite spend

### General Diminishing Return Thresholds
- Below 50% saturation: Spend is in the efficient scaling zone. CPA should be
  within 10% of baseline. Recommend maintaining or modestly increasing investment.
- 50–75% saturation: Normal operating range for mature campaigns. CPA degradation
  of up to 20–25% is acceptable. Monitor closely but no action required unless
  budget reallocation opportunity exists.
- 75–85% saturation: Approaching ceiling. Marginal returns are declining rapidly.
  Evaluate whether incremental budget has a better home in a lower-saturation channel.
- Above 85% saturation: Freeze budget expansion. Every additional dollar is likely
  generating sub-threshold returns. Redirect to highest-ROI underspent channel.

### Health Status Assignment
- ⚖️ Optimal: saturation < 75% AND marginal ROI > 1.0
- ⚠️ Saturated: saturation 75–85% OR marginal ROI 0.7–1.0
- 🛑 Diminishing: saturation > 85% OR marginal ROI < 0.7

### Marginal ROI Approximation
When full Hill function parameters are unavailable, use:
  marginal_roi ≈ roi_mean * (1 - saturation_pct / 100)

This approximates the expected return on the next incremental dollar of spend.

### Output Format
Always produce:
1. A markdown table with: Channel | Spend | Saturation % | Marginal ROI | Status
2. A JSON recommendation vector for the Operator agent with: channel, action,
   rationale, recommended_cap_usd, reallocation_target, urgency
""".strip()


# ── Default channel parameters (used when private file is absent or unparseable) ──

_DEFAULT_CHANNEL_PARAMS: dict[str, dict[str, Any]] = {
    "google_ads":  {"alpha": 1.8, "ec50_monthly_usd": 38_000, "max_efficient_monthly_usd": 95_000, "cpa_target_usd": 420,  "cpa_tolerance_pct": 22},
    "linkedin":    {"alpha": 1.1, "ec50_monthly_usd": 22_000, "max_efficient_monthly_usd": 55_000, "cpa_target_usd": 680,  "cpa_tolerance_pct": 30},
    "meta":        {"alpha": 1.75,"ec50_monthly_usd": 38_000, "max_efficient_monthly_usd": 80_000, "cpa_target_usd": 310,  "cpa_tolerance_pct": 25},
    "tiktok":      {"alpha": 2.1, "ec50_monthly_usd": 18_000, "max_efficient_monthly_usd": 45_000, "cpa_target_usd": 290,  "cpa_tolerance_pct": 35},
    "reddit_ads":  {"alpha": 0.9, "ec50_monthly_usd":  7_500, "max_efficient_monthly_usd": 18_000, "cpa_target_usd": 380,  "cpa_tolerance_pct": 40},
}


class SaturationAnalyst:
    """
    Computes channel saturation metrics from Meridian MMM posteriors and generates
    structured briefs for the Analyst and Operator agents.
    """

    def __init__(self) -> None:
        self._resolver = SkillResolver()
        self._prompt: str | None = None
        self._prompt_source: str | None = None
        self._channel_params: dict[str, dict[str, Any]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, target_channels: list[str] | None = None) -> dict[str, Any]:
        """
        Execute the full saturation analysis pipeline.

        Args:
            target_channels: List of channel names to analyse. None = all channels
                             present in the latest MMM run.

        Returns:
            dict with keys:
                prompt_source    — "private" | "public_fallback"
                channels_analysed — list of channel names
                saturation_data  — list of per-channel metric dicts
                markdown_brief   — rendered markdown table + narrative
                operator_vector  — list of JSON recommendation dicts for Operator agent
                run_id           — MMM run_id this analysis is based on
                analysed_at      — ISO timestamp
        """
        # Step 1: Resolve prompt + load channel params
        self._prompt, self._prompt_source = self._resolver.resolve_skill_prompt(
            public_fallback_string=PUBLIC_FALLBACK_PROMPT,
            private_filename="saturation_rules",
        )
        self._channel_params = self._load_channel_params(self._prompt)

        # Step 2: Fetch MMM posterior data
        mmm_rows = self._fetch_mmm_data()
        if not mmm_rows:
            return {
                "error": "No completed MMM runs found in mmm_channel_contributions. "
                         "Run run_mmm_model() first to generate saturation inputs.",
                "prompt_source": self._prompt_source,
            }

        run_id = mmm_rows[0].get("run_id", "unknown")

        # Step 3: Fetch current 30-day spend per channel
        spend_by_channel = self._fetch_current_spend()

        # Step 4: Filter to target_channels if specified
        if target_channels:
            mmm_rows = [r for r in mmm_rows if r.get("channel") in target_channels]
            if not mmm_rows:
                return {
                    "error": f"No MMM data found for requested channels: {target_channels}",
                    "available_channels": [r.get("channel") for r in self._fetch_mmm_data()],
                }

        # Step 5: Compute saturation metrics per channel
        saturation_data = [
            self._compute_saturation(row, spend_by_channel)
            for row in mmm_rows
        ]

        # Step 6: Render outputs
        markdown_brief = self._render_markdown_brief(saturation_data)
        operator_vector = self._build_operator_vector(saturation_data)

        log.info(
            "saturation_analyst.complete",
            channels=len(saturation_data),
            prompt_source=self._prompt_source,
            run_id=run_id,
            saturated_count=sum(1 for d in saturation_data if d["status"] != "optimal"),
        )

        return {
            "prompt_source":      self._prompt_source,
            "channels_analysed":  [d["channel"] for d in saturation_data],
            "saturation_data":    saturation_data,
            "markdown_brief":     markdown_brief,
            "operator_vector":    operator_vector,
            "run_id":             run_id,
            "analysed_at":        datetime.now(timezone.utc).isoformat(),
        }

    # ── Data fetching ─────────────────────────────────────────────────────────

    def _fetch_mmm_data(self) -> list[dict]:
        """Pull the latest completed MMM run's channel contributions."""
        try:
            rows = bq.run_query(f"""
                SELECT
                    cc.contribution_id,
                    cc.run_id,
                    cc.channel,
                    CAST(cc.total_spend_usd AS FLOAT64) AS total_spend_usd,
                    cc.total_impressions,
                    cc.roi_mean,
                    cc.roi_p5,
                    cc.roi_p50,
                    cc.roi_p95,
                    cc.contribution_pct,
                    cc.roi_prior_injected,
                    cc.roi_prior_mu,
                    cc.roi_prior_sigma,
                    cc.created_at
                FROM {bq.table_ref('mmm_channel_contributions')} cc
                INNER JOIN (
                    SELECT run_id
                    FROM {bq.table_ref('mmm_runs')}
                    WHERE status IN ('completed', 'converged')
                    ORDER BY run_started_at DESC
                    LIMIT 1
                ) latest USING (run_id)
                ORDER BY cc.contribution_pct DESC
            """)
            return [dict(r) for r in rows]
        except Exception as exc:
            log.warning("saturation_analyst.mmm_fetch_error", error=str(exc)[:200])
            return []

    def _fetch_current_spend(self, lookback_days: int = 30) -> dict[str, float]:
        """Return total spend per channel for the last N days from platform_daily_spend."""
        try:
            rows = bq.run_query(f"""
                SELECT
                    platform AS channel,
                    SUM(CAST(spend AS FLOAT64)) AS spend_30d
                FROM {bq.table_ref('platform_daily_spend')}
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                GROUP BY platform
            """)
            return {str(r["channel"]): float(r.get("spend_30d") or 0) for r in rows}
        except Exception as exc:
            log.warning("saturation_analyst.spend_fetch_error", error=str(exc)[:200])
            return {}

    # ── Parameter loading ─────────────────────────────────────────────────────

    def _load_channel_params(self, prompt_text: str) -> dict[str, dict[str, Any]]:
        """
        Extract the JSON config block embedded in the private skill markdown.
        Falls back to _DEFAULT_CHANNEL_PARAMS if the block is absent or malformed.

        The JSON block is delimited by triple-backtick json fences in the .md file.
        Stack Leak Protection: only channel params (alpha, EC50, CPA targets) are
        extracted. The full private text is not stored or logged.
        """
        try:
            match = re.search(r"```json\s*(\{.*?\})\s*```", prompt_text, re.DOTALL)
            if match:
                parsed = json.loads(match.group(1))
                channels = parsed.get("channels", {})
                if channels:
                    log.info(
                        "saturation_analyst.params_loaded",
                        source="private_skill",
                        channel_count=len(channels),
                    )
                    return channels
        except (json.JSONDecodeError, AttributeError) as exc:
            log.warning("saturation_analyst.param_parse_error", error=str(exc))

        log.info("saturation_analyst.params_loaded", source="defaults")
        return _DEFAULT_CHANNEL_PARAMS

    # ── Core computation ──────────────────────────────────────────────────────

    def _compute_saturation(
        self,
        mmm_row: dict[str, Any],
        spend_by_channel: dict[str, float],
    ) -> dict[str, Any]:
        """
        Compute Hill function saturation metrics for one channel.

        Saturation %:  100 * x^alpha / (EC50^alpha + x^alpha)
        Marginal ROI:  roi_mean * alpha * (EC50/x)^alpha / (1 + (EC50/x)^alpha)^2
                       (derivative of Hill function, scaled by posterior roi_mean)
        Inflection:    EC50 * ((alpha-1)/(alpha+1))^(1/alpha)  [alpha > 1 only]
        """
        channel      = str(mmm_row.get("channel", "unknown"))
        roi_mean     = float(mmm_row.get("roi_mean") or 0)
        roi_p5       = float(mmm_row.get("roi_p5")  or 0)
        roi_p95      = float(mmm_row.get("roi_p95") or 0)
        prior_mu     = float(mmm_row.get("roi_prior_mu") or roi_mean)
        contrib_pct  = float(mmm_row.get("contribution_pct") or 0) * 100

        # Use current 30-day spend; fall back to MMM window spend ÷ 3 (90-day window)
        current_monthly = spend_by_channel.get(channel)
        if current_monthly is None:
            mmm_total = float(mmm_row.get("total_spend_usd") or 0)
            current_monthly = mmm_total / 3.0

        params     = self._channel_params.get(channel, _DEFAULT_CHANNEL_PARAMS.get(channel, {}))
        alpha      = float(params.get("alpha", 1.5))
        ec50       = float(params.get("ec50_monthly_usd", 30_000))
        max_eff    = float(params.get("max_efficient_monthly_usd", ec50 * 2.5))
        cpa_target = float(params.get("cpa_target_usd", 500))
        cpa_tol    = float(params.get("cpa_tolerance_pct", 25))

        # ── Hill function saturation % ────────────────────────────────────────
        x = max(current_monthly, 1.0)
        x_alpha   = x ** alpha
        ec50_alpha = ec50 ** alpha
        sat_pct   = round(100.0 * x_alpha / (ec50_alpha + x_alpha), 1)

        # ── Marginal ROI (Hill derivative × posterior mean) ────────────────────
        # d/dx [r_max * x^a / (EC50^a + x^a)] at current x:
        # = r_max * a * EC50^a * x^(a-1) / (EC50^a + x^a)^2
        # Normalised by x to get "return per next dollar":
        ec50_ratio = ec50 / x
        denom      = (1.0 + ec50_ratio ** alpha) ** 2
        marginal_scale = alpha * (ec50_ratio ** alpha) / denom
        marginal_roi   = round(roi_mean * marginal_scale, 3)

        # ── Inflection point ──────────────────────────────────────────────────
        if alpha > 1.0:
            inflection_usd = round(ec50 * ((alpha - 1) / (alpha + 1)) ** (1.0 / alpha))
        else:
            inflection_usd = None  # convex curve — no mathematical inflection

        # ── 85% saturation rule threshold ─────────────────────────────────────
        threshold_85_usd = round(max_eff * 0.85)
        above_85_rule    = current_monthly >= threshold_85_usd

        # ── CPA tolerance at current saturation ───────────────────────────────
        if sat_pct < 50:
            cpa_ceiling = cpa_target * 1.10
        elif sat_pct < 75:
            cpa_ceiling = cpa_target * (1 + cpa_tol / 100)
        elif sat_pct < 85:
            cpa_ceiling = cpa_target * (1 + cpa_tol / 100 * 1.5)
        else:
            cpa_ceiling = cpa_target * (1 + cpa_tol / 100 * 2.0)  # ceiling irrelevant — freeze

        # ── CI width as uncertainty flag ──────────────────────────────────────
        ci_ratio        = (roi_p95 / roi_p5) if roi_p5 > 0 else 99.0
        wide_ci_warning = ci_ratio > 4.0

        # ── Health status ──────────────────────────────────────────────────────
        if sat_pct > 85 or marginal_roi < 0.70:
            status = "diminishing"
            badge  = "🛑 Diminishing"
        elif sat_pct > 75 or marginal_roi < 1.00:
            status = "saturated"
            badge  = "⚠️ Saturated"
        else:
            status = "optimal"
            badge  = "⚖️ Optimal"

        # Override: 85% rule always forces diminishing regardless of marginal_roi
        if above_85_rule:
            status = "diminishing"
            badge  = "🛑 Diminishing"

        return {
            "channel":               channel,
            "current_monthly_spend": round(current_monthly, 2),
            "roi_mean":              roi_mean,
            "roi_p5":                roi_p5,
            "roi_p95":               roi_p95,
            "roi_prior_mu":          prior_mu,
            "contribution_pct":      round(contrib_pct, 1),
            "saturation_pct":        sat_pct,
            "marginal_roi":          marginal_roi,
            "inflection_usd":        inflection_usd,
            "threshold_85_usd":      threshold_85_usd,
            "above_85_rule":         above_85_rule,
            "cpa_target_usd":        cpa_target,
            "cpa_ceiling_usd":       round(cpa_ceiling, 2),
            "wide_ci_warning":       wide_ci_warning,
            "status":                status,
            "badge":                 badge,
            "alpha":                 alpha,
            "ec50_monthly_usd":      ec50,
        }

    # ── Output rendering ──────────────────────────────────────────────────────

    def _render_markdown_brief(self, data: list[dict[str, Any]]) -> str:
        """Render the scannable markdown performance brief."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Sort: diminishing first, then saturated, then optimal
        order = {"diminishing": 0, "saturated": 1, "optimal": 2}
        sorted_data = sorted(data, key=lambda d: (order.get(d["status"], 9), -d["saturation_pct"]))

        # Summary counts
        n_dim = sum(1 for d in data if d["status"] == "diminishing")
        n_sat = sum(1 for d in data if d["status"] == "saturated")
        n_opt = sum(1 for d in data if d["status"] == "optimal")

        lines = [
            f"## 📊 Channel Saturation Brief — {now}",
            "",
            f"**{len(data)} channels analysed** · "
            f"🛑 {n_dim} Diminishing · ⚠️ {n_sat} Saturated · ⚖️ {n_opt} Optimal",
            "",
            "| Channel | Monthly Spend | Saturation | Marginal ROI | Posterior ROI | Status |",
            "|---|---|---|---|---|---|",
        ]

        for d in sorted_data:
            spend_fmt   = f"${d['current_monthly_spend']:,.0f}"
            sat_fmt     = f"{d['saturation_pct']:.1f}%"
            mroi_fmt    = f"{d['marginal_roi']:.3f}"
            proi_fmt    = f"{d['roi_mean']:.2f} [{d['roi_p5']:.2f}–{d['roi_p95']:.2f}]"
            warn        = " ⚡" if d["wide_ci_warning"] else ""
            lines.append(
                f"| **{d['channel']}** | {spend_fmt} | {sat_fmt} | {mroi_fmt} | {proi_fmt}{warn} | {d['badge']} |"
            )

        lines += [
            "",
            "> ⚡ = wide posterior CI (roi_p95/roi_p5 > 4×) — inject incrementality priors to tighten.",
            "",
            "---",
            "",
            "### Channel Detail",
            "",
        ]

        for d in sorted_data:
            lines.append(f"#### {d['badge']} {d['channel'].replace('_', ' ').title()}")
            lines.append(f"- **Saturation:** {d['saturation_pct']:.1f}% of EC50 response "
                         f"(EC50 = ${d['ec50_monthly_usd']:,}/mo, alpha = {d['alpha']})")
            lines.append(f"- **Current spend:** ${d['current_monthly_spend']:,.0f}/mo")
            lines.append(f"- **Marginal ROI:** {d['marginal_roi']:.3f} "
                         f"(next incremental $1K returns ${d['marginal_roi']*1000:,.0f} in pipeline value)")
            lines.append(f"- **Posterior ROI:** {d['roi_mean']:.2f} "
                         f"[90% CI: {d['roi_p5']:.2f} – {d['roi_p95']:.2f}]")
            if d["inflection_usd"]:
                above_inf = d["current_monthly_spend"] > d["inflection_usd"]
                lines.append(f"- **Hill inflection:** ${d['inflection_usd']:,}/mo "
                             f"({'⚠️ already past inflection' if above_inf else '✓ below inflection'})")
            else:
                lines.append("- **Hill inflection:** N/A — convex curve (alpha < 1); monitor CPM trend instead")
            lines.append(f"- **CPA ceiling at saturation:** ${d['cpa_ceiling_usd']:,.0f} "
                         f"(target: ${d['cpa_target_usd']:,.0f})")
            if d["above_85_rule"]:
                lines.append(f"- **🛑 85% Rule triggered** — spend exceeds ${d['threshold_85_usd']:,}/mo cap. "
                             "Budget expansion frozen.")
            lines.append("")

        return "\n".join(lines)

    def _build_operator_vector(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Build a machine-readable JSON recommendation vector for the Operator agent.

        Each dict corresponds to one channel action. The Operator agent reads this
        to queue budget cap proposals in operator_pending_approvals.
        """
        now_iso = datetime.now(timezone.utc).isoformat()

        # Find the best reallocation target: lowest saturation, highest marginal ROI
        optimal = sorted(
            [d for d in data if d["status"] == "optimal"],
            key=lambda d: (-d["marginal_roi"], d["saturation_pct"]),
        )
        best_target = optimal[0]["channel"] if optimal else None

        vector: list[dict[str, Any]] = []

        for d in data:
            channel = d["channel"]
            spend   = d["current_monthly_spend"]
            sat     = d["saturation_pct"]
            mroi    = d["marginal_roi"]
            status  = d["status"]

            if status == "diminishing":
                # Hard cap: recommend reducing to 85% threshold
                cap_usd = d["threshold_85_usd"]
                excess  = max(0.0, spend - cap_usd)
                vector.append({
                    "channel":                  channel,
                    "action":                   "cap_budget",
                    "urgency":                  "high",
                    "current_monthly_spend_usd": round(spend, 2),
                    "recommended_cap_usd":       cap_usd,
                    "reallocation_amount_usd":   round(excess, 2),
                    "reallocation_target":       best_target,
                    "saturation_pct":            sat,
                    "marginal_roi":              mroi,
                    "rationale": (
                        f"{channel} is {sat:.1f}% saturated (85% rule threshold: "
                        f"${cap_usd:,}/mo). Marginal ROI of {mroi:.3f} indicates "
                        f"each additional $1K generates only ${mroi*1000:,.0f} in value. "
                        f"Cap spend at ${cap_usd:,}/mo and reallocate ${excess:,.0f} "
                        f"to {best_target or 'next-best channel'}."
                    ),
                    "generated_at": now_iso,
                })

            elif status == "saturated":
                # Soft warning: freeze expansion, no active cut yet
                vector.append({
                    "channel":                  channel,
                    "action":                   "freeze_budget_expansion",
                    "urgency":                  "medium",
                    "current_monthly_spend_usd": round(spend, 2),
                    "recommended_cap_usd":       round(spend, 2),  # hold current
                    "reallocation_amount_usd":   0.0,
                    "reallocation_target":       None,
                    "saturation_pct":            sat,
                    "marginal_roi":              mroi,
                    "rationale": (
                        f"{channel} is approaching saturation ({sat:.1f}%). "
                        f"Marginal ROI of {mroi:.3f} is declining. "
                        "Hold budget flat. Do not approve any incremental budget requests "
                        "for this channel until saturation drops below 75%."
                    ),
                    "generated_at": now_iso,
                })

            else:  # optimal
                # No action — but flag as reallocation candidate if budget is freed elsewhere
                vector.append({
                    "channel":                  channel,
                    "action":                   "hold",
                    "urgency":                  "low",
                    "current_monthly_spend_usd": round(spend, 2),
                    "recommended_cap_usd":       None,
                    "reallocation_amount_usd":   0.0,
                    "reallocation_target":       None,
                    "saturation_pct":            sat,
                    "marginal_roi":              mroi,
                    "rationale": (
                        f"{channel} is in the efficient zone ({sat:.1f}% saturation, "
                        f"marginal ROI {mroi:.3f}). Suitable reallocation target if "
                        "budget is freed from saturated channels."
                    ),
                    "generated_at": now_iso,
                })

        return vector
