# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
MMM Optimizer Analyst — Meridian model output analysis and budget optimization (Task 27).

Reads pre-computed Meridian posterior summaries from BigQuery and translates them
into an actionable media-mix optimization brief with Operator-ready budget shift
directives. Does NOT run the JAX/NumPyro MCMC chain inline (delegated to
_tool_run_mmm_model / run_mmm_pipeline). This design avoids Cloud Run thread
blocking from 35–45 minute sampling runs.

Architecture
────────────
  mmm_channel_contributions          ← posterior ROI summaries (from Meridian run)
  mmm_runs                           ← convergence diagnostics (R-hat, ESS)
  v_attribution_correction_weights   ← Task 37 data quality multipliers
  causal_impact_metrics              ← Task 24 BSTS trend cross-reference
         │
         ▼
  MMMOptimizerAnalyst.run_optimization()
  ┌──────────────────────────────────────────────────────────────────┐
  │  1. Load latest MMM run + channel posteriors                     │
  │  2. Load Task 37 correction weights → adjust ROI estimates       │
  │  3. Load Task 24 BSTS results → counterfactual cross-reference   │
  │  4. Compute optimal spend allocation (Dorfman gradient method)   │
  │     — capped at MAX_BUDGET_SHIFT_PCT=10% per security policy     │
  │  5. Resolve SkillResolver prompt (private_meridian_priors.md)    │
  │  6. Format Markdown allocation package (3 sections)              │
  │  7. Build Operator execution JSON payload                        │
  └──────────────────────────────────────────────────────────────────┘
         │
         ▼
  {markdown_brief, operator_execution_package, evaluation_context, ...}

Budget Optimization (Dorfman Gradient)
───────────────────────────────────────
  Portfolio-weighted average ROI = Σ(spend_i × adj_roi_i) / Σ(spend_i)
  adj_roi_i = roi_mean_i × correction_multiplier_i   (Task 37 integration)
  gap_i     = (adj_roi_i − avg_roi) / avg_roi
  shift_pct = clamp(gap_i × 50%, −MAX_SHIFT, +MAX_SHIFT)

  Channels above portfolio avg ROI → positive gap → budget increase
  Channels below portfolio avg ROI → negative gap → budget decrease
  Hard cap: MAX_BUDGET_SHIFT_PCT = 10% in either direction (security constraint)
  OPERATOR_REQUIRE_APPROVAL = True on all generated directives

Private skills file
───────────────────
  Path:    agents/analyst/skills/private_meridian_priors.md
  Purpose: Proprietary channel saturation priors, counterfactual audit framework,
           and Operator reallocation vector heuristics.
  Git:     gitignored, never committed.  Falls back to public_fallback_prompt.

Security constraints (non-negotiable):
  • MAX_BUDGET_SHIFT_PCT = 10.0 — hard cap per shift directive
  • requires_operator_approval = True on all Operator payloads
  • No write actions executed inline — Operator agent must confirm
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog

from tools import bigquery_client as bq
from tools.skill_resolver import SkillResolver

log = structlog.get_logger()

# ── Security constraint ────────────────────────────────────────────────────────
MAX_BUDGET_SHIFT_PCT = 10.0   # hard cap: never shift more than 10% per directive

# ── Skill resolver ─────────────────────────────────────────────────────────────
_skill_resolver = SkillResolver()

# ── Public fallback evaluation prompt ─────────────────────────────────────────
# Fully functional for open-source deployments.
# Guides standard Bayesian MMM interpretation without proprietary heuristics.
# For extended priors, counterfactual audit, and reallocation heuristics, place
# private_meridian_priors.md at agents/analyst/skills/private_meridian_priors.md
_PUBLIC_FALLBACK_PROMPT = """
You are a media mix modeling analyst interpreting Bayesian posterior output from
Google Meridian. Your task is to translate posterior ROI summaries into actionable
budget allocation recommendations.

Core interpretation framework:

1. CONVERGENCE CHECK: Assess model quality before interpreting results.
   - R-hat max < 1.1 → chains converged → estimates are reliable
   - R-hat > 1.1 → flag for re-run with more draws; don't act on diverged estimates
   - ESS bulk min < 100 per chain → uncertain estimates → recommend incrementality test
   - Divergences > 0 → note in brief; minor divergences are acceptable

2. ROI READING: Interpret posterior ROI as expected return per dollar of media spend.
   - roi_mean: the central estimate — use this for decisions
   - roi_p5 to roi_p95: the 90% Bayesian credible interval
   - Wide CI (roi_p95/roi_p5 > 5) → low certainty → recommend more data or a geo holdout
   - roi_mean < 1.0 → spending exceeds return on this channel → strong reduce signal

3. SPEND EFFICIENCY ANALYSIS:
   - Compare roi_mean × spend_share to contribution_pct for each channel
   - Channels where contribution_pct >> spend_share are over-performing relative to budget
   - Channels where spend_share >> contribution_pct are under-performing — investigate
   - Apply Task 37 attribution correction multipliers to adjust raw ROI estimates before
     comparing channels: adj_roi = roi_mean × correction_multiplier

4. BUDGET REALLOCATION RATIONALE:
   - Recommend shifting budget from low-adj-ROI to high-adj-ROI channels
   - Express shifts as percentage changes (never raw dollar amounts alone)
   - Cap recommendations at 10% per channel per period (operational risk management)
   - Label every recommendation with the ROI gap that justifies it

5. CROSS-REFERENCE WITH BSTS (Task 24):
   - If causal impact data is available, check whether the BSTS absolute_effect
     for a channel corroborates the MMM ROI signal
   - Agreement between BSTS and MMM → high confidence in the signal
   - Disagreement → surface the discrepancy; do not act on the MMM signal alone

Output format: present findings as a structured analysis in three sections:
  (a) Model quality assessment
  (b) Channel-by-channel efficiency verdict
  (c) Budget reallocation recommendations with rationale
""".strip()


def _resolve_prompt() -> tuple[str, str]:
    """Resolve Meridian evaluation prompt via SkillResolver."""
    return _skill_resolver.resolve_skill_prompt(
        public_fallback_string=_PUBLIC_FALLBACK_PROMPT,
        private_filename="meridian_priors",
    )


# ── BQ data loaders ───────────────────────────────────────────────────────────

def _load_latest_mmm_run(lookback_days: int) -> tuple[dict | None, list[dict]]:
    """
    Load the most recently completed MMM run within the lookback window.

    Returns:
        (run_row, contribution_rows) — both empty if no qualifying run found.
    """
    run_sql = f"""
    SELECT
        run_id, status, date_from, date_to, n_geos, n_weeks, n_channels,
        n_draws, n_chains, elapsed_seconds,
        r_hat_max, r_hat_mean, ess_bulk_min, n_divergences,
        spend_total_usd, kpi_total, channel_index, geo_index,
        roi_priors_used, run_started_at
    FROM {bq.table_ref("mmm_runs")}
    WHERE status IN ('completed', 'converged')
      AND DATE(run_started_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    ORDER BY run_started_at DESC
    LIMIT 1
    """
    run_rows = bq.run_query(run_sql)
    if not run_rows:
        return None, []

    run = run_rows[0]
    run_id = run["run_id"]

    contrib_sql = f"""
    SELECT
        channel, total_spend_usd, total_impressions,
        roi_mean, roi_p5, roi_p50, roi_p95, contribution_pct,
        roi_prior_injected, roi_prior_source, roi_prior_mu, roi_prior_sigma
    FROM {bq.table_ref("mmm_channel_contributions")}
    WHERE run_id = '{run_id}'
    ORDER BY roi_mean DESC
    """
    contribs = bq.run_query(contrib_sql)
    return run, contribs


def _load_correction_weights() -> dict[str, float]:
    """
    Load Task 37 attribution correction multipliers per channel.
    Returns {channel: avg_correction_multiplier} — 1.0 where no anomalies detected.
    """
    try:
        sql = f"""
        SELECT channel, AVG(correction_multiplier) AS avg_correction
        FROM {bq.table_ref("v_attribution_correction_weights")}
        GROUP BY channel
        """
        rows = bq.run_query(sql)
        return {r["channel"]: float(r["avg_correction"] or 1.0) for r in rows}
    except Exception as exc:
        log.warning("mmm_optimizer.correction_weights_unavailable", error=str(exc))
        return {}


def _load_causal_results() -> list[dict]:
    """
    Load recent Task 24 BSTS causal impact results per channel.
    Used for counterfactual cross-reference.
    """
    try:
        sql = f"""
        SELECT
            r.target_channel,
            r.target_metric,
            m.absolute_effect,
            m.relative_effect_pct,
            m.absolute_effect_lower_90,
            m.absolute_effect_upper_90
        FROM {bq.table_ref("causal_impact_runs")} r
        JOIN {bq.table_ref("causal_impact_metrics")} m
          ON r.run_id = m.run_id AND m.metric_type = 'cumulative'
        WHERE r.status = 'completed'
          AND DATE(r.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
        ORDER BY r.created_at DESC
        """
        return bq.run_query(sql)
    except Exception as exc:
        log.warning("mmm_optimizer.causal_results_unavailable", error=str(exc))
        return []


def _load_diagnostic_tensor_meta() -> dict:
    """
    Diagnostic mode: check data loader readiness for a fresh MMM run.
    Queries platform_daily_spend to assess available data volume.
    """
    try:
        sql = f"""
        SELECT
            platform                         AS channel,
            COUNT(DISTINCT date)             AS days_available,
            MIN(date)                        AS earliest_date,
            MAX(date)                        AS latest_date,
            SUM(CAST(spend AS FLOAT64))      AS total_spend,
            COUNT(DISTINCT geo_country_code) AS geo_count
        FROM {bq.table_ref("platform_daily_spend")}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
        GROUP BY platform
        ORDER BY total_spend DESC
        """
        rows = bq.run_query(sql)
        return {"channels": rows, "status": "ok"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ── Optimization engine ───────────────────────────────────────────────────────

def _compute_optimal_allocation(
    contributions: list[dict],
    correction_weights: dict[str, float],
) -> list[dict]:
    """
    Dorfman gradient budget optimization.

    For each channel, compute the adjusted ROI (Task 37 correction applied),
    then derive a budget shift direction and magnitude based on the gap
    from the portfolio-weighted average ROI.

    Shift magnitude is capped at MAX_BUDGET_SHIFT_PCT in either direction.
    All directives carry requires_operator_approval = True.

    Returns list of allocation dicts, sorted by recommended_shift_pct descending.
    """
    if not contributions:
        return []

    # ── Compute adjusted ROI per channel ──────────────────────────────────────
    adjusted = []
    for c in contributions:
        channel = c.get("channel", "unknown")
        roi_mean = float(c.get("roi_mean") or 0.0)
        spend = float(c.get("total_spend_usd") or 0.0)
        correction = correction_weights.get(channel, 1.0)
        adj_roi = roi_mean * correction
        adjusted.append({
            "channel":          channel,
            "roi_mean":         roi_mean,
            "roi_p5":           float(c.get("roi_p5") or 0.0),
            "roi_p50":          float(c.get("roi_p50") or 0.0),
            "roi_p95":          float(c.get("roi_p95") or 0.0),
            "contribution_pct": float(c.get("contribution_pct") or 0.0),
            "total_spend_usd":  spend,
            "correction_multiplier": correction,
            "adj_roi":          adj_roi,
            "roi_prior_injected": bool(c.get("roi_prior_injected") or False),
            "roi_prior_source": c.get("roi_prior_source"),
        })

    # ── Portfolio-weighted average adjusted ROI ───────────────────────────────
    total_spend = sum(a["total_spend_usd"] for a in adjusted)
    if total_spend <= 0:
        return adjusted  # can't compute without spend data

    avg_roi = sum(a["adj_roi"] * a["total_spend_usd"] for a in adjusted) / total_spend

    # ── Per-channel gap and shift ─────────────────────────────────────────────
    results = []
    for a in adjusted:
        channel_spend = a["total_spend_usd"]
        adj_roi = a["adj_roi"]

        # gap_pct: how far above/below portfolio average (as %)
        gap_pct = ((adj_roi - avg_roi) / avg_roi * 100.0) if avg_roi > 0 else 0.0

        # Translate gap to a budget shift: 50% sensitivity (±100% gap → ±50% shift)
        # then clamp to MAX_BUDGET_SHIFT_PCT
        raw_shift_pct = gap_pct * 0.5
        shift_pct = max(min(raw_shift_pct, MAX_BUDGET_SHIFT_PCT), -MAX_BUDGET_SHIFT_PCT)
        shift_usd = round(channel_spend * shift_pct / 100.0, 2)
        new_budget = round(channel_spend + shift_usd, 2)

        # Saturation status
        roi_ci_width = a["roi_p95"] - a["roi_p5"]
        if a["roi_mean"] < 1.0:
            status = "🔴 Exhausted"
            confidence = "high"
        elif roi_ci_width > (a["roi_mean"] * 5):
            status = "⚠️ Uncertain"
            confidence = "low"
        elif adj_roi > avg_roi * 1.25:
            status = "⬆️ Under-invested"
            confidence = "high" if a["roi_prior_injected"] else "medium"
        elif adj_roi < avg_roi * 0.75:
            status = "⬇️ Over-invested"
            confidence = "high" if a["roi_prior_injected"] else "medium"
        else:
            status = "⚖️ Efficient"
            confidence = "medium"

        # Build rationale
        correction_note = (
            f" (Task 37 correction: ×{a['correction_multiplier']:.2f})"
            if a["correction_multiplier"] < 0.99
            else ""
        )
        prior_note = (
            f" [experimentally calibrated via {a['roi_prior_source']}]"
            if a["roi_prior_injected"] and a.get("roi_prior_source")
            else ""
        )
        if shift_pct > 0:
            rationale = (
                f"adj-ROI {adj_roi:.2f}x vs portfolio avg {avg_roi:.2f}x "
                f"({gap_pct:+.1f}%){correction_note}{prior_note} — increase budget"
            )
        elif shift_pct < 0:
            rationale = (
                f"adj-ROI {adj_roi:.2f}x vs portfolio avg {avg_roi:.2f}x "
                f"({gap_pct:+.1f}%){correction_note}{prior_note} — reduce budget"
            )
        else:
            rationale = f"ROI aligned with portfolio average{correction_note} — maintain"

        results.append({
            **a,
            "avg_portfolio_roi":          round(avg_roi, 4),
            "gap_from_avg_pct":           round(gap_pct, 1),
            "recommended_shift_pct":      round(shift_pct, 2),
            "recommended_shift_usd":      shift_usd,
            "new_target_budget_usd":      new_budget,
            "saturation_status":          status,
            "confidence":                 confidence,
            "rationale":                  rationale,
            "requires_operator_approval": True,
        })

    return sorted(results, key=lambda x: x["recommended_shift_pct"], reverse=True)


# ── Markdown builder ──────────────────────────────────────────────────────────

def _convergence_badge(r_hat_max: float | None, n_divergences: int | None) -> str:
    """Return a visual status badge for model convergence."""
    if r_hat_max is None:
        return "⚠️ Unknown"
    if r_hat_max > 1.1:
        return "🔴 Not converged"
    if (n_divergences or 0) > 10:
        return "⚠️ Divergences present"
    return "✅ Converged"


def _ess_badge(ess_bulk_min: float | None, n_chains: int | None) -> str:
    chains = n_chains or 4
    threshold = 100 * chains
    if ess_bulk_min is None:
        return "⚠️ Unknown"
    if ess_bulk_min < threshold:
        return f"⚠️ Low ({ess_bulk_min:.0f})"
    return f"✅ {ess_bulk_min:.0f}"


def _build_markdown_brief(
    run: dict,
    allocation: list[dict],
    causal_results: list[dict],
    operator_package: dict,
    prompt_source: str,
    geo_focus: str,
    run_diagnostic_mode: bool,
    tensor_meta: dict | None,
) -> str:
    """Assemble the three-section Markdown allocation package."""

    r_hat = float(run.get("r_hat_max") or 99.0)
    ess   = run.get("ess_bulk_min")
    divs  = int(run.get("n_divergences") or 0)
    n_chains = int(run.get("n_chains") or 4)
    n_draws  = int(run.get("n_draws") or 0)
    elapsed  = run.get("elapsed_seconds")
    date_from = run.get("date_from", "—")
    date_to   = run.get("date_to", "—")
    run_id    = run.get("run_id", "—")

    elapsed_str = f"{elapsed:.0f}s ({elapsed/60:.1f} min)" if elapsed else "—"
    badge = _convergence_badge(r_hat if r_hat < 99 else None, divs)
    ess_badge = _ess_badge(ess, n_chains)

    # Build causal cross-reference lookup
    causal_by_channel: dict[str, dict] = {}
    for cr in causal_results:
        ch = cr.get("target_channel")
        if ch:
            causal_by_channel.setdefault(ch, cr)

    lines: list[str] = [
        "## Meridian MMM — Optimization Brief",
        "",
        f"**Run ID:** `{run_id}` · **Modeling window:** {date_from} → {date_to} "
        f"({run.get('n_weeks', '?')} weeks, {run.get('n_geos', '?')} geos) "
        f"· **Geo focus:** {geo_focus}",
        "",
        "---",
        "",
        "### 📊 Model Convergence Metrics",
        "",
        "| Metric | Value | Status |",
        "|--------|-------|--------|",
        f"| R-hat max | `{r_hat:.3f}` | {badge} |",
        f"| ESS bulk min | `{ess_badge}` | {'✅' if ess and ess > 100 * n_chains else '⚠️'} |",
        f"| Divergences | `{divs}` | {'✅ Clean' if divs == 0 else f'⚠️ {divs} divergences'} |",
        f"| Channels modeled | `{run.get('n_channels', '?')}` | — |",
        f"| MCMC draws | `{n_draws} × {n_chains} chains` | — |",
        f"| Sampling time | `{elapsed_str}` | — |",
        f"| Task 22 priors | `{'injected' if run.get('roi_priors_used') and run.get('roi_priors_used') != '{}' else 'default weakly informative'}` | — |",
        "",
    ]

    if r_hat > 1.1:
        lines += [
            "> ⚠️ **Convergence warning:** R-hat > 1.1 — chains have not fully mixed. "
            "Budget recommendations below should be treated as directional signals only. "
            "Re-run with `n_draws=1000` and `n_adapt=500` before acting on allocations.",
            "",
        ]

    # ── Channel saturation matrix ─────────────────────────────────────────────
    lines += [
        "---",
        "",
        "### 📈 Channel Marginal Return Saturation",
        "",
        "| Channel | Spend (model window) | adj-ROI | 90% CI | Contrib % | "
        "Task 37 corr. | BSTS signal | Status |",
        "|---------|---------------------|---------|--------|-----------|"
        "-------------|-------------|--------|",
    ]

    for a in allocation:
        channel = a["channel"]
        spend_k = f"${a['total_spend_usd']/1000:.1f}K" if a['total_spend_usd'] >= 1000 else f"${a['total_spend_usd']:.0f}"
        adj_roi = a["adj_roi"]
        roi_p5  = a["roi_p5"]
        roi_p95 = a["roi_p95"]
        corr    = a["correction_multiplier"]
        corr_str = f"×{corr:.2f}" if corr < 0.99 else "✅ 1.00"

        causal = causal_by_channel.get(channel)
        if causal:
            rel = float(causal.get("relative_effect_pct") or 0.0)
            bsts_str = f"{rel:+.0f}% lift"
        else:
            bsts_str = "no data"

        lines.append(
            f"| **{channel}** | {spend_k} | **{adj_roi:.2f}x** | "
            f"[{roi_p5:.2f}x, {roi_p95:.2f}x] | {a['contribution_pct']:.1f}% | "
            f"{corr_str} | {bsts_str} | {a['saturation_status']} |"
        )

    lines += [""]

    # BSTS counterfactual note
    if causal_results:
        lines += [
            "> **Counterfactual cross-reference (Task 24 BSTS):** BSTS absolute effects "
            "are shown per channel where available. Agreement between MMM ROI direction "
            "and positive BSTS lift → high-confidence signal. Divergence → do not act "
            "on MMM signal alone; commission a geo holdout experiment.",
            "",
        ]

    # ── Operator execution package ────────────────────────────────────────────
    lines += [
        "---",
        "",
        "### 🤖 Operator Execution Instructions",
        "",
        f"> **Security constraint:** All budget shifts capped at ±{MAX_BUDGET_SHIFT_PCT:.0f}% "
        f"per channel per run. `requires_operator_approval = true` on all directives. "
        f"No funds move without explicit Operator agent confirmation.",
        "",
    ]

    active_recs = [r for r in allocation if abs(r["recommended_shift_pct"]) >= 0.5]

    if not active_recs:
        lines += [
            "> ⚖️ All channels are within ±{:.0f}% of portfolio-average ROI. "
            "No reallocation recommended at this time. Re-run after the next "
            "monthly data refresh for updated signals.".format(MAX_BUDGET_SHIFT_PCT / 2),
            "",
        ]
    else:
        for rec in active_recs:
            direction = "increase" if rec["recommended_shift_pct"] > 0 else "decrease"
            sign = "+" if rec["recommended_shift_pct"] > 0 else ""
            lines += [
                f"**{rec['channel']}** — {rec['saturation_status']}",
                f"> Shift: **{sign}{rec['recommended_shift_pct']:.1f}%** "
                f"(${abs(rec['recommended_shift_usd']):,.0f} {direction}) "
                f"→ new monthly target ~${rec['new_target_budget_usd']:,.0f}",
                f"> *{rec['rationale']}*",
                f"> Confidence: {rec['confidence']}  |  "
                f"Requires approval: ✅  |  Prior calibrated: "
                f"{'✅ ' + str(rec.get('roi_prior_source') or '') if rec['roi_prior_injected'] else '—'}",
                "",
            ]

    # Machine-readable payload
    lines += [
        "**Operator Execution JSON Payload:**",
        "",
        "```json",
        json.dumps(operator_package, indent=2, default=str),
        "```",
        "",
        "---",
        "",
        "*Evaluation framework: "
        + ("`Extended Secure Framework` (private Meridian priors active)"
           if prompt_source == "private"
           else "`Standard Open Core Engine` (public fallback)")
        + " | Tables: `mmm_runs`, `mmm_channel_contributions`, "
        "`v_attribution_correction_weights`, `causal_impact_metrics`*",
    ]

    if run_diagnostic_mode and tensor_meta:
        status = tensor_meta.get("status", "unknown")
        if status == "ok":
            channels_data = tensor_meta.get("channels", [])
            lines += [
                "",
                "---",
                "",
                "### 🔬 Diagnostic Mode — Data Loader Readiness",
                "",
                "| Channel | Days available | Date range | Total spend | Geos |",
                "|---------|---------------|------------|-------------|------|",
            ]
            for ch in channels_data:
                spend_k = f"${float(ch.get('total_spend') or 0)/1000:.1f}K"
                lines.append(
                    f"| {ch.get('channel','?')} | {ch.get('days_available','?')} | "
                    f"{ch.get('earliest_date','?')} → {ch.get('latest_date','?')} | "
                    f"{spend_k} | {ch.get('geo_count','?')} |"
                )
            lines.append("")
            lines.append(
                "> **Readiness verdict:** "
                + ("✅ Ready for fresh MMM run — call `run_mmm_model` with `date_from`/`date_to`."
                   if any(int(c.get("days_available") or 0) >= 547 for c in channels_data)
                   else "⚠️ Less than 78 weeks of data available on at least one channel — MMM may underfit. Extend the date range or use more channels.")
            )
        else:
            lines += [
                "",
                f"> Diagnostic data loader query failed: `{tensor_meta.get('error', 'unknown error')}`",
            ]

    return "\n".join(lines)


def _build_operator_package(
    allocation: list[dict],
    run: dict,
) -> dict:
    """
    Build the machine-readable Operator execution JSON payload.

    Each recommendation is formatted to feed directly into the Operator agent's
    budget mutation tools. All directives carry requires_operator_approval=True.
    """
    recommendations = []
    for a in allocation:
        shift_pct = a["recommended_shift_pct"]
        if abs(shift_pct) < 0.5:
            continue  # skip negligible shifts

        recommendations.append({
            "action":                  "adjust_channel_budget",
            "channel":                 a["channel"],
            "direction":               "increase" if shift_pct > 0 else "decrease",
            "current_modeled_spend_usd": round(a["total_spend_usd"], 2),
            "recommended_shift_pct":   shift_pct,
            "recommended_shift_usd":   abs(a["recommended_shift_usd"]),
            "new_target_budget_usd":   a["new_target_budget_usd"],
            "saturation_status":       a["saturation_status"],
            "adj_roi_mean":            round(a["adj_roi"], 4),
            "portfolio_avg_roi":       round(a["avg_portfolio_roi"], 4),
            "task37_correction_applied": a["correction_multiplier"] < 0.99,
            "correction_multiplier":   a["correction_multiplier"],
            "roi_prior_injected":      a["roi_prior_injected"],
            "roi_prior_source":        a.get("roi_prior_source"),
            "confidence":              a["confidence"],
            "rationale":               a["rationale"],
            "requires_operator_approval": True,
            "max_shift_pct_policy":    MAX_BUDGET_SHIFT_PCT,
        })

    return {
        "schema_version":         "task27.v1",
        "generated_at":           datetime.now(timezone.utc).isoformat(),
        "mmm_run_id":             run.get("run_id", ""),
        "model_window_from":      str(run.get("date_from", "")),
        "model_window_to":        str(run.get("date_to", "")),
        "r_hat_max":              run.get("r_hat_max"),
        "operator_approval_required": True,
        "max_shift_pct_policy":   MAX_BUDGET_SHIFT_PCT,
        "recommendations":        recommendations,
        "note": (
            "All budget shifts require explicit Operator agent approval before execution. "
            "Shifts are expressed as changes to the MODELED spend window — scale to "
            "actual monthly budgets proportionally. Do not execute >1 reallocation cycle "
            "per calendar month without a fresh MMM run to avoid compound drift."
        ),
    }


# ── Main optimizer class ───────────────────────────────────────────────────────

class MMMOptimizerAnalyst:
    """
    Meridian MMM output reader and budget optimization engine.

    Reads pre-computed posterior summaries from BigQuery and produces
    an actionable Markdown allocation brief with Operator execution directives.

    Does NOT run JAX/NumPyro MCMC inline. Use run_mmm_model (or run_mmm_pipeline)
    to generate model outputs first, then call this optimizer to analyze them.

    Usage:
        optimizer = MMMOptimizerAnalyst()
        result = optimizer.run_optimization(
            historical_lookback_days=365,
            geo_focus="US",
            run_diagnostic_mode=False,
        )
    """

    def run_optimization(
        self,
        historical_lookback_days: int = 365,
        geo_focus: str = "US",
        run_diagnostic_mode: bool = False,
    ) -> dict:
        """
        Execute the MMM optimization pipeline.

        Reads pre-computed Meridian results from BigQuery (no JAX execution).
        If no qualifying MMM run is found in the lookback window, returns a
        clear prompt to run run_mmm_model first.

        Args:
            historical_lookback_days: Days to look back for a qualifying MMM run.
            geo_focus:               Geo filter label (informational — included
                                     in brief header; not used to filter BQ rows
                                     since geo breakdown is in the model tensor).
            run_diagnostic_mode:     If True, also queries platform_daily_spend
                                     to assess data loader readiness for a new run.

        Returns:
            dict with keys:
                status                    — "ok" | "no_model_found" | "bq_error"
                markdown_brief            — formatted Markdown allocation package
                operator_execution_package — machine-readable budget shift JSON
                mmm_run_id                — run_id of the analyzed model run
                r_hat_max                 — convergence quality (< 1.1 = converged)
                allocation                — per-channel allocation dicts
                evaluation_context        — resolved SkillResolver prompt
                prompt_source             — "private" | "public_fallback"
        """
        # ── Resolve prompt ─────────────────────────────────────────────────────
        evaluation_context, prompt_source = _resolve_prompt()

        # ── Load MMM results ───────────────────────────────────────────────────
        try:
            run, contributions = _load_latest_mmm_run(historical_lookback_days)
        except Exception as exc:
            log.error("mmm_optimizer.bq_load_failed", error=str(exc))
            return {
                "status":        "bq_error",
                "error":         str(exc),
                "markdown_brief": (
                    "**BigQuery error** — could not load MMM results.\n\n"
                    f"```\n{exc}\n```"
                ),
                "operator_execution_package": {},
                "evaluation_context": evaluation_context,
                "prompt_source":      prompt_source,
            }

        if run is None:
            return {
                "status": "no_model_found",
                "markdown_brief": (
                    "## Meridian MMM — Optimization Brief\n\n"
                    f"_No completed MMM run found within the last {historical_lookback_days} days._\n\n"
                    "**Next step:** run `run_mmm_model` to execute the Meridian sampling pipeline "
                    "(35–45 min on Cloud Run). After the run completes and writes results to "
                    "`mmm_runs` and `mmm_channel_contributions`, call this tool again to "
                    "generate the optimization brief.\n\n"
                    "Recommended parameters:\n"
                    "```json\n"
                    '{"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD", '
                    '"n_draws": 500, "n_chains": 4}\n'
                    "```"
                ),
                "operator_execution_package": {},
                "mmm_run_id":         None,
                "evaluation_context": evaluation_context,
                "prompt_source":      prompt_source,
            }

        # ── Load cross-reference data ──────────────────────────────────────────
        correction_weights = _load_correction_weights()
        causal_results     = _load_causal_results()

        # ── Compute optimal allocation ─────────────────────────────────────────
        allocation = _compute_optimal_allocation(contributions, correction_weights)

        # ── Diagnostic tensor readiness (optional) ────────────────────────────
        tensor_meta: dict | None = None
        if run_diagnostic_mode:
            tensor_meta = _load_diagnostic_tensor_meta()

        # ── Build Operator package ─────────────────────────────────────────────
        operator_package = _build_operator_package(allocation, run)

        # ── Format Markdown brief ──────────────────────────────────────────────
        markdown = _build_markdown_brief(
            run=run,
            allocation=allocation,
            causal_results=causal_results,
            operator_package=operator_package,
            prompt_source=prompt_source,
            geo_focus=geo_focus,
            run_diagnostic_mode=run_diagnostic_mode,
            tensor_meta=tensor_meta,
        )

        log.info(
            "mmm_optimizer.optimization_complete",
            run_id=run.get("run_id"),
            channels=len(allocation),
            active_recommendations=len(operator_package["recommendations"]),
            prompt_source=prompt_source,
            r_hat_max=run.get("r_hat_max"),
        )

        return {
            "status":                     "ok",
            "markdown_brief":             markdown,
            "operator_execution_package": operator_package,
            "mmm_run_id":                 run.get("run_id"),
            "r_hat_max":                  run.get("r_hat_max"),
            "n_active_recommendations":   len(operator_package["recommendations"]),
            "allocation":                 allocation,
            "evaluation_context":         evaluation_context,
            "prompt_source":              prompt_source,
            "correction_weights_applied": bool(correction_weights),
            "causal_results_available":   bool(causal_results),
        }
