# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Adstock Analyst — Ad Decay, Carryover & Pacing Engine.

Computes geometric adstock residuals per channel from historical spend data,
correlates carry-over effects with organic/direct session decay in the clickstream,
and generates programmatic flighting schedules that exploit the halo effect while
enforcing cool-down gates and baseline memory floors.

Architecture
────────────
    AdstockAnalyst
        ├── resolve_prompt()              — SkillResolver: private pacing rules or fallback
        ├── fetch_daily_spend()           — platform_daily_spend for lookback window
        ├── fetch_session_decay()         — sessions table: organic/direct daily volume
        ├── fetch_mmm_context()           — mmm_channel_contributions for ROI weighting
        ├── load_channel_params()         — parse JSON config from private skill
        ├── compute_adstock_series()      — geometric decay series per channel
        ├── detect_surge_windows()        — find weeks where spend > threshold × avg
        ├── check_organic_anomaly()       — flag organic spikes uncorrelated with paid
        ├── build_halo_matrix()           — per-channel residual + half-life table
        ├── build_flighting_schedule()    — 14-day forward pulse/hold/cool-down plan
        └── render_output()              — markdown brief + JSON schedule vector

Open Core Isolation
────────────────────
PUBLIC_FALLBACK_PROMPT contains generic adstock theory and standard B2B half-life
reference ranges. Private operational parameters (lambda, cool-down gates, floor
spend, pulse cadence) live exclusively in
agents/analyst/skills/private_decay_pacing.md — never committed to the open repo.

Geometric Adstock Reference
────────────────────────────
  Series:      A[t] = spend[t] + λ * A[t-1]
               Expanded: A[t] = Σ_{k=0}^{max_lag} λ^k * spend[t-k]
  Half-life:   t_half = log(0.5) / log(λ_daily)
               where λ_daily = λ_weekly^(1/7)
  Residual %:  100 * A[today] / max(A over lookback window)

Usage
─────
  from tools.adstock_analyst import AdstockAnalyst
  analyst = AdstockAnalyst()
  result = analyst.run(lookback_days=60, execution_mode="full")
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from tools import bigquery_client as bq
from tools.skill_resolver import SkillResolver

log = structlog.get_logger()

# ── Public fallback prompt ────────────────────────────────────────────────────

PUBLIC_FALLBACK_PROMPT = """
You are a paid media adstock and flighting analyst. Evaluate channel spend carry-over
using the following standard geometric adstock framework.

## Adstock Decay Framework (Standard)

### Geometric Adstock Model
Adstock carries forward the effect of past spend via exponential decay:
  A[t] = spend[t] + λ * A[t-1]

Where λ (lambda) is the retention parameter (0 < λ < 1):
- λ close to 1 = slow decay, long memory (awareness channels)
- λ close to 0 = fast decay, short memory (direct response channels)

### Standard B2B Half-Life Reference Ranges
Half-life in days = log(0.5) / log(λ_daily), where λ_daily = λ_weekly^(1/7):

| Channel Type | Typical λ_weekly | Half-Life (days) |
|---|---|---|
| Branded Search | 0.70–0.80 | 10–18 days |
| Non-Brand Search | 0.50–0.65 | 6–10 days |
| B2B Social (LinkedIn) | 0.75–0.88 | 17–35 days |
| Paid Social (Meta/TikTok) | 0.45–0.65 | 5–10 days |
| Community (Reddit) | 0.60–0.75 | 9–18 days |

### Adstock Residual Status Thresholds
Normalize current adstock against peak adstock in the lookback window:
- 🟢 Active Halo (≥ 55%): Decay tail still productive; consider reducing live spend
- 🟡 Fading (20–55%): Carry-over exists but weakening; hold current spend
- 🔴 Depleted (< 20%): Memory near zero; next spend effectively cold

### Cool-Down Gate (General)
If a channel's adstock residual is ≥ 55% after a spend surge, a short spend
reduction (5–14 days) can recapture budget without proportional pipeline loss.
Awareness channels (LinkedIn, Reddit) tolerate longer gates than direct response.

### Baseline Floor Rule
Never reduce spend below the minimum needed to sustain brand memory:
- Search: ~$3,000–4,000/week for a typical B2B mid-market org
- Social: ~$1,500–3,000/week
- Community: ~$500–1,000/week

Below the floor, organic branded search volume and direct navigation traffic decline
within 2–3 weeks, indicating brand recall erosion.

### Organic Correlation Check
If organic/direct session volume rises without a corresponding paid spend surge,
investigate non-adstock causes: PR event, viral content, competitor outage.
Adstock decay should only be inferred from paid spend, not organic events.

### Output Format
1. Adstock Halo Matrix: Channel | λ | Half-Life | Current Residual % | Status
2. Flighting Schedule: 14-day table — Week | Channel | Action | Budget % | Rationale
""".strip()


# ── Default channel parameters (public fallback values) ──────────────────────

_DEFAULT_CHANNEL_PARAMS: dict[str, dict[str, Any]] = {
    "google_ads":  {"lambda_weekly": 0.62, "lambda_daily": 0.921, "max_lag_days": 21, "cool_down_threshold_pct": 55, "min_floor_spend_weekly_usd": 3500, "surge_multiplier_threshold": 1.35, "pulse_on_weeks": 3, "pulse_off_weeks": 1, "channel_type": "direct_response"},
    "linkedin":    {"lambda_weekly": 0.82, "lambda_daily": 0.972, "max_lag_days": 42, "cool_down_threshold_pct": 65, "min_floor_spend_weekly_usd": 2000, "surge_multiplier_threshold": 1.30, "pulse_on_weeks": 4, "pulse_off_weeks": 2, "channel_type": "awareness_b2b"},
    "meta":        {"lambda_weekly": 0.61, "lambda_daily": 0.919, "max_lag_days": 21, "cool_down_threshold_pct": 50, "min_floor_spend_weekly_usd": 2500, "surge_multiplier_threshold": 1.40, "pulse_on_weeks": 3, "pulse_off_weeks": 1, "channel_type": "direct_response"},
    "tiktok":      {"lambda_weekly": 0.55, "lambda_daily": 0.909, "max_lag_days": 14, "cool_down_threshold_pct": 45, "min_floor_spend_weekly_usd": 1500, "surge_multiplier_threshold": 1.50, "pulse_on_weeks": 2, "pulse_off_weeks": 2, "channel_type": "awareness_entertainment"},
    "reddit_ads":  {"lambda_weekly": 0.70, "lambda_daily": 0.943, "max_lag_days": 28, "cool_down_threshold_pct": 60, "min_floor_spend_weekly_usd": 800,  "surge_multiplier_threshold": 1.45, "pulse_on_weeks": 3, "pulse_off_weeks": 1, "channel_type": "community_intent"},
}


class AdstockAnalyst:
    """
    Computes geometric adstock residuals, correlates with organic session decay,
    detects surge windows, and generates programmatic flighting schedules.
    """

    def __init__(self) -> None:
        self._resolver = SkillResolver()
        self._channel_params: dict[str, dict[str, Any]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        lookback_days: int = 60,
        execution_mode: str = "full",
    ) -> dict[str, Any]:
        """
        Execute the adstock decay and pacing analysis pipeline.

        Args:
            lookback_days:   Days of spend history to use for adstock series.
                             Minimum 21 days (longest max_lag_days). Default 60.
            execution_mode:  "analyze"    — halo matrix only (no flighting schedule)
                             "flight_plan" — flighting schedule only
                             "full"       — both outputs (default)

        Returns:
            dict with keys:
                prompt_source      — "private" | "public_fallback"
                channels_analysed  — list of channel names
                halo_matrix        — list of per-channel adstock metrics
                flighting_schedule — list of 14-day recommendations (if requested)
                organic_anomaly    — dict flagging organic surges uncorrelated with paid
                markdown_brief     — full rendered markdown output
                operator_vector    — JSON flighting instructions for Operator agent
                analysed_at        — ISO timestamp
        """
        # Step 1: Resolve prompt + load channel params
        prompt, source = self._resolver.resolve_skill_prompt(
            public_fallback_string=PUBLIC_FALLBACK_PROMPT,
            private_filename="decay_pacing",
        )
        self._channel_params = self._load_channel_params(prompt)

        lookback_days = max(lookback_days, 21)  # need at least max_lag_days history
        today = date.today()
        date_from = today - timedelta(days=lookback_days)

        # Step 2: Fetch data
        daily_spend   = self._fetch_daily_spend(date_from, today)
        session_decay = self._fetch_session_decay(date_from, today)
        mmm_ctx       = self._fetch_mmm_context()

        if not daily_spend:
            return {
                "error": "No spend data found in platform_daily_spend for the lookback window.",
                "prompt_source": source,
            }

        channels = sorted(set(r["platform"] for r in daily_spend))

        # Step 3: Compute adstock series + surge windows per channel
        halo_matrix: list[dict] = []
        all_surge_windows: dict[str, list[dict]] = {}

        for ch in channels:
            if ch not in self._channel_params and ch not in _DEFAULT_CHANNEL_PARAMS:
                continue  # skip unknown channels (e.g. reddit_daily_spend source)
            ch_spend = sorted(
                [r for r in daily_spend if r["platform"] == ch],
                key=lambda r: r["date"],
            )
            params      = self._channel_params.get(ch, _DEFAULT_CHANNEL_PARAMS.get(ch, {}))
            series, surges = self._compute_adstock_series(ch_spend, params, today)
            halo_entry  = self._build_halo_entry(ch, series, surges, params, mmm_ctx, today)
            halo_matrix.append(halo_entry)
            all_surge_windows[ch] = surges

        # Step 4: Organic/direct session anomaly check
        organic_anomaly = self._check_organic_anomaly(session_decay, daily_spend)

        # Step 5: Build outputs based on execution_mode
        flighting_schedule: list[dict] = []
        if execution_mode in ("flight_plan", "full"):
            flighting_schedule = self._build_flighting_schedule(halo_matrix, today)

        operator_vector = self._build_operator_vector(halo_matrix, flighting_schedule, today)

        # Step 6: Render markdown
        markdown_brief = self._render_markdown(
            halo_matrix,
            flighting_schedule,
            organic_anomaly,
            execution_mode,
            today,
            lookback_days,
        )

        log.info(
            "adstock_analyst.complete",
            channels=len(halo_matrix),
            prompt_source=source,
            execution_mode=execution_mode,
            organic_anomaly_detected=organic_anomaly.get("anomaly_detected", False),
        )

        return {
            "prompt_source":      source,
            "channels_analysed":  [h["channel"] for h in halo_matrix],
            "halo_matrix":        halo_matrix,
            "flighting_schedule": flighting_schedule,
            "organic_anomaly":    organic_anomaly,
            "markdown_brief":     markdown_brief,
            "operator_vector":    operator_vector,
            "analysed_at":        datetime.now(timezone.utc).isoformat(),
        }

    # ── Data fetching ─────────────────────────────────────────────────────────

    def _fetch_daily_spend(self, date_from: date, date_to: date) -> list[dict]:
        """Daily spend by platform for the lookback window."""
        try:
            rows = bq.run_query(f"""
                SELECT
                    date,
                    platform,
                    SUM(CAST(spend AS FLOAT64)) AS daily_spend,
                    SUM(impressions)             AS daily_impressions
                FROM {bq.table_ref('platform_daily_spend')}
                WHERE date BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
                GROUP BY date, platform
                ORDER BY date, platform
            """)
            return [
                {
                    "date":     r["date"] if isinstance(r["date"], date) else date.fromisoformat(str(r["date"])),
                    "platform": str(r["platform"]),
                    "spend":    float(r.get("daily_spend") or 0),
                    "impressions": int(r.get("daily_impressions") or 0),
                }
                for r in rows
            ]
        except Exception as exc:
            log.warning("adstock_analyst.spend_fetch_error", error=str(exc)[:200])
            return []

    def _fetch_session_decay(self, date_from: date, date_to: date) -> list[dict]:
        """Daily organic/direct session volume for carry-over correlation."""
        try:
            rows = bq.run_query(f"""
                SELECT
                    DATE(session_start_at) AS session_date,
                    COALESCE(utm_medium, 'unknown') AS medium,
                    COUNT(*) AS sessions
                FROM {bq.table_ref('sessions')}
                WHERE DATE(session_start_at) BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
                GROUP BY 1, 2
                ORDER BY 1, 2
            """)
            return [
                {
                    "date":     r["session_date"] if isinstance(r["session_date"], date) else date.fromisoformat(str(r["session_date"])),
                    "medium":   str(r["medium"]),
                    "sessions": int(r.get("sessions") or 0),
                }
                for r in rows
            ]
        except Exception as exc:
            log.warning("adstock_analyst.session_fetch_error", error=str(exc)[:200])
            return []

    def _fetch_mmm_context(self) -> dict[str, dict]:
        """Pull latest MMM ROI posteriors for weighting context."""
        try:
            rows = bq.run_query(f"""
                SELECT cc.channel, cc.roi_mean, cc.contribution_pct,
                       CAST(cc.total_spend_usd AS FLOAT64) AS total_spend_usd
                FROM {bq.table_ref('mmm_channel_contributions')} cc
                INNER JOIN (
                    SELECT run_id FROM {bq.table_ref('mmm_runs')}
                    WHERE status IN ('completed', 'converged')
                    ORDER BY run_started_at DESC LIMIT 1
                ) latest USING (run_id)
            """)
            return {
                str(r["channel"]): {
                    "roi_mean":       float(r.get("roi_mean") or 0),
                    "contribution_pct": float(r.get("contribution_pct") or 0),
                    "total_spend_usd": float(r.get("total_spend_usd") or 0),
                }
                for r in rows
            }
        except Exception as exc:
            log.warning("adstock_analyst.mmm_fetch_error", error=str(exc)[:200])
            return {}

    # ── Parameter loading ─────────────────────────────────────────────────────

    def _load_channel_params(self, prompt_text: str) -> dict[str, dict[str, Any]]:
        """Extract JSON config block from private skill markdown."""
        try:
            match = re.search(r"```json\s*(\{.*?\})\s*```", prompt_text, re.DOTALL)
            if match:
                parsed = json.loads(match.group(1))
                channels = parsed.get("channels", {})
                if channels:
                    log.info("adstock_analyst.params_loaded", source="private_skill", channel_count=len(channels))
                    return channels
        except (json.JSONDecodeError, AttributeError) as exc:
            log.warning("adstock_analyst.param_parse_error", error=str(exc))
        log.info("adstock_analyst.params_loaded", source="defaults")
        return _DEFAULT_CHANNEL_PARAMS

    # ── Core adstock computation ──────────────────────────────────────────────

    def _compute_adstock_series(
        self,
        ch_spend_rows: list[dict],
        params: dict[str, Any],
        today: date,
    ) -> tuple[dict[date, float], list[dict]]:
        """
        Compute geometric adstock series and detect surge windows.

        Returns:
            series  — {date: adstock_value} for every day in the spend data
            surges  — list of surge window dicts {start, end, peak_spend, avg_spend}
        """
        lam      = float(params.get("lambda_daily", 0.92))
        max_lag  = int(params.get("max_lag_days", 21))
        surge_th = float(params.get("surge_multiplier_threshold", 1.40))

        # Build spend lookup by date
        spend_by_date: dict[date, float] = {r["date"]: r["spend"] for r in ch_spend_rows}
        if not spend_by_date:
            return {}, []

        all_dates = sorted(spend_by_date.keys())
        date_min, date_max = all_dates[0], all_dates[-1]

        # Fill gaps with zero spend
        full_dates: list[date] = []
        d = date_min
        while d <= date_max:
            full_dates.append(d)
            d += timedelta(days=1)

        # Compute adstock series: A[t] = spend[t] + λ * A[t-1]
        series: dict[date, float] = {}
        prev_adstock = 0.0
        for d in full_dates:
            s = spend_by_date.get(d, 0.0)
            adstock = s + lam * prev_adstock
            series[d] = adstock
            prev_adstock = adstock

        # Compute 4-week rolling average spend for surge detection
        weekly_spend: dict[date, float] = {}
        for row in ch_spend_rows:
            wk = row["date"] - timedelta(days=row["date"].weekday())  # Monday
            weekly_spend[wk] = weekly_spend.get(wk, 0.0) + row["spend"]

        weeks = sorted(weekly_spend.keys())
        surges: list[dict] = []
        for i, wk in enumerate(weeks):
            if i < 3:
                continue  # need 4 weeks for rolling avg
            rolling_avg = sum(weekly_spend[weeks[j]] for j in range(i - 3, i)) / 3
            if rolling_avg > 0 and weekly_spend[wk] >= rolling_avg * surge_th:
                surges.append({
                    "week_start":      wk,
                    "week_end":        wk + timedelta(days=6),
                    "surge_spend":     round(weekly_spend[wk], 2),
                    "avg_spend":       round(rolling_avg, 2),
                    "surge_ratio":     round(weekly_spend[wk] / rolling_avg, 2),
                    "days_since":      (today - (wk + timedelta(days=6))).days,
                })

        return series, surges

    def _build_halo_entry(
        self,
        channel: str,
        series: dict[date, float],
        surges: list[dict],
        params: dict[str, Any],
        mmm_ctx: dict[str, dict],
        today: date,
    ) -> dict[str, Any]:
        """Build the per-channel halo matrix row from the adstock series."""
        lam_w    = float(params.get("lambda_weekly", 0.70))
        lam_d    = float(params.get("lambda_daily", 0.943))
        max_lag  = int(params.get("max_lag_days", 28))
        cd_gate  = float(params.get("cool_down_threshold_pct", 60))
        floor    = float(params.get("min_floor_spend_weekly_usd", 1000))
        ch_type  = str(params.get("channel_type", "unknown"))

        # Half-life in days
        half_life_days = round(math.log(0.5) / math.log(lam_d), 1) if lam_d < 1 else 999

        # Current adstock value (today or most recent date)
        current_date = today
        while current_date not in series and current_date >= today - timedelta(days=7):
            current_date -= timedelta(days=1)
        current_adstock = series.get(current_date, 0.0)

        # Peak adstock in lookback window
        peak_adstock = max(series.values()) if series else 1.0

        # Residual % = current / peak
        residual_pct = round(100.0 * current_adstock / max(peak_adstock, 0.001), 1)

        # Implied "spend equivalent" still working in the market
        # Based on: if A[today] = Σ λ^k * spend[t-k], what single spend would produce this?
        # Approximation: residual_spend ≈ current_adstock * (1 - lam_d) since A[t] ≈ spend / (1-λ) at steady state
        residual_spend_equiv = round(current_adstock * (1 - lam_d) * 7, 2)  # weekly equivalent

        # Status
        if residual_pct >= cd_gate:
            status = "🟢 Active Halo"
        elif residual_pct >= 20:
            status = "🟡 Fading"
        else:
            status = "🔴 Depleted"

        # Cool-down gate eligibility
        recent_surges = [s for s in surges if s["days_since"] <= max_lag]
        cool_down_eligible = (
            len(recent_surges) > 0
            and residual_pct >= cd_gate
            and ch_type in ("awareness_b2b", "awareness_entertainment", "community_intent")
        )

        # MMM context
        mmm = mmm_ctx.get(channel, {})
        roi_mean = mmm.get("roi_mean", 0.0)

        return {
            "channel":               channel,
            "channel_type":          ch_type,
            "lambda_weekly":         lam_w,
            "lambda_daily":          round(lam_d, 3),
            "half_life_days":        half_life_days,
            "max_lag_days":          max_lag,
            "current_adstock":       round(current_adstock, 2),
            "peak_adstock":          round(peak_adstock, 2),
            "residual_pct":          residual_pct,
            "residual_spend_equiv_weekly": residual_spend_equiv,
            "floor_spend_weekly_usd": floor,
            "cool_down_eligible":    cool_down_eligible,
            "cool_down_gate_pct":    cd_gate,
            "status":                status,
            "recent_surges":         recent_surges,
            "roi_mean":              roi_mean,
        }

    # ── Organic anomaly detection ─────────────────────────────────────────────

    def _check_organic_anomaly(
        self,
        session_rows: list[dict],
        spend_rows: list[dict],
    ) -> dict[str, Any]:
        """
        Detect organic/direct session surges that are NOT correlated with paid spend surges.
        These are forensic flags — they represent halo effects from external events (PR, viral)
        rather than adstock carry-over from paid media.
        """
        if not session_rows:
            return {"anomaly_detected": False, "reason": "No session data available"}

        # Daily organic + direct sessions
        organic_by_date: dict[date, int] = {}
        for r in session_rows:
            if r["medium"] in ("organic", "direct"):
                organic_by_date[r["date"]] = organic_by_date.get(r["date"], 0) + r["sessions"]

        if not organic_by_date:
            return {"anomaly_detected": False, "reason": "No organic/direct sessions in window"}

        dates = sorted(organic_by_date.keys())
        if len(dates) < 14:
            return {"anomaly_detected": False, "reason": "Insufficient date range for anomaly detection"}

        # 14-day rolling average for baseline
        anomalies: list[dict] = []
        for i, d in enumerate(dates):
            if i < 14:
                continue
            window = [organic_by_date[dates[j]] for j in range(i - 14, i)]
            rolling_avg = sum(window) / len(window)
            current_vol = organic_by_date[d]

            if rolling_avg > 0 and current_vol >= rolling_avg * 3.0:
                # Check paid spend on same day (within ±3 days)
                paid_spike = any(
                    r["spend"] > 0
                    and abs((r["date"] - d).days) <= 3
                    and r.get("platform") in ("google_ads",)  # search drives organic correlation
                    for r in spend_rows
                    if isinstance(r.get("date"), date)
                )
                anomalies.append({
                    "date":         d.isoformat(),
                    "organic_sessions": current_vol,
                    "rolling_avg_sessions": round(rolling_avg, 1),
                    "surge_ratio":  round(current_vol / rolling_avg, 2),
                    "paid_surge_correlated": paid_spike,
                    "flag": "external_event" if not paid_spike else "adstock_driven",
                })

        if not anomalies:
            return {"anomaly_detected": False, "details": []}

        external_anomalies = [a for a in anomalies if not a["paid_surge_correlated"]]
        return {
            "anomaly_detected":      len(external_anomalies) > 0,
            "total_anomaly_days":    len(anomalies),
            "external_event_days":   len(external_anomalies),
            "adstock_driven_days":   len(anomalies) - len(external_anomalies),
            "details":               anomalies,
            "interpretation": (
                f"{len(external_anomalies)} organic surge day(s) detected with NO corresponding "
                "paid search spike. These are NOT attributable to adstock carry-over. "
                "Investigate external causes: PR event, competitor outage, viral content, or bot traffic."
            ) if external_anomalies else "All organic surges correlated with paid spend activity.",
        }

    # ── Flighting schedule ────────────────────────────────────────────────────

    def _build_flighting_schedule(
        self,
        halo_matrix: list[dict],
        today: date,
    ) -> list[dict]:
        """
        Build a 14-day forward flighting schedule.
        Week 1 = today + 7 days. Week 2 = today + 8-14 days.
        """
        week1_start = today + timedelta(days=1)
        week1_end   = today + timedelta(days=7)
        week2_start = today + timedelta(days=8)
        week2_end   = today + timedelta(days=14)

        schedule: list[dict] = []

        for h in halo_matrix:
            channel  = h["channel"]
            residual = h["residual_pct"]
            status   = h["status"]
            eligible = h["cool_down_eligible"]
            floor    = h["floor_spend_weekly_usd"]
            params   = self._channel_params.get(channel, _DEFAULT_CHANNEL_PARAMS.get(channel, {}))
            pulse_on = int(params.get("pulse_on_weeks", 3))
            pulse_off= int(params.get("pulse_off_weeks", 1))

            # Week 1 recommendation
            if eligible and residual >= h["cool_down_gate_pct"]:
                w1_action       = "COOL_DOWN"
                w1_budget_pct   = 30
                w1_rationale    = (
                    f"Active halo ({residual:.0f}% residual) above cool-down gate "
                    f"({h['cool_down_gate_pct']:.0f}%). "
                    f"Reduce to 30% budget for 7 days to exploit decay tail. "
                    f"Floor check: 30% must be ≥ ${floor:,.0f}/week."
                )
            elif "🟢" in status:
                w1_action       = "HOLD"
                w1_budget_pct   = 100
                w1_rationale    = (
                    f"Strong halo ({residual:.0f}%). No surge eligibility. "
                    "Maintain current pacing — decay tail is working as expected."
                )
            elif "🟡" in status:
                w1_action       = "PULSE_ON"
                w1_budget_pct   = 100
                w1_rationale    = (
                    f"Fading halo ({residual:.0f}%). Memory is declining — "
                    "maintain full spend to prevent memory erosion below floor."
                )
            else:  # Depleted
                w1_action       = "PULSE_ON"
                w1_budget_pct   = 120
                w1_rationale    = (
                    f"Depleted halo ({residual:.0f}%). Memory near zero — "
                    "re-engage at 120% normal budget to rebuild brand stock quickly."
                )

            # Floor enforcement
            w1_floor_breached = False
            if w1_budget_pct < 100:
                implied_weekly = (h.get("residual_spend_equiv_weekly", 0) or 0) * (w1_budget_pct / 100)
                if implied_weekly < floor:
                    w1_floor_breached = True
                    w1_rationale += f" ⚠️ Floor check: ${implied_weekly:,.0f}/week < floor ${floor:,.0f}/week — cap reduction at floor."

            # Week 2: typically reverse of week 1 for cool-down channels
            if w1_action == "COOL_DOWN":
                w2_action     = "PULSE_ON"
                w2_budget_pct = 100
                w2_rationale  = (
                    f"Resume full pacing after 7-day cool-down. "
                    f"Residual will have decayed to ~{residual * (h['lambda_weekly'] ** 1):.0f}% "
                    f"by week 2 — optimal re-engagement window."
                )
            elif w1_action == "PULSE_ON" and "🔴" in status:
                w2_action     = "HOLD"
                w2_budget_pct = 100
                w2_rationale  = "Maintain rebuild pacing through week 2 to restore adstock baseline."
            else:
                w2_action     = w1_action
                w2_budget_pct = w1_budget_pct
                w2_rationale  = "Continue week 1 strategy."

            schedule.append({
                "channel": channel,
                "half_life_days": h["half_life_days"],
                "current_residual_pct": residual,
                "week_1": {
                    "period":         f"{week1_start.isoformat()} → {week1_end.isoformat()}",
                    "action":         w1_action,
                    "budget_pct":     w1_budget_pct,
                    "floor_breached": w1_floor_breached,
                    "rationale":      w1_rationale,
                },
                "week_2": {
                    "period":         f"{week2_start.isoformat()} → {week2_end.isoformat()}",
                    "action":         w2_action,
                    "budget_pct":     w2_budget_pct,
                    "floor_breached": False,
                    "rationale":      w2_rationale,
                },
            })

        return schedule

    # ── Output rendering ──────────────────────────────────────────────────────

    def _render_markdown(
        self,
        halo_matrix: list[dict],
        flighting_schedule: list[dict],
        organic_anomaly: dict,
        execution_mode: str,
        today: date,
        lookback_days: int,
    ) -> str:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Sort by residual descending
        halo_sorted = sorted(halo_matrix, key=lambda h: -h["residual_pct"])

        lines = [
            f"## 📡 Ad Decay & Carryover Brief — {now_str}",
            f"Lookback: {lookback_days} days · Mode: `{execution_mode}`",
            "",
        ]

        # ── Section 1: Adstock Halo Matrix ──────────────────────────────────
        if execution_mode in ("analyze", "full"):
            lines += [
                "---",
                "",
                "### 📊 Adstock Halo Matrix",
                "",
                "| Channel | λ (weekly) | Half-Life | Residual % | Equiv. Spend/Wk | Status |",
                "|---|---|---|---|---|---|",
            ]
            for h in halo_sorted:
                cd_flag = " 🔘" if h["cool_down_eligible"] else ""
                lines.append(
                    f"| **{h['channel']}** "
                    f"| {h['lambda_weekly']:.2f} "
                    f"| {h['half_life_days']} days "
                    f"| **{h['residual_pct']:.1f}%** "
                    f"| ${h['residual_spend_equiv_weekly']:,.0f}/wk "
                    f"| {h['status']}{cd_flag} |"
                )

            lines += [
                "",
                "> 🔘 = Cool-Down Gate eligible (recent surge + high residual + awareness channel type)",
                "",
                "---",
                "",
                "### Channel Detail",
                "",
            ]
            for h in halo_sorted:
                lines.append(f"#### {h['status']} {h['channel'].replace('_', ' ').title()}")
                lines.append(f"- **Decay rate:** λ_weekly = {h['lambda_weekly']:.2f}, λ_daily = {h['lambda_daily']:.3f}")
                lines.append(f"- **Half-life:** {h['half_life_days']} days "
                             f"(50% of peak adstock lost by day {h['half_life_days']:.0f}; "
                             f"~90% lost by day {h['half_life_days'] * 3.32:.0f})")
                lines.append(f"- **Max lag window:** {h['max_lag_days']} days "
                             f"(carry-over effectively zero after day {h['max_lag_days']})")
                lines.append(f"- **Current residual:** {h['residual_pct']:.1f}% of peak adstock "
                             f"≈ **${h['residual_spend_equiv_weekly']:,.0f}/week** still working in-market")
                lines.append(f"- **MMM posterior ROI:** {h['roi_mean']:.2f}x "
                             f"(carry-over is multiplying this ROI via halo effect)")
                lines.append(f"- **Floor spend:** ${h['floor_spend_weekly_usd']:,.0f}/week minimum "
                             f"to prevent brand recall erosion")
                if h["recent_surges"]:
                    s = h["recent_surges"][0]
                    lines.append(f"- **Recent surge:** {s['week_start']} "
                                 f"({s['days_since']} days ago) — "
                                 f"${s['surge_spend']:,.0f} vs ${s['avg_spend']:,.0f} avg "
                                 f"({s['surge_ratio']:.1f}× normal)")
                if h["cool_down_eligible"]:
                    lines.append("- **Cool-Down Gate: ELIGIBLE** — reduce to floor spend for 7–14 days to harvest decay tail")
                lines.append("")

        # ── Organic anomaly section ──────────────────────────────────────────
        if organic_anomaly.get("anomaly_detected"):
            lines += [
                "---",
                "",
                "### ⚠️ Organic Session Anomaly — External Event Detected",
                "",
                organic_anomaly.get("interpretation", ""),
                "",
            ]
            for detail in organic_anomaly.get("details", [])[:5]:
                flag_icon = "🔴" if not detail["paid_surge_correlated"] else "🟡"
                lines.append(
                    f"- {flag_icon} **{detail['date']}**: "
                    f"{detail['organic_sessions']} organic sessions "
                    f"({detail['surge_ratio']:.1f}× rolling avg of {detail['rolling_avg_sessions']:.0f}) "
                    f"— {'NOT correlated with paid surge' if not detail['paid_surge_correlated'] else 'correlated with paid activity'}"
                )
            lines.append("")

        # ── Section 2: Flighting Schedule ───────────────────────────────────
        if execution_mode in ("flight_plan", "full") and flighting_schedule:
            lines += [
                "---",
                "",
                "### 🗓️ Programmatic Flighting Schedule — Next 14 Days",
                "",
                f"| Channel | Week 1 ({(today + timedelta(days=1)).isoformat()} – {(today + timedelta(days=7)).isoformat()}) | Week 2 ({(today + timedelta(days=8)).isoformat()} – {(today + timedelta(days=14)).isoformat()}) |",
                "|---|---|---|",
            ]
            for s in flighting_schedule:
                w1 = s["week_1"]
                w2 = s["week_2"]
                floor_w = "⚠️ Floor" if w1["floor_breached"] else ""
                lines.append(
                    f"| **{s['channel']}** "
                    f"| `{w1['action']}` @ {w1['budget_pct']}% {floor_w}"
                    f"| `{w2['action']}` @ {w2['budget_pct']}% |"
                )

            lines += ["", "**Week-by-week rationale:**", ""]
            for s in flighting_schedule:
                lines.append(f"**{s['channel'].replace('_', ' ').title()}**")
                lines.append(f"- Week 1: {s['week_1']['rationale']}")
                lines.append(f"- Week 2: {s['week_2']['rationale']}")
                lines.append("")

        return "\n".join(lines)

    def _build_operator_vector(
        self,
        halo_matrix: list[dict],
        flighting_schedule: list[dict],
        today: date,
    ) -> list[dict]:
        """JSON instruction vector for the Operator agent."""
        now_iso = datetime.now(timezone.utc).isoformat()
        vector: list[dict] = []

        schedule_by_channel = {s["channel"]: s for s in flighting_schedule}

        for h in halo_matrix:
            ch  = h["channel"]
            sch = schedule_by_channel.get(ch, {})
            w1  = sch.get("week_1", {})
            w2  = sch.get("week_2", {})

            vector.append({
                "channel":              ch,
                "adstock_status":       h["status"].split(" ", 1)[-1] if " " in h["status"] else h["status"],
                "residual_pct":         h["residual_pct"],
                "half_life_days":       h["half_life_days"],
                "cool_down_eligible":   h["cool_down_eligible"],
                "floor_spend_weekly_usd": h["floor_spend_weekly_usd"],
                "week_1_action":        w1.get("action", "HOLD"),
                "week_1_budget_pct":    w1.get("budget_pct", 100),
                "week_1_floor_breach":  w1.get("floor_breached", False),
                "week_2_action":        w2.get("action", "HOLD"),
                "week_2_budget_pct":    w2.get("budget_pct", 100),
                "generated_at":         now_iso,
            })

        return vector
