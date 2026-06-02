# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Meridian MMM — Data Extraction & Tensor Building Layer (Component 1 / Task 27).

Pulls multi-channel geo-level performance data from our BigQuery unified reporting
layer and assembles the structured NumPy arrays that Google Meridian requires for
Bayesian Media Mix Modeling.

Tensor shapes produced (following Meridian's [G, T, C] convention):

    kpi          [G, T]       — conversions (or revenue) per geo per week
    media        [G, T, C]    — raw impressions per geo per week per channel
    media_spend  [G, T, C]    — spend (NUMERIC→float) per geo per week per channel
    controls     [G, T, V]    — control variables: baseline GA4 sessions + seasonal indices
    population   [G]          — relative geo population weights (defaults to ones)

Index registry (always maintained alongside every tensor):

    geo_index      list[str]  — maps integer axis-0 coordinate → geo label (ISO 3166-1 country)
    time_index     list[str]  — maps integer axis-1 coordinate → ISO week label "YYYY-Www"
    channel_index  list[str]  — maps integer axis-2 coordinate → platform/channel name
    control_index  list[str]  — maps integer axis-2 coordinate → control variable name

Temporal aggregation: daily rows are aggregated to Monday-Sunday ISO weeks.
Geos with fewer than `min_weekly_impressions` average weekly impressions across
all channels are filtered out to avoid zero-inflation in the Meridian prior.

Data sources (paid-media-schema):
    platform_daily_spend   — spend + impressions per campaign per geo per day
    platform_campaigns     — channel/platform metadata for pivot labelling
    ga4_sessions           — baseline web sessions for control variable construction
    conversion_events      — KPI (conversions) per geo per day

Install dependencies:
    pip install 'paid-media-agent[mmm]'
    i.e. pandas, numpy, pyarrow, google-meridian, jax[cpu], numpyro
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Sequence

import structlog

log = structlog.get_logger()

# ── Dependency check ───────────────────────────────────────────────────────────


def _check_deps() -> None:
    missing = []
    for pkg, import_name in [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("pyarrow", "pyarrow"),
    ]:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pkg)
    if missing:
        raise ImportError(
            f"Missing MMM dependencies: {', '.join(missing)}. "
            "Install with: pip install 'paid-media-agent[mmm]'"
        )


# ── Data contract ──────────────────────────────────────────────────────────────


@dataclass
class MeridianInputData:
    """
    Fully self-describing input package for the Meridian model.

    All NumPy arrays use float64. Tensor shapes strictly follow Meridian's
    [Geos, Time, Channels] convention (geo axis always first).

    Attributes
    ----------
    kpi : ndarray [G, T]
        Target KPI (conversions or revenue) per geo per week.
        Use conversion_events.platform_conversions as proxy before true
        multi-touch KPI data is available.

    media : ndarray [G, T, C]
        Raw media volume (impressions) per geo × week × channel.
        Meridian uses this to fit the Hill saturation curve.

    media_spend : ndarray [G, T, C]
        Media cost (USD) per geo × week × channel.
        Meridian uses this to compute ROI and response curves.

    controls : ndarray [G, T, V]
        Exogenous control variables per geo × week × variable.
        Currently: [baseline_sessions, season_cos, season_sin].

    population : ndarray [G]
        Relative geo population weights (1.0 for equal weighting).
        Override with actual population data for geo-weighted models.

    geo_index : list[str]
        Axis-0 labels. geo_index[g] → ISO 3166-1 country code (e.g. "US").

    time_index : list[str]
        Axis-1 labels. time_index[t] → ISO week label "YYYY-Www" (e.g. "2025-W04").

    channel_index : list[str]
        Axis-2 labels for media/media_spend.
        channel_index[c] → platform channel (e.g. "google_ads", "meta", "tiktok").

    control_index : list[str]
        Axis-2 labels for controls.
        control_index[v] → control variable name (e.g. "baseline_sessions").

    date_from : str
        Earliest date included (YYYY-MM-DD), for audit traceability.

    date_to : str
        Latest date included (YYYY-MM-DD), for audit traceability.

    n_geos, n_weeks, n_channels, n_controls : int
        Convenience dimension properties.
    """
    kpi:           "np.ndarray"  # [G, T]
    media:         "np.ndarray"  # [G, T, C]
    media_spend:   "np.ndarray"  # [G, T, C]
    controls:      "np.ndarray"  # [G, T, V]
    population:    "np.ndarray"  # [G]

    geo_index:     list[str]
    time_index:    list[str]
    channel_index: list[str]
    control_index: list[str]

    date_from: str
    date_to:   str

    # ── Derived properties ─────────────────────────────────────────────────────

    @property
    def n_geos(self) -> int:
        return len(self.geo_index)

    @property
    def n_weeks(self) -> int:
        return len(self.time_index)

    @property
    def n_channels(self) -> int:
        return len(self.channel_index)

    @property
    def n_controls(self) -> int:
        return len(self.control_index)

    def summary(self) -> str:
        return (
            f"MeridianInputData\n"
            f"  Period   : {self.date_from} → {self.date_to}\n"
            f"  Shape    : [{self.n_geos} geos × {self.n_weeks} weeks × {self.n_channels} channels]\n"
            f"  Geos     : {self.geo_index}\n"
            f"  Channels : {self.channel_index}\n"
            f"  Controls : {self.control_index}\n"
            f"  KPI sum  : {float(self.kpi.sum()):.0f} total conversions\n"
            f"  Spend sum: ${float(self.media_spend.sum()):,.0f} total across all geos/channels\n"
        )

    def validate(self) -> None:
        """Assert that all tensor shapes are internally consistent."""
        G, T, C, V = self.n_geos, self.n_weeks, self.n_channels, self.n_controls
        assert self.kpi.shape         == (G, T),    f"kpi shape mismatch: {self.kpi.shape} ≠ ({G}, {T})"
        assert self.media.shape       == (G, T, C), f"media shape mismatch: {self.media.shape} ≠ ({G}, {T}, {C})"
        assert self.media_spend.shape == (G, T, C), f"media_spend shape mismatch: {self.media_spend.shape} ≠ ({G}, {T}, {C})"
        assert self.controls.shape    == (G, T, V), f"controls shape mismatch: {self.controls.shape} ≠ ({G}, {T}, {V})"
        assert self.population.shape  == (G,),       f"population shape mismatch: {self.population.shape} ≠ ({G},)"


# ── BigQuery query helpers ─────────────────────────────────────────────────────

_MEDIA_QUERY = """\
-- Meridian data loader: daily geo-level media metrics by platform channel.
-- Aggregated to channel (platform) to form the C dimension.
-- geo_country_code populated via migration 001 (geo dimensions on spend tables).
SELECT
    s.date,
    COALESCE(s.geo_country_code, 'XX')                     AS geo,
    c.platform                                              AS channel,
    SUM(CAST(s.impressions AS FLOAT64))                     AS impressions,
    SUM(CAST(s.spend AS FLOAT64))                           AS spend
FROM `{project}.{dataset}.platform_daily_spend` s
LEFT JOIN `{project}.{dataset}.platform_campaigns` c
       ON s.campaign_id = c.campaign_id
WHERE s.date BETWEEN '{date_from}' AND '{date_to}'
  AND s.geo_country_code IS NOT NULL
  AND c.platform IS NOT NULL
  AND s.impressions > 0
GROUP BY
    s.date,
    geo,
    channel
ORDER BY s.date, geo, channel
"""

_KPI_QUERY = """\
-- Meridian data loader: daily geo-level conversions (KPI).
-- Uses platform_conversions from platform_daily_spend as the KPI proxy.
-- Replace or supplement with attribution_results.credit_conversions when
-- geo-level MTA data is available (requires sessions.geo_country_code join).
SELECT
    s.date,
    COALESCE(s.geo_country_code, 'XX')                     AS geo,
    SUM(CAST(s.platform_conversions AS FLOAT64))            AS conversions
FROM `{project}.{dataset}.platform_daily_spend` s
WHERE s.date BETWEEN '{date_from}' AND '{date_to}'
  AND s.geo_country_code IS NOT NULL
GROUP BY s.date, geo
ORDER BY s.date, geo
"""

_SESSIONS_QUERY = """\
-- Meridian data loader: daily geo-level web sessions for the baseline control variable.
-- Joins ga4_sessions to ip_resolution_cache to recover the company_domain geo proxy,
-- then rolls up to country level. Falls back gracefully if ip_resolution_cache is empty.
SELECT
    gs.event_date                                           AS date,
    COALESCE(irc.geo_country_code, 'XX')                   AS geo,
    COUNT(DISTINCT gs.session_id)                           AS sessions
FROM `{project}.{dataset}.ga4_sessions` gs
LEFT JOIN `{project}.{dataset}.ip_resolution_cache` irc
       ON gs.ip_address = irc.ip_prefix_24     -- /24 prefix match (Task 30 privacy design)
WHERE gs.event_date BETWEEN '{date_from}' AND '{date_to}'
  AND irc.geo_country_code IS NOT NULL
GROUP BY date, geo
ORDER BY date, geo
"""


# ── Core loader ────────────────────────────────────────────────────────────────


def load_meridian_data(
    date_from: str,
    date_to: str,
    platforms: list[str] | None = None,
    geo_allowlist: list[str] | None = None,
    min_weekly_impressions: int = 1_000,
    include_sessions_control: bool = True,
    apply_attribution_correction: bool = False,
) -> MeridianInputData:
    """
    Extract and transform BigQuery performance data into Meridian-ready tensors.

    This is the primary entry point for Component 1. Runs three BigQuery queries,
    aggregates to ISO weekly intervals, pivots into [G × T × C] arrays, builds
    control variables, and assembles the complete MeridianInputData package.

    Parameters
    ----------
    date_from : str
        Start date for extraction, inclusive. Format: "YYYY-MM-DD".
        Recommendation: use at least 52 weeks for Meridian's seasonal decomposition.
        104 weeks (2 years) is ideal; 78 weeks (18 months) is the practical minimum.

    date_to : str
        End date for extraction, inclusive. Format: "YYYY-MM-DD".

    platforms : list[str] | None
        Restrict to specific platforms (e.g. ["google_ads", "meta", "tiktok"]).
        None = all platforms with geo-level data in the date range.

    geo_allowlist : list[str] | None
        Restrict to specific ISO country codes (e.g. ["US", "CA", "GB"]).
        None = all geos with sufficient data (after min_weekly_impressions filter).

    min_weekly_impressions : int
        Minimum average weekly impressions for a geo to be included.
        Geos below this threshold are dropped to avoid zero-inflation in MMM priors.
        Default 1,000. Reduce for smaller markets. Raise for high-volume accounts.

    include_sessions_control : bool
        If True, query ga4_sessions for a baseline traffic control variable.
        Set False if the sessions table is not yet populated.

    apply_attribution_correction : bool
        If True, load Task 37 attribution correction weights from
        v_attribution_correction_weights and apply them to the KPI tensor
        before returning. Correction multipliers in [0.60, 1.0) reduce KPI
        values for channel/geo/week combinations flagged by the forensic
        audit engine as contaminated by phantom conversions or CRM overwrites.
        Default False (use uncorrected platform-reported conversions).
        Run audit_data_attribution_cleanliness first to populate the view.

    Returns
    -------
    MeridianInputData
        Fully validated input package. Call .summary() for a human-readable overview
        and .validate() to assert shape consistency before passing to the engine.

    Raises
    ------
    ImportError
        If pandas / numpy / pyarrow are not installed (pip install 'paid-media-agent[mmm]').
    ValueError
        If the extracted data is too sparse to build a valid tensor
        (fewer than 2 geos or 8 weekly time periods).
    """
    _check_deps()
    import numpy as np
    import pandas as pd
    from tools.bigquery_client import get_client
    from config import settings

    log.info(
        "meridian_loader.start",
        date_from=date_from,
        date_to=date_to,
        platforms=platforms,
        geos=geo_allowlist,
    )

    project = settings.gcp_project_id
    dataset = settings.gcp_dataset_id

    # ── 1. Pull media data from BigQuery ──────────────────────────────────────

    client = get_client()

    def _run_bq(sql: str) -> pd.DataFrame:
        job = client.query(sql)
        return job.to_dataframe(create_bqstorage_client=False)  # pyarrow transport

    log.info("meridian_loader.querying_media")
    media_sql = _MEDIA_QUERY.format(
        project=project, dataset=dataset,
        date_from=date_from, date_to=date_to,
    )
    media_df = _run_bq(media_sql)

    log.info("meridian_loader.querying_kpi")
    kpi_sql = _KPI_QUERY.format(
        project=project, dataset=dataset,
        date_from=date_from, date_to=date_to,
    )
    kpi_df = _run_bq(kpi_sql)

    sessions_df: pd.DataFrame | None = None
    if include_sessions_control:
        log.info("meridian_loader.querying_sessions")
        try:
            sessions_sql = _SESSIONS_QUERY.format(
                project=project, dataset=dataset,
                date_from=date_from, date_to=date_to,
            )
            sessions_df = _run_bq(sessions_sql)
            log.info("meridian_loader.sessions_ok", rows=len(sessions_df))
        except Exception as exc:
            log.warning("meridian_loader.sessions_unavailable", error=str(exc))
            sessions_df = None

    # ── 2. Validate raw data ──────────────────────────────────────────────────

    if media_df.empty:
        raise ValueError(
            f"No geo-level media data found for {date_from}→{date_to}. "
            "Verify that: (1) migration 001 has been applied to populate geo_country_code, "
            "(2) platform_daily_spend has rows in this date range, "
            "(3) platform_campaigns is populated with platform values."
        )

    log.info(
        "meridian_loader.raw_data",
        media_rows=len(media_df),
        kpi_rows=len(kpi_df),
        unique_geos=media_df["geo"].nunique(),
        unique_channels=media_df["channel"].nunique(),
    )

    # ── 3. Aggregate to ISO weekly intervals (Monday start) ───────────────────

    def _to_iso_week(df: pd.DataFrame) -> pd.DataFrame:
        """Add iso_week column: 'YYYY-Www' based on ISO 8601 week (Monday start)."""
        dates = pd.to_datetime(df["date"])
        # ISO year + week: %G = ISO year, %V = ISO week number (01–53)
        df["iso_week"] = dates.dt.strftime("%G-W%V")
        # Week start date for consistent ordering
        df["week_start"] = dates - pd.to_timedelta(dates.dt.weekday, unit="D")
        return df

    media_df  = _to_iso_week(media_df)
    kpi_df    = _to_iso_week(kpi_df)
    if sessions_df is not None:
        sessions_df = _to_iso_week(sessions_df)

    # Filter platforms if specified
    if platforms:
        media_df = media_df[media_df["channel"].isin(platforms)]

    # Weekly aggregations
    media_weekly = (
        media_df.groupby(["geo", "iso_week", "week_start", "channel"], as_index=False)
        .agg(impressions=("impressions", "sum"), spend=("spend", "sum"))
    )
    kpi_weekly = (
        kpi_df.groupby(["geo", "iso_week", "week_start"], as_index=False)
        .agg(conversions=("conversions", "sum"))
    )
    if sessions_df is not None:
        sessions_weekly = (
            sessions_df.groupby(["geo", "iso_week", "week_start"], as_index=False)
            .agg(sessions=("sessions", "sum"))
        )

    # ── 4. Determine canonical geo and time axes ──────────────────────────────

    # Sorted time axis (ISO week labels are lexicographically sortable)
    all_weeks = sorted(media_weekly["iso_week"].unique())
    # All geos with any media data
    all_geos_raw = sorted(media_weekly["geo"].unique())

    # Filter geos by allowlist
    if geo_allowlist:
        all_geos_raw = [g for g in all_geos_raw if g in geo_allowlist]

    # Filter geos by minimum weekly impressions
    geo_avg_weekly = (
        media_weekly.groupby("geo")["impressions"].sum()
        / max(len(all_weeks), 1)
    )
    qualifying_geos = [
        g for g in all_geos_raw
        if geo_avg_weekly.get(g, 0) >= min_weekly_impressions
    ]

    if len(qualifying_geos) < 2:
        raise ValueError(
            f"Only {len(qualifying_geos)} geo(s) passed the min_weekly_impressions={min_weekly_impressions} "
            "filter. Meridian requires ≥ 2 geos for geo-level modeling. Options: "
            "(1) Lower min_weekly_impressions, (2) Add more geo-level spend data, "
            "(3) Extend the date range to include more data."
        )
    if len(all_weeks) < 8:
        raise ValueError(
            f"Only {len(all_weeks)} weeks in the date range. Meridian requires ≥ 8 weekly "
            "time periods for reliable adstock estimation. Extend date_from backward."
        )

    all_channels = sorted(media_weekly["channel"].unique())

    geo_index     = qualifying_geos
    time_index    = all_weeks
    channel_index = all_channels

    G = len(geo_index)
    T = len(time_index)
    C = len(channel_index)

    geo_to_i     = {g: i for i, g in enumerate(geo_index)}
    time_to_i    = {t: i for i, t in enumerate(time_index)}
    channel_to_i = {c: i for i, c in enumerate(channel_index)}

    log.info(
        "meridian_loader.dimensions",
        G=G, T=T, C=C,
        geos=geo_index,
        channels=channel_index,
    )

    # ── 5. Build media tensors [G, T, C] ──────────────────────────────────────

    media       = np.zeros((G, T, C), dtype=np.float64)
    media_spend = np.zeros((G, T, C), dtype=np.float64)

    for _, row in media_weekly.iterrows():
        g_i = geo_to_i.get(row["geo"])
        t_i = time_to_i.get(row["iso_week"])
        c_i = channel_to_i.get(row["channel"])
        if g_i is None or t_i is None or c_i is None:
            continue
        media[g_i, t_i, c_i]       += float(row["impressions"])
        media_spend[g_i, t_i, c_i] += float(row["spend"])

    # ── 6. Build KPI tensor [G, T] ────────────────────────────────────────────

    kpi = np.zeros((G, T), dtype=np.float64)

    for _, row in kpi_weekly.iterrows():
        g_i = geo_to_i.get(row["geo"])
        t_i = time_to_i.get(row["iso_week"])
        if g_i is None or t_i is None:
            continue
        kpi[g_i, t_i] += float(row["conversions"])

    # ── 7. Build controls tensor [G, T, V] ────────────────────────────────────

    # Control variables:
    #   [0] baseline_sessions  — GA4 session count (organic traffic proxy)
    #   [1] season_cos         — cosine of week-of-year (captures seasonal periodicity)
    #   [2] season_sin         — sine of week-of-year (phase-shifted seasonal component)
    #
    # Using both sin + cos of week-of-year captures the full annual cycle shape
    # without assuming a specific peak week. Meridian uses these as linear controls.

    week_of_year_map: dict[str, float] = {}
    week_start_map: dict[str, pd.Timestamp] = {}
    for _, row in media_weekly.drop_duplicates("iso_week").iterrows():
        ws = pd.to_datetime(row["week_start"])
        week_num = ws.isocalendar().week
        week_of_year_map[row["iso_week"]] = float(week_num)
        week_start_map[row["iso_week"]] = ws

    control_labels = ["baseline_sessions", "season_cos", "season_sin"]
    V = len(control_labels)
    controls = np.zeros((G, T, V), dtype=np.float64)

    # Populate seasonal controls (available for all geos × weeks regardless of data)
    for t_i, week_label in enumerate(time_index):
        woy = week_of_year_map.get(week_label, float(t_i % 52 + 1))
        cos_val = math.cos(2 * math.pi * woy / 52.0)
        sin_val = math.sin(2 * math.pi * woy / 52.0)
        controls[:, t_i, 1] = cos_val   # season_cos — same for all geos in a given week
        controls[:, t_i, 2] = sin_val   # season_sin

    # Populate baseline sessions if available
    if sessions_df is not None and not sessions_weekly.empty:
        for _, row in sessions_weekly.iterrows():
            g_i = geo_to_i.get(row["geo"])
            t_i = time_to_i.get(row["iso_week"])
            if g_i is None or t_i is None:
                continue
            controls[g_i, t_i, 0] += float(row["sessions"])

        # Normalize baseline_sessions to [0, 1] per geo to prevent scale dominance
        for g_i in range(G):
            max_sess = controls[g_i, :, 0].max()
            if max_sess > 0:
                controls[g_i, :, 0] /= max_sess

    # ── 8. Population weights [G] ─────────────────────────────────────────────

    # Default: equal weighting (1.0 per geo). Override with actual population data
    # (e.g., from a GCP BigQuery public census table) for geo-weighted models.
    population = np.ones(G, dtype=np.float64)

    # ── 9. Assemble and validate ──────────────────────────────────────────────

    result = MeridianInputData(
        kpi=kpi,
        media=media,
        media_spend=media_spend,
        controls=controls,
        population=population,
        geo_index=geo_index,
        time_index=time_index,
        channel_index=channel_index,
        control_index=control_labels,
        date_from=date_from,
        date_to=date_to,
    )
    result.validate()

    # ── Task 37: Attribution Correction Weights ────────────────────────────
    # Load forensic correction vectors from v_attribution_correction_weights and
    # apply per-channel/geo/week KPI multipliers if attribution auditing is enabled.
    if apply_attribution_correction:
        log.info(
            "meridian_loader.correction_weights.requested",
            note="Loading Task 37 attribution correction weights from BigQuery.",
        )
        weights = load_attribution_correction_weights(
            date_from=date_from,
            date_to=date_to,
        )
        if weights:
            result = apply_attribution_correction(
                data=result,
                correction_weights=weights,
                warn_only=False,
            )
        else:
            log.info(
                "meridian_loader.correction_weights.empty",
                note=(
                    "No attribution correction weights found. "
                    "Either the data is clean or audit_data_attribution_cleanliness "
                    "has not been run yet. Proceeding with uncorrected KPI tensor."
                ),
            )
    else:
        # Even when not applying corrections, check for contamination and warn.
        # This surfaces the issue without blocking the MMM run.
        try:
            weights = load_attribution_correction_weights(
                date_from=date_from,
                date_to=date_to,
            )
            if weights:
                contaminated = sorted({ch for (ch, _, _) in weights if ch in channel_index})
                if contaminated:
                    log.warning(
                        "meridian_loader.attribution_contamination_warning",
                        contaminated_channels=contaminated,
                        note=(
                            "Attribution anomalies exist for these channels in "
                            "v_attribution_correction_weights. "
                            "Pass apply_attribution_correction=True to apply "
                            "forensic correction vectors to the KPI tensor, "
                            "or run audit_data_attribution_cleanliness for a full report."
                        ),
                    )
        except Exception:
            pass  # passive check — never block the MMM run

    log.info(
        "meridian_loader.complete",
        kpi_total=float(result.kpi.sum()),
        spend_total=float(result.media_spend.sum()),
        impressions_total=float(result.media.sum()),
        shape=f"[{G}×{T}×{C}]",
        attribution_correction_applied=apply_attribution_correction,
    )

    return result


# ── Convenience helpers ────────────────────────────────────────────────────────


# ── Task 37 — Attribution Correction Weight Integration ───────────────────────

_CORRECTION_WEIGHT_QUERY = """\
-- Task 37 correction weights: channel / geo / ISO week multipliers.
-- Reads from v_attribution_correction_weights (rolling 90-day deduped anomaly window).
-- Returns only degraded/contaminated channel-geo-week combinations (multiplier < 1.0).
SELECT
    channel,
    geo_country_code,
    FORMAT_DATE('%G-W%V', week_start) AS iso_week,
    correction_multiplier,
    quality_tier,
    anomaly_count,
    phantom_conversion_count,
    weighted_severity_sum,
    estimated_at_risk_pipeline
FROM `{project}.{dataset}.v_attribution_correction_weights`
WHERE correction_multiplier < 1.0
ORDER BY correction_multiplier ASC
"""


def load_attribution_correction_weights(
    date_from: str,
    date_to: str,
) -> dict[tuple[str, str, str], float]:
    """
    Load Task 37 attribution correction multipliers from v_attribution_correction_weights.

    Returns a lookup dict keyed by (channel, geo_country_code, iso_week) → multiplier
    where multiplier is in [0.60, 1.0]. Missing keys imply multiplier = 1.0 (clean).

    Called by load_meridian_data() when apply_attribution_correction=True.
    Requires that audit_data_attribution_cleanliness has been run at least once
    to populate data_attribution_anomalies. Returns an empty dict (no corrections)
    if the forensics table has never been populated.

    Parameters
    ----------
    date_from : str
        Start of the MMM data window. Used for logging only — the view operates
        on a rolling 90-day anomaly window regardless of this parameter.
    date_to : str
        End of the MMM data window. Used for logging only.

    Returns
    -------
    dict[(channel, geo, iso_week), float]
        Correction multipliers. Apply to KPI tensor cell (g_i, t_i) for the
        matching channel / geo / week combination.
    """
    try:
        from config import settings
        from tools.bigquery_client import get_client
    except ImportError:
        log.warning("meridian_loader.correction_weights.import_failed")
        return {}

    project = settings.gcp_project_id
    dataset = settings.gcp_dataset_id
    sql = _CORRECTION_WEIGHT_QUERY.format(project=project, dataset=dataset)

    try:
        client = get_client()
        job = client.query(sql)
        rows = list(job.result())
    except Exception as exc:
        # Graceful degradation: if the forensics table hasn't been created yet,
        # or v_attribution_correction_weights has never been populated, log a
        # warning and proceed with uncorrected data.
        log.warning(
            "meridian_loader.correction_weights.unavailable",
            error=str(exc),
            note=(
                "v_attribution_correction_weights is not yet populated. "
                "Run audit_data_attribution_cleanliness at least once to generate "
                "correction vectors. Proceeding with uncorrected KPI tensor."
            ),
        )
        return {}

    weights: dict[tuple[str, str, str], float] = {}
    for row in rows:
        channel    = str(row["channel"] or "")
        geo        = str(row["geo_country_code"] or "XX")
        iso_week   = str(row["iso_week"] or "")
        multiplier = float(row["correction_multiplier"] or 1.0)
        if channel and iso_week and 0.0 < multiplier < 1.0:
            weights[(channel, geo, iso_week)] = multiplier

    log.info(
        "meridian_loader.correction_weights.loaded",
        date_from=date_from,
        date_to=date_to,
        contaminated_combinations=len(weights),
    )
    return weights


def apply_attribution_correction(
    data: "MeridianInputData",
    correction_weights: dict[tuple[str, str, str], float],
    warn_only: bool = False,
) -> "MeridianInputData":
    """
    Apply Task 37 attribution correction multipliers to the KPI tensor.

    For each (channel, geo, week) combination with a correction_multiplier < 1.0,
    reduces the corresponding KPI cell by the multiplier to compensate for
    phantom conversions and timestamp-divergence attribution inflation.

    The media and media_spend tensors are NOT modified — only KPI is adjusted.
    This is intentional: the spend data is factual; the conversion signal is what
    needs correcting.

    Parameters
    ----------
    data : MeridianInputData
        The fully assembled input package from load_meridian_data().

    correction_weights : dict[(channel, geo, iso_week), float]
        Output of load_attribution_correction_weights(). Empty dict = no-op.

    warn_only : bool
        If True, log engineering warnings but do NOT modify the KPI tensor.
        Useful for auditing the magnitude of corrections without applying them.

    Returns
    -------
    MeridianInputData
        Modified input package with KPI tensor adjusted.
        If warn_only=True or correction_weights is empty, returns data unchanged.
    """
    import numpy as np

    if not correction_weights:
        log.info("meridian_loader.correction.no_weights", note="KPI tensor unchanged.")
        return data

    # Identify which channels in the correction dict overlap with channel_index
    contaminated_channels: list[str] = sorted(
        {ch for (ch, _geo, _wk) in correction_weights if ch in data.channel_index}
    )

    if not contaminated_channels:
        log.info(
            "meridian_loader.correction.no_channel_overlap",
            correction_channels=sorted({ch for (ch, _, _) in correction_weights}),
            model_channels=data.channel_index,
            note="No correction channels match MMM channel_index. KPI tensor unchanged.",
        )
        return data

    # Engineering warning: always emit regardless of warn_only
    log.warning(
        "meridian_loader.attribution_contamination_detected",
        contaminated_channels=contaminated_channels,
        total_combinations=len(correction_weights),
        correction_applied=not warn_only,
        note=(
            "Attribution anomalies detected in v_attribution_correction_weights for "
            f"channel(s): {contaminated_channels}. "
            "These channels have phantom conversions or timestamp-divergence overwrites "
            "that inflate the platform-reported conversion signal used as the MMM KPI. "
            + (
                "Correction vectors applied to KPI tensor — multipliers in [0.60, 1.0)."
                if not warn_only
                else "warn_only=True — run with apply_attribution_correction=True to apply corrections."
            )
        ),
    )

    if warn_only:
        return data

    # Apply corrections: KPI[g_i, t_i] *= multiplier for each affected (geo, week)
    # We apply the MOST conservative (lowest) multiplier across all channels for a
    # given (geo, week) pair, since the KPI tensor is not channel-disaggregated.
    kpi_adjusted = data.kpi.copy()
    cells_corrected = 0

    geo_to_i  = {g: i for i, g in enumerate(data.geo_index)}
    week_to_i = {w: i for i, w in enumerate(data.time_index)}

    # Group corrections by (geo, iso_week), take minimum multiplier across channels
    gt_multipliers: dict[tuple[int, int], float] = {}
    for (channel, geo, iso_week), mult in correction_weights.items():
        if channel not in data.channel_index:
            continue  # skip channels not in this MMM run
        g_i = geo_to_i.get(geo)
        t_i = week_to_i.get(iso_week)
        if g_i is None or t_i is None:
            continue
        key = (g_i, t_i)
        gt_multipliers[key] = min(gt_multipliers.get(key, 1.0), mult)

    for (g_i, t_i), multiplier in gt_multipliers.items():
        original = kpi_adjusted[g_i, t_i]
        kpi_adjusted[g_i, t_i] = original * multiplier
        if original > 0:
            cells_corrected += 1

    log.info(
        "meridian_loader.correction.applied",
        cells_corrected=cells_corrected,
        kpi_original_sum=float(data.kpi.sum()),
        kpi_corrected_sum=float(kpi_adjusted.sum()),
        reduction_pct=round(
            (1.0 - float(kpi_adjusted.sum()) / max(float(data.kpi.sum()), 1e-9)) * 100,
            2,
        ),
    )

    # Return new MeridianInputData with adjusted KPI tensor
    return MeridianInputData(
        kpi=kpi_adjusted,
        media=data.media,
        media_spend=data.media_spend,
        controls=data.controls,
        population=data.population,
        geo_index=data.geo_index,
        time_index=data.time_index,
        channel_index=data.channel_index,
        control_index=data.control_index,
        date_from=data.date_from,
        date_to=data.date_to,
    )


def lookup_geo(data: MeridianInputData, geo: str) -> int:
    """Return the integer axis-0 index for a geo label. Raises ValueError if not found."""
    try:
        return data.geo_index.index(geo)
    except ValueError:
        raise ValueError(f"Geo '{geo}' not in index. Available: {data.geo_index}")


def lookup_channel(data: MeridianInputData, channel: str) -> int:
    """Return the integer axis-2 index for a channel label."""
    try:
        return data.channel_index.index(channel)
    except ValueError:
        raise ValueError(f"Channel '{channel}' not in index. Available: {data.channel_index}")


def lookup_week(data: MeridianInputData, iso_week: str) -> int:
    """Return the integer axis-1 index for an ISO week label (e.g. '2025-W04')."""
    try:
        return data.time_index.index(iso_week)
    except ValueError:
        raise ValueError(f"Week '{iso_week}' not in index. Available range: {data.time_index[0]}→{data.time_index[-1]}")


def describe_tensor(data: MeridianInputData) -> dict:
    """
    Return a diagnostic dict for logging and the MMM run record in BigQuery.
    Contains shape metadata, spend totals, and data sparsity metrics.
    """
    import numpy as np
    return {
        "n_geos":              data.n_geos,
        "n_weeks":             data.n_weeks,
        "n_channels":          data.n_channels,
        "n_controls":          data.n_controls,
        "geo_index":           data.geo_index,
        "channel_index":       data.channel_index,
        "control_index":       data.control_index,
        "date_from":           data.date_from,
        "date_to":             data.date_to,
        "kpi_total":           float(data.kpi.sum()),
        "kpi_mean_per_geo_week": float(data.kpi.mean()),
        "kpi_zero_pct":        float((data.kpi == 0).mean() * 100),
        "spend_total_usd":     float(data.media_spend.sum()),
        "impressions_total":   float(data.media.sum()),
        "media_zero_pct":      float((data.media == 0).mean() * 100),
        # Per-channel totals (useful for spend allocation visibility)
        "spend_by_channel":    {
            ch: float(data.media_spend[:, :, i].sum())
            for i, ch in enumerate(data.channel_index)
        },
        "impressions_by_channel": {
            ch: float(data.media[:, :, i].sum())
            for i, ch in enumerate(data.channel_index)
        },
    }
