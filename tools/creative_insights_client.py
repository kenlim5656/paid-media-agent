# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Creative performance intelligence — ad copy extraction and asset-type analysis.

Reads from platform_ads and platform_daily_spend_ad to surface the historical
top-performing copy strings and creative formats for the copy assistant tool.

This module provides two query surfaces:
  get_top_performing_ads()               — ranked by CVR/CTR/ROAS, returns copy text
  get_asset_type_performance_correlation() — aggregate CVR/CTR by creative_format

Privacy note: ad copy strings are marketing content, not PII. No email, phone,
or personal data is ever returned or logged from this module.
"""

from datetime import date, timedelta

import structlog

from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Constants ─────────────────────────────────────────────────────────────────

_RANK_METRICS   = {"cvr", "ctr", "roas", "attributed_cpa"}
_ASSET_TYPES    = {"image", "video", "carousel", "responsive", "native", "text"}
_MIN_IMPRESSIONS = 100   # filter out statistically insignificant ads
_MIN_CLICKS      = 10    # minimum clicks required for CVR stability


def get_top_performing_ads(
    channels: list[str] | None = None,
    lookback_days: int = 90,
    rank_by: str = "cvr",
    limit: int = 20,
    asset_type: str | None = None,
) -> list[dict]:
    """
    Return the top-performing ads by copy string, ranked by the given metric.

    Joins platform_daily_spend_ad with platform_ads to retrieve:
      - headline, description, creative_format, ad_name
      - Aggregate CTR, CVR (platform_conversions / clicks), spend, conversions
      - Asset format classification for cross-format comparisons

    Useful as few-shot context for the copy assistant — shows the agent which
    copy patterns (specific hook styles, CTAs, value prop framing) historically
    drove the highest conversion rates or click-through rates.

    Args:
        channels:      List of channel strings, e.g. ["google_ads", "meta"].
                       None = all channels.
        lookback_days: Calendar days to look back from today (default 90).
        rank_by:       "cvr" | "ctr" | "roas" | "attributed_cpa"
                       CVR and CTR are ascending/descending respectively.
                       attributed_cpa is ascending (lower cost = better).
        limit:         Maximum ads to return (default 20).
        asset_type:    Filter to a specific creative_format if provided.

    Returns:
        List of dicts with keys:
          ad_id, channel, platform, headline, description, creative_format,
          ad_name, total_spend, total_clicks, total_impressions,
          total_conversions, ctr, cvr, cpm
    """
    if rank_by not in _RANK_METRICS:
        raise ValueError(
            f"rank_by must be one of {_RANK_METRICS}. Got: {rank_by!r}"
        )
    if asset_type and asset_type not in _ASSET_TYPES:
        raise ValueError(
            f"asset_type must be one of {_ASSET_TYPES}. Got: {asset_type!r}"
        )

    since = (date.today() - timedelta(days=lookback_days)).isoformat()

    channel_filter = ""
    if channels:
        quoted = ", ".join(f"'{c}'" for c in channels)
        channel_filter = f"AND s.channel IN ({quoted})"

    asset_filter = ""
    if asset_type:
        asset_filter = f"AND a.creative_format = '{asset_type}'"

    # Build ORDER BY expression
    if rank_by == "cvr":
        order_expr = "cvr DESC NULLS LAST"
    elif rank_by == "ctr":
        order_expr = "ctr DESC NULLS LAST"
    elif rank_by == "roas":
        order_expr = (
            "SAFE_DIVIDE(total_conversions, "
            "NULLIF(CAST(total_spend AS FLOAT64), 0)) DESC NULLS LAST"
        )
    else:  # attributed_cpa — lower is better
        order_expr = (
            "SAFE_DIVIDE(CAST(total_spend AS FLOAT64), "
            "NULLIF(total_conversions, 0)) ASC NULLS LAST"
        )

    sql = f"""
        SELECT
            s.ad_id,
            s.channel,
            s.platform,
            a.headline,
            a.description,
            a.creative_format,
            a.ad_name,
            SUM(CAST(s.spend AS FLOAT64))                                       AS total_spend,
            SUM(s.clicks)                                                       AS total_clicks,
            SUM(s.impressions)                                                  AS total_impressions,
            SUM(s.platform_conversions)                                         AS total_conversions,
            SAFE_DIVIDE(SUM(s.clicks), NULLIF(SUM(s.impressions), 0))          AS ctr,
            SAFE_DIVIDE(SUM(s.platform_conversions), NULLIF(SUM(s.clicks), 0)) AS cvr,
            SAFE_DIVIDE(CAST(SUM(s.spend) AS FLOAT64) * 1000,
                        NULLIF(SUM(s.impressions), 0))                         AS cpm
        FROM {bq.table_ref('platform_daily_spend_ad')} s
        JOIN {bq.table_ref('platform_ads')} a USING (ad_id)
        WHERE s.date >= '{since}'
          AND a.headline IS NOT NULL
          AND s.impressions >= {_MIN_IMPRESSIONS}
          {channel_filter}
          {asset_filter}
        GROUP BY
            s.ad_id, s.channel, s.platform,
            a.headline, a.description, a.creative_format, a.ad_name
        HAVING SUM(s.clicks) >= {_MIN_CLICKS}
        ORDER BY {order_expr}
        LIMIT {limit}
    """
    rows = bq.run_query(sql)
    log.info(
        "creative_insights.top_ads_fetched",
        count=len(rows),
        rank_by=rank_by,
        lookback_days=lookback_days,
        channels=channels,
        asset_type=asset_type,
    )
    return rows


def get_asset_type_performance_correlation(
    channels: list[str] | None = None,
    lookback_days: int = 90,
) -> list[dict]:
    """
    Return aggregate performance grouped by creative_format.

    Surfaces which asset types (image, video, carousel, responsive, etc.) drive
    the highest CVR and CTR across the lookback window. Used by the copy assistant
    to inform the visual creative brief's format and placement recommendations —
    e.g., if video shows 3× the CVR of static images, the brief should lead with
    a video concept.

    Args:
        channels:      List of channel strings. None = all channels.
        lookback_days: Calendar days to look back (default 90).

    Returns:
        List of dicts sorted by avg_cvr DESC:
          creative_format, ad_count, avg_ctr, avg_cvr, avg_cpm,
          total_spend, total_conversions, share_of_conversions_pct
    """
    since = (date.today() - timedelta(days=lookback_days)).isoformat()

    channel_filter = ""
    if channels:
        quoted = ", ".join(f"'{c}'" for c in channels)
        channel_filter = f"AND s.channel IN ({quoted})"

    sql = f"""
        WITH ad_agg AS (
            SELECT
                COALESCE(a.creative_format, 'unknown')                          AS creative_format,
                COUNT(DISTINCT s.ad_id)                                         AS ad_count,
                SAFE_DIVIDE(SUM(s.clicks), NULLIF(SUM(s.impressions), 0))      AS avg_ctr,
                SAFE_DIVIDE(SUM(s.platform_conversions),
                            NULLIF(SUM(s.clicks), 0))                          AS avg_cvr,
                SAFE_DIVIDE(CAST(SUM(s.spend) AS FLOAT64) * 1000,
                            NULLIF(SUM(s.impressions), 0))                     AS avg_cpm,
                CAST(SUM(s.spend) AS FLOAT64)                                   AS total_spend,
                SUM(s.platform_conversions)                                     AS total_conversions
            FROM {bq.table_ref('platform_daily_spend_ad')} s
            JOIN {bq.table_ref('platform_ads')} a USING (ad_id)
            WHERE s.date >= '{since}'
              AND a.creative_format IS NOT NULL
              {channel_filter}
            GROUP BY a.creative_format
        )
        SELECT
            *,
            SAFE_DIVIDE(
                total_conversions * 100.0,
                SUM(total_conversions) OVER ()
            ) AS share_of_conversions_pct
        FROM ad_agg
        ORDER BY avg_cvr DESC NULLS LAST
    """
    rows = bq.run_query(sql)
    log.info(
        "creative_insights.asset_correlation_fetched",
        count=len(rows),
        lookback_days=lookback_days,
        channels=channels,
    )
    return rows


# ── Formatting helpers (used by operator agent tool) ──────────────────────────

def format_few_shot_context(top_ads: list[dict], max_examples: int = 5) -> str:
    """
    Format top-performing ads as a structured few-shot text block for inclusion
    in the Claude creative generation prompt.

    Returns a compact, human-readable string listing each ad's copy and metrics.
    """
    if not top_ads:
        return "(No historical ad data available — generate from scratch.)"

    lines = ["=== Historical Top-Performing Ads (few-shot context) ==="]
    for i, ad in enumerate(top_ads[:max_examples], 1):
        ctr_pct = f"{ad.get('ctr', 0) * 100:.1f}%" if ad.get("ctr") else "n/a"
        cvr_pct = f"{ad.get('cvr', 0) * 100:.2f}%" if ad.get("cvr") else "n/a"
        fmt = ad.get("creative_format") or "unknown"
        lines += [
            f"\n[{i}] Channel: {ad.get('channel', 'unknown')} | Format: {fmt}",
            f"    Headline:    {ad.get('headline', '(none)')}",
            f"    Description: {ad.get('description', '(none)')}",
            f"    CTR: {ctr_pct}  CVR: {cvr_pct}  Spend: ${ad.get('total_spend', 0):.0f}",
        ]
    return "\n".join(lines)


def format_asset_correlation_context(correlations: list[dict]) -> str:
    """
    Format asset-type performance data as a briefing note for creative format selection.
    """
    if not correlations:
        return "(No asset-type data available.)"

    lines = ["=== Asset Format Performance (last 90 days) ==="]
    for row in correlations:
        cvr_pct = f"{row.get('avg_cvr', 0) * 100:.2f}%" if row.get("avg_cvr") else "n/a"
        ctr_pct = f"{row.get('avg_ctr', 0) * 100:.1f}%" if row.get("avg_ctr") else "n/a"
        share   = f"{row.get('share_of_conversions_pct', 0):.1f}%"
        lines.append(
            f"  {row.get('creative_format', 'unknown'):14s}  "
            f"CVR: {cvr_pct}  CTR: {ctr_pct}  "
            f"Conv share: {share}  ({row.get('ad_count', 0)} ads)"
        )
    return "\n".join(lines)
