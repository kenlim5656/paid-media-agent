# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Data-driven attribution models: Shapley Value and Markov Chain.

Both models consume conversion path data from BigQuery and return
per-channel credit weights compatible with the attribution_results schema.

These are called by the Analyst agent as tool implementations.
"""
import math
import itertools
import random
import structlog
from collections import defaultdict
from datetime import datetime, timezone

from tools import bigquery_client as bq

log = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Shared: path data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_paths(
    period_start: str,
    period_end: str,
    conversion_types: list[str] | None = None,
    max_paths: int = 10_000,
) -> list[dict]:
    """
    Load conversion paths from BigQuery.
    Returns a list of paths, each as:
      {
        path_id: str,
        entity_id: str,
        conversion_id: str,
        conversion_type: str,
        conversion_value: float,
        deal_value: float,
        touches: [{"channel": str, "platform": str, "campaign_id": str,
                   "touchpoint_id": str, "touchpoint_at": str, "position": int}]
      }
    """
    conv_filter = ""
    if conversion_types:
        quoted = ", ".join(f"'{t}'" for t in conversion_types)
        conv_filter = f"AND c.conversion_type IN ({quoted})"

    # Pull paths directly from touchpoint + conversion join
    sql = f"""
    WITH numbered AS (
        SELECT
            TO_HEX(MD5(CONCAT(ies.entity_id, c.conversion_id))) AS path_id,
            ies.entity_id,
            c.conversion_id,
            c.conversion_type,
            COALESCE(c.conversion_value, 0)                    AS conversion_value,
            COALESCE(c.deal_value, 0)                          AS deal_value,
            t.touchpoint_id,
            t.channel,
            t.platform,
            t.campaign_id,
            t.touchpoint_at,
            ROW_NUMBER() OVER (
                PARTITION BY ies.entity_id, c.conversion_id
                ORDER BY t.touchpoint_at ASC
            )                                                  AS position
        FROM {bq.table_ref('touchpoint_events')} t
        JOIN {bq.table_ref('identity_entity_signals')} ies
          ON ies.namespace_id = 'analytics_cookie.google.ga4_client_id'
         AND ies.identifier_value = t.session_id
        JOIN {bq.table_ref('conversion_events')} c
          ON c.entity_id = ies.entity_id
         AND c.converted_at > t.touchpoint_at
         AND DATE(c.converted_at) BETWEEN '{period_start}' AND '{period_end}'
        WHERE DATE(t.touchpoint_at) BETWEEN
              DATE_SUB(DATE '{period_start}', INTERVAL 90 DAY) AND '{period_end}'
        {conv_filter}
    )
    SELECT * FROM numbered
    ORDER BY path_id, position
    LIMIT {max_paths * 20}   -- over-fetch rows; we'll group into paths below
    """
    rows = bq.run_query(sql)

    # Group rows into path dicts
    paths: dict[str, dict] = {}
    for r in rows:
        pid = str(r["path_id"])
        if pid not in paths:
            paths[pid] = {
                "path_id":         pid,
                "entity_id":       str(r["entity_id"]),
                "conversion_id":   str(r["conversion_id"]),
                "conversion_type": str(r["conversion_type"]),
                "conversion_value": float(r.get("conversion_value") or 0),
                "deal_value":      float(r.get("deal_value") or 0),
                "touches":         [],
            }
        paths[pid]["touches"].append({
            "touchpoint_id": str(r["touchpoint_id"]),
            "channel":       str(r["channel"]),
            "platform":      str(r["platform"]),
            "campaign_id":   str(r.get("campaign_id") or ""),
            "touchpoint_at": str(r["touchpoint_at"]),
            "position":      int(r["position"]),
        })

    path_list = list(paths.values())

    # Sample if over limit
    if len(path_list) > max_paths:
        path_list = random.sample(path_list, max_paths)
        log.info("attribution_models.paths_sampled", total=len(paths), sampled=max_paths)

    log.info("attribution_models.paths_loaded", count=len(path_list))
    return path_list


def _channel_key(touch: dict) -> str:
    """Stable channel identifier for model computations."""
    return touch["channel"]


# ─────────────────────────────────────────────────────────────────────────────
# Shapley Value Model
# ─────────────────────────────────────────────────────────────────────────────

def compute_shapley(
    paths: list[dict],
    max_channels: int = 10,
) -> dict[str, float]:
    """
    Compute Shapley values for each channel across all conversion paths.

    The Shapley value φᵢ for channel i is the weighted average of its marginal
    contribution across all possible orderings of channels.

    φᵢ = Σₛ [|S|!(n-|S|-1)!/n!] × [v(S ∪ {i}) - v(S)]

    where v(S) = conversion probability given touchpoints in coalition S.

    For performance: uses a sampling approximation when n > 6 channels.

    Returns: {channel: shapley_weight}  — weights sum to ~1.0 across paths.
    """
    # Count total conversions per channel combination (the characteristic function v)
    channel_conversion_counts: dict[frozenset, float] = defaultdict(float)
    channel_total_counts: dict[frozenset, int] = defaultdict(int)

    # All unique channels (capped)
    all_channels: set[str] = set()
    for path in paths:
        for touch in path["touches"]:
            all_channels.add(_channel_key(touch))

    if len(all_channels) > max_channels:
        # Keep the top N channels by path frequency
        freq: dict[str, int] = defaultdict(int)
        for path in paths:
            for touch in path["touches"]:
                freq[_channel_key(touch)] += 1
        all_channels = set(sorted(freq, key=freq.get, reverse=True)[:max_channels])  # type: ignore[arg-type]
        log.info("shapley.channels_capped", kept=max_channels, total=len(freq))

    channels = sorted(all_channels)
    n = len(channels)

    # Build characteristic function: v(S) = # conversions where path ⊆ S / # paths through S
    for path in paths:
        path_channels = frozenset(_channel_key(t) for t in path["touches"] if _channel_key(t) in all_channels)
        if not path_channels:
            continue
        # Each subset of path_channels that is a superset or equal contributes
        # For efficiency: track (coalition → conversion count)
        channel_conversion_counts[path_channels] += 1.0
        channel_total_counts[path_channels] += 1

    def v(coalition: frozenset) -> float:
        """Value function: conversion rate for this coalition."""
        if not coalition:
            return 0.0
        # Use only paths whose channel set equals or is a subset of coalition
        converts = sum(
            cnt for s, cnt in channel_conversion_counts.items()
            if s.issubset(coalition)
        )
        total = sum(
            cnt for s, cnt in channel_total_counts.items()
            if s.issubset(coalition)
        )
        return converts / max(total, 1)

    # Compute Shapley values
    shapley: dict[str, float] = {}

    for i, channel in enumerate(channels):
        others = [c for c in channels if c != channel]
        phi = 0.0

        if n <= 8:
            # Exact computation: enumerate all subsets of others
            for size in range(len(others) + 1):
                weight = (math.factorial(size) * math.factorial(n - size - 1)
                          / math.factorial(n))
                for subset in itertools.combinations(others, size):
                    S = frozenset(subset)
                    marginal = v(S | {channel}) - v(S)
                    phi += weight * marginal
        else:
            # Monte Carlo approximation (1000 random permutations)
            mc_samples = 1000
            for _ in range(mc_samples):
                perm = random.sample(channels, len(channels))
                pos = perm.index(channel)
                S = frozenset(perm[:pos])
                phi += (v(S | {channel}) - v(S))
            phi /= mc_samples

        shapley[channel] = phi

    # Normalize so weights sum to 1.0
    total = sum(abs(v) for v in shapley.values())
    if total > 0:
        shapley = {k: abs(v) / total for k, v in shapley.items()}

    log.info("shapley.complete", channels=list(shapley.keys()))
    return shapley


# ─────────────────────────────────────────────────────────────────────────────
# Markov Chain Model
# ─────────────────────────────────────────────────────────────────────────────

def compute_markov(paths: list[dict]) -> dict[str, float]:
    """
    Compute channel attribution via Markov Chain removal effects.

    Steps:
    1. Build a transition matrix from all touchpoint sequences.
       States: START, channel_1, channel_2, ..., CONVERSION, NULL (no conversion).
    2. For each channel, remove its state from the graph and recompute
       the conversion probability.
    3. Removal effect = (base_conversion_rate - rate_without_channel) / base_conversion_rate
    4. Normalize removal effects to sum to 1.0 → these are the attribution weights.

    Returns: {channel: markov_weight}
    """
    START = "__START__"
    CONV  = "__CONVERSION__"
    NULL  = "__NULL__"

    # Transition counts: {from_state: {to_state: count}}
    transitions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_paths = len(paths)

    for path in paths:
        if not path["touches"]:
            transitions[START][NULL] += 1
            continue
        # Sequence: START → ch1 → ch2 → ... → CONVERSION (or NULL if no conv)
        sequence = [START] + [_channel_key(t) for t in sorted(path["touches"], key=lambda x: x["position"])] + [CONV]
        for j in range(len(sequence) - 1):
            transitions[sequence[j]][sequence[j + 1]] += 1

    # Convert counts to probabilities
    trans_prob: dict[str, dict[str, float]] = {}
    for from_state, to_counts in transitions.items():
        total = sum(to_counts.values())
        trans_prob[from_state] = {to: cnt / total for to, cnt in to_counts.items()}

    def conversion_rate(exclude_channel: str | None = None) -> float:
        """Walk the Markov chain to get conversion probability, optionally removing a channel."""
        # States: all channels except excluded
        states = set(transitions.keys()) | {START, CONV, NULL}
        if exclude_channel and exclude_channel in states:
            states.discard(exclude_channel)

        # Redistribute excluded channel's transitions proportionally
        tp = {}
        for from_state in states:
            if from_state in (CONV, NULL):
                continue
            row = {
                to: prob
                for to, prob in trans_prob.get(from_state, {}).items()
                if to in states or to in (CONV, NULL)
            }
            if not row:
                row = {NULL: 1.0}
            total = sum(row.values())
            if total > 0:
                row = {k: v / total for k, v in row.items()}
            tp[from_state] = row

        # Iterative computation of absorption probability (conversion)
        # p[state] = probability of eventually reaching CONV from this state
        p: dict[str, float] = {CONV: 1.0, NULL: 0.0}
        for state in states - {CONV, NULL}:
            p[state] = 0.0

        # Power iteration until convergence
        for _ in range(200):
            new_p = {CONV: 1.0, NULL: 0.0}
            for state in states - {CONV, NULL}:
                new_p[state] = sum(
                    prob * p.get(to, 0.0)
                    for to, prob in tp.get(state, {}).items()
                )
            if all(abs(new_p.get(s, 0) - p.get(s, 0)) < 1e-6 for s in states):
                break
            p = {**p, **new_p}

        return p.get(START, 0.0)

    base_rate = conversion_rate(exclude_channel=None)
    if base_rate == 0:
        base_rate = 1e-9  # avoid division by zero

    # All unique channels (excluding special states)
    channels = sorted(
        s for s in transitions.keys()
        if s not in (START, CONV, NULL)
    )

    removal_effects: dict[str, float] = {}
    for channel in channels:
        rate_without = conversion_rate(exclude_channel=channel)
        removal_effects[channel] = max(0.0, (base_rate - rate_without) / base_rate)

    # Normalize to sum to 1.0
    total = sum(removal_effects.values())
    if total > 0:
        markov_weights = {k: v / total for k, v in removal_effects.items()}
    else:
        # Fallback: equal credit if no removal effect detected
        markov_weights = {k: 1.0 / len(channels) for k in channels}

    log.info("markov.complete", channels=list(markov_weights.keys()), base_rate=round(base_rate, 4))
    return markov_weights


# ─────────────────────────────────────────────────────────────────────────────
# Write results to BigQuery schema tables
# ─────────────────────────────────────────────────────────────────────────────

def write_model_results(
    run_id: str,
    model_name: str,
    period_start: str,
    period_end: str,
    paths: list[dict],
    channel_weights: dict[str, float],
) -> dict:
    """
    Write per-touchpoint attribution results to attribution_results and
    rebuild attribution_channel_summary using the computed model weights.

    channel_weights: {channel: weight_0_to_1}  — from compute_shapley or compute_markov
    """
    now = datetime.now(timezone.utc).isoformat()

    # Clear any existing rows for this run
    bq.run_dml(f"DELETE FROM {bq.table_ref('attribution_results')} WHERE run_id = @run_id", params={"run_id": run_id})

    # Build rows
    result_rows = []
    for path in paths:
        touches = path["touches"]
        if not touches:
            continue
        total_touches = len(touches)

        for touch in touches:
            ch = _channel_key(touch)
            weight = channel_weights.get(ch, 0.0)
            # Split path weight equally if channel not in model (should not happen)
            if weight == 0 and len(channel_weights) == 0:
                weight = 1.0 / total_touches

            conv_value = path["conversion_value"]
            deal_value = path["deal_value"]

            result_rows.append({
                "result_id":         bq.new_uuid(),
                "run_id":            run_id,
                "path_id":           path["path_id"],
                "touchpoint_id":     touch["touchpoint_id"],
                "conversion_id":     path["conversion_id"],
                "entity_id":         path["entity_id"],
                "conversion_date":   period_end,
                "touchpoint_date":   touch["touchpoint_at"][:10],
                "platform":          touch["platform"],
                "channel":           ch,
                "campaign_id":       touch["campaign_id"],
                "touchpoint_type":   "click",
                "path_position":     touch["position"],
                "path_total_touches": total_touches,
                "conversion_type":   path["conversion_type"],
                "conversion_value":  conv_value,
                "deal_value":        deal_value,
                "credit_weight":     weight,
                "credit_conversions": weight,
                "credit_value":      conv_value * weight,
                "credit_deal_value": deal_value * weight,
                "model_name":        model_name,
                "period_start":      period_start,
                "period_end":        period_end,
                "created_at":        now,
            })

    # Batch insert (streaming API in chunks of 500)
    total_errors = 0
    chunk_size = 500
    for i in range(0, len(result_rows), chunk_size):
        chunk = result_rows[i:i + chunk_size]
        errors = bq.insert_rows("attribution_results", chunk)
        total_errors += len(errors)

    log.info("attribution_models.results_written", run_id=run_id, rows=len(result_rows), errors=total_errors)

    # Aggregate to channel summary
    channel_summary: dict[str, dict] = {}
    for row in result_rows:
        ch = row["channel"]
        pl = row["platform"]
        ct = row["conversion_type"]
        key = f"{pl}|{ch}|{ct}"
        if key not in channel_summary:
            channel_summary[key] = {
                "summary_id":            bq.new_uuid(),
                "run_id":                run_id,
                "model_name":            model_name,
                "period_start":          period_start,
                "period_end":            period_end,
                "platform":              pl,
                "channel":               ch,
                "conversion_type":       ct,
                "funnel_stage":          None,
                "total_touches":         0,
                "unique_entities":       set(),
                "first_touch_count":     0,
                "last_touch_count":      0,
                "attributed_conversions": 0.0,
                "attributed_value":      0.0,
                "attributed_deal_value": 0.0,
                "credit_share_pct":      0.0,
                "total_spend":           None,
                "attributed_cpa":        None,
                "attributed_roas":       None,
                "platform_conversions":  None,
                "platform_cpa":          None,
                "attribution_vs_platform_delta_pct": None,
                "generated_at":          now,
            }
        s = channel_summary[key]
        s["total_touches"] += 1
        s["unique_entities"].add(row["entity_id"])
        if row["path_position"] == 1:
            s["first_touch_count"] += 1
        if row["path_position"] == row["path_total_touches"]:
            s["last_touch_count"] += 1
        s["attributed_conversions"] += row["credit_conversions"]
        s["attributed_value"] += row["credit_value"]
        s["attributed_deal_value"] += row["credit_deal_value"]

    total_credit = sum(v["attributed_conversions"] for v in channel_summary.values())
    summary_rows = []
    for s in channel_summary.values():
        s["unique_entities"] = len(s["unique_entities"])  # convert set → count
        s["credit_share_pct"] = (
            round(s["attributed_conversions"] / total_credit * 100, 2)
            if total_credit > 0 else 0.0
        )
        summary_rows.append(s)

    bq.run_dml(f"DELETE FROM {bq.table_ref('attribution_channel_summary')} WHERE run_id = @run_id", params={"run_id": run_id})
    for i in range(0, len(summary_rows), chunk_size):
        bq.insert_rows("attribution_channel_summary", summary_rows[i:i + chunk_size])

    top = sorted(summary_rows, key=lambda x: x["attributed_conversions"], reverse=True)[:10]
    return {
        "run_id":             run_id,
        "model_name":         model_name,
        "paths_modeled":      len(paths),
        "result_rows_written": len(result_rows),
        "channel_rows_written": len(summary_rows),
        "errors":             total_errors,
        "top_channels":       [
            {
                "channel":               t["channel"],
                "platform":              t["platform"],
                "attributed_conversions": round(t["attributed_conversions"], 2),
                "credit_share_pct":       t["credit_share_pct"],
            }
            for t in top
        ],
    }
