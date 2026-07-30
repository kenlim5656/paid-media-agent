# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""Markov / Shapley attribution on tiny fixtures with known answers."""
import pytest

from tools.attribution_models import compute_markov, compute_shapley


def _path(*channels: str) -> dict:
    return {
        "touches": [
            {"channel": ch, "platform": "test", "campaign_id": "", "position": i + 1}
            for i, ch in enumerate(channels)
        ]
    }


# ── Markov removal effects ──────────────────────────────────────────────────────

def test_markov_single_channel_gets_all_credit():
    weights = compute_markov([_path("search")])
    assert weights == {"search": 1.0}


def test_markov_sequential_channels_split_equally():
    # One path search → social → conversion: removing either breaks the chain,
    # so removal effects are equal and normalize to 0.5 each.
    weights = compute_markov([_path("search", "social")])
    assert weights["search"] == pytest.approx(0.5)
    assert weights["social"] == pytest.approx(0.5)


def test_markov_weights_sum_to_one():
    paths = [
        _path("search"),
        _path("search", "social"),
        _path("display", "search", "social"),
    ]
    weights = compute_markov(paths)
    assert sum(weights.values()) == pytest.approx(1.0)
    assert all(w >= 0 for w in weights.values())


def test_markov_zero_conversions_raises():
    # All paths empty → baseline conversion rate 0 → must raise, not divide by
    # epsilon and fabricate removal effects (REVIEW 3.6).
    with pytest.raises(ValueError, match="baseline conversion rate is 0"):
        compute_markov([{"touches": []}, {"touches": []}])


# ── Shapley values ──────────────────────────────────────────────────────────────

def test_shapley_symmetric_channels_split_equally():
    # Two single-touch converting paths, one per channel — perfectly symmetric.
    weights = compute_shapley([_path("search"), _path("social")])
    assert weights["search"] == pytest.approx(0.5)
    assert weights["social"] == pytest.approx(0.5)


def test_shapley_single_channel_gets_all_credit():
    weights = compute_shapley([_path("search"), _path("search")])
    assert weights == {"search": pytest.approx(1.0)}


def test_shapley_weights_normalized():
    paths = [
        _path("search"),
        _path("search", "social"),
        _path("social", "display"),
        _path("display"),
    ]
    weights = compute_shapley(paths)
    assert sum(weights.values()) == pytest.approx(1.0)
    assert set(weights) == {"search", "social", "display"}
    assert all(w >= 0 for w in weights.values())
