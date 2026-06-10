# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Operator task27 execution: pre-flight sweep (bad package ⇒ zero mutations),
idempotency replay blocking, and honest terminal statuses.

No network: platform mutation clients, BigQuery lookups, and audit writes are
all monkeypatched. A mutation call on a path that must not mutate fails the
test immediately.
"""
import copy

import pytest

import tools.google_ads_operator as gops
import tools.tiktok_ads_operator as tops
from agents.operator.agent import OperatorAgent, _hash_execution_package

VALID_PACKAGE = {
    "schema_version": "task27.v1",
    "operator_approval_required": True,
    "mmm_run_id": "mmm-test-run",
    "max_shift_pct_policy": 10.0,
    "recommendations": [
        {
            "channel": "google_ads",
            "direction": "increase",
            "recommended_shift_pct": 8.0,
            "recommended_shift_usd": 4.0,
            "new_target_budget_usd": 54.0,
            "confidence_tier": "high",
        },
        {
            "channel": "tiktok",
            "direction": "decrease",
            "recommended_shift_pct": -6.0,
            "recommended_shift_usd": -3.0,
            "new_target_budget_usd": 47.0,
            "confidence_tier": "medium",
        },
    ],
}

CHANNEL_MAP = {
    "google_ads": {"customer_id": "111", "campaign_id": "222"},
    "tiktok": {"advertiser_id": "333", "campaign_id": "444"},
}


@pytest.fixture
def operator(monkeypatch):
    """OperatorAgent with all I/O stubbed; records every mutation call."""
    calls: list[tuple] = []

    monkeypatch.setattr(
        gops, "modify_google_campaign_budget",
        lambda **kw: calls.append(("google_ads", kw)) or {"status": "updated", **kw},
    )
    monkeypatch.setattr(
        tops, "modify_tiktok_campaign_budget",
        lambda **kw: calls.append(("tiktok", kw)) or {"status": "updated", **kw},
    )
    monkeypatch.setattr(OperatorAgent, "_find_executed_package", lambda self, h: None)
    monkeypatch.setattr(OperatorAgent, "_update_action_status", lambda self, a, s: True)
    monkeypatch.setattr(
        OperatorAgent, "_record_package_execution", lambda self, **kw: True
    )

    agent = OperatorAgent()
    agent._test_mutation_calls = calls
    return agent


# ── Schema / approval guards ────────────────────────────────────────────────────

def test_unknown_schema_version_rejected(operator):
    pkg = copy.deepcopy(VALID_PACKAGE)
    pkg["schema_version"] = "task99.v1"
    result = operator._tool_execute_system_budget_reallocation("a-1", pkg, CHANNEL_MAP)
    assert result["executed"] is False
    assert operator._test_mutation_calls == []


def test_tampered_approval_flag_rejected(operator):
    pkg = copy.deepcopy(VALID_PACKAGE)
    pkg["operator_approval_required"] = False
    result = operator._tool_execute_system_budget_reallocation("a-1", pkg, CHANNEL_MAP)
    assert result["executed"] is False
    assert operator._test_mutation_calls == []


# ── Pre-flight sweep: bad package ⇒ zero mutations ─────────────────────────────

def test_shift_over_policy_blocks_entire_package(operator):
    pkg = copy.deepcopy(VALID_PACKAGE)
    pkg["recommendations"][0]["recommended_shift_pct"] = 25.0  # over the 10% policy
    result = operator._tool_execute_system_budget_reallocation("a-1", pkg, CHANNEL_MAP)
    assert result["preflight_failed"] is True
    assert result["executed"] is False
    # The OTHER (valid) channel must not execute either — all-or-nothing
    assert operator._test_mutation_calls == []


def test_budget_floor_breach_blocks_entire_package(operator):
    pkg = copy.deepcopy(VALID_PACKAGE)
    pkg["recommendations"][0]["new_target_budget_usd"] = 3.0  # below $5 Google floor
    result = operator._tool_execute_system_budget_reallocation("a-1", pkg, CHANNEL_MAP)
    assert result["preflight_failed"] is True
    assert operator._test_mutation_calls == []


def test_missing_channel_map_blocks_entire_package(operator):
    result = operator._tool_execute_system_budget_reallocation(
        "a-1", VALID_PACKAGE, {"google_ads": CHANNEL_MAP["google_ads"]}
    )
    assert result["preflight_failed"] is True
    assert operator._test_mutation_calls == []


def test_preflight_reports_all_errors_at_once(operator):
    pkg = copy.deepcopy(VALID_PACKAGE)
    pkg["recommendations"][0]["recommended_shift_pct"] = 25.0
    pkg["recommendations"][1]["new_target_budget_usd"] = 1.0  # below $20 TikTok floor
    result = operator._tool_execute_system_budget_reallocation("a-1", pkg, CHANNEL_MAP)
    assert result["error_count"] == 2


# ── Happy path + terminal status ───────────────────────────────────────────────

def test_valid_package_executes_all_channels(operator):
    result = operator._tool_execute_system_budget_reallocation(
        "a-1", VALID_PACKAGE, CHANNEL_MAP
    )
    assert result["terminal_status"] == "executed"
    assert result["executed"] == 2
    assert result["failed"] == 0
    assert [c[0] for c in operator._test_mutation_calls] == ["google_ads", "tiktok"]


def test_partial_failure_reports_partial_status(operator, monkeypatch):
    def boom(**kw):
        raise gops.GoogleAdsAPIError("simulated platform failure")
    monkeypatch.setattr(gops, "modify_google_campaign_budget", boom)

    result = operator._tool_execute_system_budget_reallocation(
        "a-1", VALID_PACKAGE, CHANNEL_MAP
    )
    assert result["terminal_status"] == "partial"
    assert result["executed"] == 1
    assert result["failed"] == 1
    # TikTok (channel 2) still executed — semantics documented in README
    assert [c[0] for c in operator._test_mutation_calls] == [("tiktok")]


def test_all_failures_report_failed_status(operator, monkeypatch):
    def gboom(**kw):
        raise gops.GoogleAdsAPIError("down")
    def tboom(**kw):
        raise tops.TikTokAdsError("down")
    monkeypatch.setattr(gops, "modify_google_campaign_budget", gboom)
    monkeypatch.setattr(tops, "modify_tiktok_campaign_budget", tboom)

    result = operator._tool_execute_system_budget_reallocation(
        "a-1", VALID_PACKAGE, CHANNEL_MAP
    )
    assert result["terminal_status"] == "failed"
    assert result["executed"] == 0


# ── Idempotency ─────────────────────────────────────────────────────────────────

def test_replay_of_executed_package_is_blocked(operator, monkeypatch):
    monkeypatch.setattr(
        OperatorAgent,
        "_find_executed_package",
        lambda self, h: {"action_id": "prior-action", "status": "executed",
                         "executed_at": "2026-06-08T00:00:00Z"},
    )
    result = operator._tool_execute_system_budget_reallocation(
        "a-2", VALID_PACKAGE, CHANNEL_MAP
    )
    assert result["replay_blocked"] is True
    assert result["executed"] is False
    assert result["prior_action_id"] == "prior-action"
    assert operator._test_mutation_calls == []


def test_idempotency_check_failure_fails_closed(operator, monkeypatch):
    def boom(self, h):
        raise RuntimeError("BigQuery unreachable")
    monkeypatch.setattr(OperatorAgent, "_find_executed_package", boom)

    result = operator._tool_execute_system_budget_reallocation(
        "a-3", VALID_PACKAGE, CHANNEL_MAP
    )
    assert result["executed"] is False
    assert "replay protection" in result["reason"]
    assert operator._test_mutation_calls == []


def test_package_hash_is_stable_and_order_insensitive():
    a = {"x": 1, "y": [1, 2], "z": "s"}
    b = {"z": "s", "y": [1, 2], "x": 1}  # same content, different key order
    assert _hash_execution_package(a) == _hash_execution_package(b)
    assert _hash_execution_package(a) != _hash_execution_package({**a, "x": 2})
