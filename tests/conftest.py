# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Test environment bootstrap. Must run before any `config` import: settings are
instantiated at import time and require ANTHROPIC_API_KEY + a GCP project.
No test in this suite touches the network — anything that would (BigQuery,
platform APIs, Anthropic) is monkeypatched in the individual tests.
"""
import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-real")
os.environ.setdefault("PAID_MEDIA_GCP_PROJECT", "test-project")
os.environ.setdefault("PAID_MEDIA_BQ_DATASET", "test_dataset")
os.environ.setdefault("OPERATOR_REQUIRE_APPROVAL", "true")
os.environ.setdefault("MAX_BUDGET_SHIFT_PCT", "10")
os.environ.setdefault("HTTP_AUTH_ENABLED", "false")
