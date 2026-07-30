# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
Shared HTTP retry helper for platform-client READ paths.

Retries transient failures (connection errors, timeouts, HTTP 429/5xx) with
exponential backoff. On the final attempt the response is returned as-is, so
every client keeps its own error taxonomy (`_raise_for_meta_error`, etc.) —
this helper only absorbs blips, it never reinterprets errors.

WRITE paths must NOT use this: the platform mutation endpoints have no
idempotency keys, so a retried write that actually landed the first time
would apply twice (see REVIEW 3.2/3.7).
"""
from __future__ import annotations

import time

import httpx
import structlog

log = structlog.get_logger()

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3


def get_with_retry(url: str, **kwargs) -> httpx.Response:
    """
    httpx.get with up to MAX_ATTEMPTS tries on transient failures.
    Drop-in replacement for httpx.get on read paths.
    """
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = httpx.get(url, **kwargs)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            log.warning(
                "http_retry.transport_error",
                url=url, attempt=attempt, max_attempts=MAX_ATTEMPTS, error=str(exc),
            )
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue

        if resp.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            log.warning(
                "http_retry.retryable_status",
                url=url, status=resp.status_code, attempt=attempt,
            )
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue

        # Success, non-retryable status, or final attempt — the caller's own
        # error handling takes it from here.
        return resp

    raise last_exc if last_exc else RuntimeError("unreachable")
