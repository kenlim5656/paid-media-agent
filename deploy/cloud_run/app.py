# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
FastAPI wrapper for Cloud Run.

Routes:
    POST /run?agent=<name>              — Cloud Scheduler cron entry point
    POST /query/account-journey         — read-only journey lookup (MCP)
    POST /action/audience-suppression   — Operator-guarded write (MCP)
    POST /action/reallocate-budget      — Operator-guarded write (MCP)
    GET  /health                        — liveness probe

The /query and /action routes are the server side of the paid-media-mcp
integration (PAID_MEDIA_AGENT_URL). Action routes run through the Operator's
log_proposed_action → execution-tool path, so approval gating and budget-cap
guardrails apply identically to HTTP-initiated and autonomous actions.

Auth: every route except /health requires a Google-signed OIDC identity token
(`Authorization: Bearer <id-token>`), verified in-process against Google's
signing keys — defense-in-depth on top of Cloud Run's
--no-allow-unauthenticated. Configure via HTTP_AUTH_AUDIENCE (the Cloud Run
URL) and HTTP_AUTH_ALLOWED_EMAILS (caller service accounts); disable for
local development only with HTTP_AUTH_ENABLED=false.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import settings, validate_settings
from orchestrator import http_actions
from orchestrator.runner import run_watchdog, run_analyst, run_operator


@asynccontextmanager
async def lifespan(_app: FastAPI):
    validate_settings()  # fail at startup, not on the first request
    yield


app = FastAPI(title="Attribution Agent Runner", lifespan=lifespan)


# ── OIDC verification ───────────────────────────────────────────────────────────

def _verify_id_token(token: str) -> dict:
    """
    Verify a Google-signed OIDC identity token (signature, expiry, audience)
    and enforce the service-account email allowlist. Raises ValueError on any
    failure — callers translate that to 401.
    """
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token as google_id_token

    claims = google_id_token.verify_oauth2_token(
        token,
        google_requests.Request(),
        audience=settings.http_auth_audience or None,
    )
    allowed = [e.strip() for e in settings.http_auth_allowed_emails.split(",") if e.strip()]
    if allowed:
        email = claims.get("email", "")
        if not claims.get("email_verified") or email not in allowed:
            raise ValueError(f"Caller '{email}' is not in HTTP_AUTH_ALLOWED_EMAILS")
    return claims


@app.middleware("http")
async def require_oidc(request: Request, call_next):
    if request.url.path == "/health" or not settings.http_auth_enabled:
        return await call_next(request)

    header = request.headers.get("authorization", "")
    if not header.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing 'Authorization: Bearer <id-token>' header"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        # verify_oauth2_token fetches Google's certs synchronously — keep it
        # off the event loop
        await run_in_threadpool(_verify_id_token, header.removeprefix("Bearer "))
    except ValueError as exc:
        return JSONResponse(
            status_code=401,
            content={"detail": f"Invalid identity token: {exc}"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    return await call_next(request)


# ── Request models ──────────────────────────────────────────────────────────────

class AccountJourneyRequest(BaseModel):
    account_domain: str = Field(min_length=3, max_length=255)
    lookback_days: int = Field(default=90, ge=1, le=730)
    conversion_type: str | None = None


class AudienceSuppressionRequest(BaseModel):
    platform: str = Field(min_length=2, max_length=40)
    advertiser_id: str = Field(min_length=1, max_length=128)
    audience_list_id: str = Field(min_length=1, max_length=256)
    domains: list[str] = Field(min_length=1, max_length=10_000)
    rationale: str = Field(min_length=1, max_length=4_000)


class BudgetReallocationRequest(BaseModel):
    platform: str = Field(min_length=2, max_length=40)
    advertiser_id: str = Field(min_length=1, max_length=128)
    source_campaign_id: str = Field(min_length=1, max_length=128)
    target_campaign_id: str = Field(min_length=1, max_length=128)
    amount_usd: float = Field(gt=0)
    rationale: str = Field(min_length=1, max_length=4_000)


# ── Routes ──────────────────────────────────────────────────────────────────────

@app.post("/run")
def run_agent(agent: str = Query(..., description="watchdog | analyst | operator")):
    match agent:
        case "watchdog":
            result = run_watchdog()
        case "analyst":
            result = run_analyst()
        case "operator":
            result = run_operator()
        case _:
            raise HTTPException(status_code=400, detail=f"Unknown agent: {agent}")
    return {"agent": agent, "result": result}


@app.post("/query/account-journey")
def query_account_journey(req: AccountJourneyRequest):
    try:
        return http_actions.query_account_journey(
            account_domain=req.account_domain,
            lookback_days=req.lookback_days,
            conversion_type=req.conversion_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/action/audience-suppression")
def action_audience_suppression(req: AudienceSuppressionRequest):
    try:
        return http_actions.push_audience_suppression(
            platform=req.platform,
            advertiser_id=req.advertiser_id,
            audience_list_id=req.audience_list_id,
            domains=req.domains,
            rationale=req.rationale,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/action/reallocate-budget")
def action_reallocate_budget(req: BudgetReallocationRequest):
    try:
        return http_actions.reallocate_budget(
            platform=req.platform,
            advertiser_id=req.advertiser_id,
            source_campaign_id=req.source_campaign_id,
            target_campaign_id=req.target_campaign_id,
            amount_usd=req.amount_usd,
            rationale=req.rationale,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/health")
def health():
    return {"status": "ok"}
