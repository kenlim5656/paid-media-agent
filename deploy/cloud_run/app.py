# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Minimal FastAPI wrapper for Cloud Run.
Cloud Scheduler POSTs to /run?agent=<name> on its cron.
"""
from fastapi import FastAPI, HTTPException, Query
from orchestrator.runner import run_watchdog, run_analyst, run_operator

app = FastAPI(title="Attribution Agent Runner")


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


@app.get("/health")
def health():
    return {"status": "ok"}
