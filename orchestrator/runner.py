# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Orchestrator: runs the three agents on their respective schedules.
Can be invoked directly (python -m orchestrator.runner --agent watchdog)
or triggered by Cloud Scheduler hitting a Cloud Run endpoint.
"""
import argparse
import sys
from datetime import date, timedelta
import structlog

log = structlog.get_logger()


def run_watchdog() -> str:
    from agents.watchdog.agent import WatchdogAgent
    agent = WatchdogAgent()
    return agent.run(
        "Run your hourly data governance audit. "
        "1. Call audit_signal_capture_rates (hours_back=1) for all monitored namespaces. "
        "2. Call audit_crm_null_fields (since_hours=1). "
        "3. For any metric below threshold, call write_alert with full diagnosis. "
        "4. Call log_capture_rates with all measurements regardless of status. "
        "Return a terse status: GREEN / YELLOW / RED with specific metrics."
    )


def run_analyst() -> str:
    from agents.analyst.agent import AnalystAgent
    agent = AnalystAgent()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()
    return agent.run(
        f"Run the daily attribution analysis. "
        f"1. Call start_attribution_run (model: full_path, period: {thirty_days_ago} to {yesterday}). "
        f"2. Call stitch_identities (lookback_days=1) to process yesterday's new sessions. "
        f"3. Call run_mta_model with the run_id for the 30-day window. "
        f"4. Call build_channel_summary with the run_id. "
        f"5. Call write_analyst_insight with your most important finding. "
        f"6. Call complete_attribution_run to mark the run done. "
        f"Return a summary of top channels by attributed conversions and any anomalies."
    )


def run_operator(analyst_summary: str = "") -> str:
    from agents.operator.agent import OperatorAgent
    agent = OperatorAgent()
    context = f"\nAnalyst summary from today's run:\n{analyst_summary}\n" if analyst_summary else ""
    return agent.run(
        f"Run the daily media optimization pass.{context}"
        "1. Call get_attribution_summary to see channel performance. "
        "2. Call get_accounts_in_open_pipeline to get domains for suppression. "
        "3. For each proposed action: call log_proposed_action FIRST, then the execution tool. "
        "4. Execute push_audience_suppression for domains in open pipeline. "
        "5. Execute reallocate_budget for channels with high credit but underfunding. "
        "Explain your attribution-based reasoning for every action proposed."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent", choices=["watchdog", "analyst", "operator", "all"], required=True)
    args = parser.parse_args()

    from config import validate_settings
    validate_settings()

    if args.agent == "watchdog":
        result = run_watchdog()
    elif args.agent == "analyst":
        result = run_analyst()
    elif args.agent == "operator":
        result = run_operator()
    elif args.agent == "all":
        # Sequential: analyst output feeds into operator context
        log.info("orchestrator.start", sequence="watchdog→analyst→operator")
        watchdog_out = run_watchdog()
        log.info("watchdog.complete")
        analyst_out = run_analyst()
        log.info("analyst.complete")
        result = run_operator(analyst_summary=analyst_out)
        log.info("operator.complete")
    else:
        sys.exit(1)

    print(result)


if __name__ == "__main__":
    main()
