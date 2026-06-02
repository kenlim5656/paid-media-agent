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
        "Check GTM capture rates and Salesforce null media fields. "
        "Send an alert if any threshold is breached. Return a status summary."
    )


def run_analyst() -> str:
    from agents.analyst.agent import AnalystAgent
    agent = AnalystAgent()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return agent.run(
        f"Run the daily attribution analysis for {yesterday}. "
        "First run account stitching for yesterday's data, "
        "then execute the Full-Path MTA model for the past 30 days to update attribution_results. "
        "Return a summary of top channels and any data anomalies."
    )


def run_operator(analyst_summary: str = "") -> str:
    from agents.operator.agent import OperatorAgent
    agent = OperatorAgent()
    context = f"\nAnalyst summary from today's run:\n{analyst_summary}\n" if analyst_summary else ""
    return agent.run(
        f"Run the daily media optimization pass.{context}"
        "Fetch attribution results, identify accounts in open pipeline, "
        "and propose or execute budget reallocation and audience exclusion actions. "
        "Explain your reasoning for each action."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent", choices=["watchdog", "analyst", "operator", "all"], required=True)
    args = parser.parse_args()

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
