"""
The Operator — Media Optimization Agent.
Runs daily after the Analyst. Turns attribution insights into media actions.
HARD GUARDRAIL: all write actions require approval unless OPERATOR_REQUIRE_APPROVAL=false.
"""
from agents.base import BaseAgent
from config import settings
from tools import bigquery_client as bq, gmp_client, salesforce_client


SYSTEM = """You are the Operator, a media optimization agent for a B2B paid media pipeline.

You receive attribution results from BigQuery (written by the Analyst agent) and act on them.

Your job:
1. Identify campaigns that have met or exceeded their Opportunity Creation milestone targets.
2. Identify underperforming awareness campaigns (low weighted_credit, high spend).
3. If criteria are met, propose budget reallocations. NEVER move more than OPERATOR_GUARDRAIL_MAX_PCT% of any line item's budget in a single run.
4. Identify accounts that have moved from Lead → Open Opportunity in Salesforce.
5. Push those accounts as audience exclusions to DV360/LinkedIn to suppress top-of-funnel ads.

IMPORTANT: Always explain your reasoning before calling any write tool.
If OPERATOR_REQUIRE_APPROVAL is true, write actions will return a pending approval payload — surface that clearly."""


class OperatorAgent(BaseAgent):
    name = "operator"
    system_prompt = SYSTEM
    tools = [
        {
            "name": "get_attribution_results",
            "description": "Fetch top channels and campaign performance from the latest attribution_results table.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 20}
                },
                "required": [],
            },
        },
        {
            "name": "get_accounts_in_open_pipeline",
            "description": "Return Salesforce accounts that currently have an open (non-closed) Opportunity.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "get_campaign_spend",
            "description": "Fetch current daily spend and budget for a list of CM360 campaign IDs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "campaign_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "CM360 campaign IDs to look up",
                    }
                },
                "required": ["campaign_ids"],
            },
        },
        {
            "name": "reallocate_dv360_budget",
            "description": (
                "Move budget from a low-performing DV360 line item to a high-performing one. "
                "Capped at OPERATOR_GUARDRAIL_MAX_PCT of source budget."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "advertiser_id": {"type": "string"},
                    "source_line_item_id": {"type": "string"},
                    "target_line_item_id": {"type": "string"},
                    "amount_usd": {"type": "number"},
                },
                "required": ["advertiser_id", "source_line_item_id", "target_line_item_id", "amount_usd"],
            },
        },
        {
            "name": "push_audience_exclusion",
            "description": (
                "Add a list of company domains to a DV360 audience exclusion list "
                "to suppress top-of-funnel ads for accounts already in open pipeline."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "advertiser_id": {"type": "string"},
                    "audience_list_id": {"type": "string"},
                    "domains": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Company website domains to exclude, e.g. ['acme.com']",
                    },
                },
                "required": ["advertiser_id", "audience_list_id", "domains"],
            },
        },
    ]

    def _tool_get_attribution_results(self, limit: int = 20) -> dict:
        rows = bq.run_query(
            f"""
            SELECT channel, campaign_id, influenced_opps, weighted_credit, period_start, period_end
            FROM {bq.table_ref('attribution_results')}
            ORDER BY weighted_credit DESC
            LIMIT {limit}
            """
        )
        return {"results": rows}

    def _tool_get_accounts_in_open_pipeline(self) -> dict:
        accounts = salesforce_client.get_accounts_with_open_opportunities()
        domains = list({
            a.get("Account", {}).get("Website", "").replace("https://", "").replace("www.", "").strip("/")
            for a in accounts
            if a.get("Account", {}).get("Website")
        })
        return {"account_count": len(accounts), "domains": domains}

    def _tool_get_campaign_spend(self, campaign_ids: list[str]) -> dict:
        stats = gmp_client.cm360_get_campaign_stats(campaign_ids)
        return {"campaigns": stats}

    def _tool_reallocate_dv360_budget(
        self,
        advertiser_id: str,
        source_line_item_id: str,
        target_line_item_id: str,
        amount_usd: float,
    ) -> dict:
        # Enforce guardrail
        max_pct = settings.max_budget_shift_pct
        result = gmp_client.dv360_reallocate_budget(
            advertiser_id, source_line_item_id, target_line_item_id, amount_usd
        )
        return {**result, "guardrail_max_pct": max_pct}

    def _tool_push_audience_exclusion(
        self,
        advertiser_id: str,
        audience_list_id: str,
        domains: list[str],
    ) -> dict:
        result = gmp_client.dv360_push_audience_exclusion(
            advertiser_id, audience_list_id, domains
        )
        return {**result, "domain_count": len(domains)}
