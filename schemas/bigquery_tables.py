# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Expected BigQuery table schemas. Run create_tables() once during initial setup.
These match the tables the agents query and write.
"""
from google.cloud import bigquery
from tools.bigquery_client import get_client
from config import settings

DATASET = settings.gcp_dataset_id
PROJECT = settings.gcp_project_id


TABLES = {
    "sgtm_request_logs": [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("httpRequest", "JSON"),
    ],
    "ga4_sessions": [
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("ip_address", "STRING"),
        bigquery.SchemaField("page_hostname", "STRING"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("gclid", "STRING"),
    ],
    "salesforce_leads": [
        bigquery.SchemaField("lead_id", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("ip_address", "STRING"),
        bigquery.SchemaField("gclid", "STRING"),
        bigquery.SchemaField("ga_client_id", "STRING"),
        bigquery.SchemaField("utm_source", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "touchpoint_events": [
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("opportunity_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("touchpoint_timestamp", "TIMESTAMP"),
    ],
    "account_identity_map": [
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("email_domain", "STRING"),
        bigquery.SchemaField("stitched_at", "TIMESTAMP"),
    ],
    "attribution_results": [
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("influenced_opps", "INTEGER"),
        bigquery.SchemaField("weighted_credit", "FLOAT"),
        bigquery.SchemaField("period_start", "DATE"),
        bigquery.SchemaField("period_end", "DATE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ],
}


def create_tables(exist_ok: bool = True) -> None:
    client = get_client()
    dataset_ref = bigquery.DatasetReference(PROJECT, DATASET)

    # Ensure dataset exists
    try:
        client.create_dataset(dataset_ref)
        print(f"Created dataset {DATASET}")
    except Exception:
        if not exist_ok:
            raise

    for table_name, schema in TABLES.items():
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.create_table(table)
            print(f"  Created table {table_name}")
        except Exception:
            if not exist_ok:
                raise
            print(f"  Table {table_name} already exists, skipping")


if __name__ == "__main__":
    create_tables()
