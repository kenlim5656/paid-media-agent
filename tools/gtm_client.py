# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Query Server-Side GTM logs via BigQuery (GA4 export or sGTM log sink).
Assumes Cloud Logging is sinking sGTM request logs to BigQuery.
"""
from tools.bigquery_client import run_query, table_ref


def get_gclid_capture_rate(hours_back: int = 1) -> dict:
    """
    Returns the percentage of sessions that arrived with a gclid
    over the past N hours, from sGTM request logs.
    """
    sql = f"""
    WITH requests AS (
        SELECT
            COUNTIF(JSON_EXTRACT_SCALAR(httpRequest.requestUrl, '$.gclid') IS NOT NULL) AS with_gclid,
            COUNT(*) AS total
        FROM {table_ref('sgtm_request_logs')}
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
          AND httpRequest.requestMethod = 'POST'
    )
    SELECT
        with_gclid,
        total,
        SAFE_DIVIDE(with_gclid, total) * 100 AS capture_rate_pct
    FROM requests
    """
    rows = run_query(sql, {"hours": hours_back})
    return rows[0] if rows else {"with_gclid": 0, "total": 0, "capture_rate_pct": 0.0}


def get_client_id_capture_rate(hours_back: int = 1) -> dict:
    sql = f"""
    WITH requests AS (
        SELECT
            COUNTIF(JSON_EXTRACT_SCALAR(httpRequest.requestUrl, '$.client_id') IS NOT NULL) AS with_client_id,
            COUNT(*) AS total
        FROM {table_ref('sgtm_request_logs')}
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
          AND httpRequest.requestMethod = 'POST'
    )
    SELECT
        with_client_id,
        total,
        SAFE_DIVIDE(with_client_id, total) * 100 AS capture_rate_pct
    FROM requests
    """
    rows = run_query(sql, {"hours": hours_back})
    return rows[0] if rows else {"with_client_id": 0, "total": 0, "capture_rate_pct": 0.0}
