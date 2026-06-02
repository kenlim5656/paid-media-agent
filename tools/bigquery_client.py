from google.cloud import bigquery
from config import settings

_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=settings.gcp_project_id)
    return _client


def run_query(sql: str, params: dict | None = None) -> list[dict]:
    client = get_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, _infer_type(v), v)
            for k, v in (params or {}).items()
        ]
    )
    job = client.query(sql, job_config=job_config)
    return [dict(row) for row in job.result()]


def run_dml(sql: str) -> int:
    """Returns rows affected."""
    client = get_client()
    job = client.query(sql)
    job.result()
    return job.num_dml_affected_rows or 0


def table_ref(table: str) -> str:
    return f"`{settings.gcp_project_id}.{settings.gcp_dataset_id}.{table}`"


def _infer_type(v: object) -> str:
    if isinstance(v, bool):
        return "BOOL"
    if isinstance(v, int):
        return "INT64"
    if isinstance(v, float):
        return "FLOAT64"
    return "STRING"
