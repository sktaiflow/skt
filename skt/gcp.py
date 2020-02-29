def get_bigquery_client():
    import os
    import tempfile
    from google.cloud import bigquery
    from skt.vault_utils import get_secrets
    key = get_secrets('gcp/sktaic-datahub/dataflow')['config']
    with tempfile.NamedTemporaryFile() as f:
        f.write(key.encode())
        f.seek(0)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name
        client = bigquery.Client()
    return client


def bq_to_pandas(query):
    bq = get_bigquery_client()
    query_job = bq.query(query)
    return query_job.to_dataframe()
