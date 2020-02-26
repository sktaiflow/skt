from skt.vault_utils import get_secrets


def get_hive_conn():
    from pyhive import hive
    hiveserver2 = get_secrets(path='ye/hiveserver2')
    host = hiveserver2['ip']
    port = hiveserver2['port']
    user = hiveserver2['user']
    conn = hive.connect(host, port=port, username=user)
    return conn


def hive_execute(query):
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute(query, async_=False)
    logs = c.fetch_logs()
    for message in logs:
        print(message)
    c.close()
    conn.close()


def hive_get_result(query):
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute(query, async_=False)
    result = c.fetchall()
    logs = c.fetch_logs()
    for message in logs:
        print(message)
    c.close()
    conn.close()
    return result


def hive_to_pandas(query):
    import uuid
    tmp_id = str(uuid.uuid4()).replace('-', '_')
    ctas = f'CREATE TABLE dumbo.{tmp_id} stored as parquet as {query}'
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute('set parquet.column.index.access=false', async_=False)
    c.execute(ctas, async_=False)
    logs = c.fetch_logs()
    for message in logs:
        print(message)
    c.close()
    conn.close()
