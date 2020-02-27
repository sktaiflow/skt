from skt.vault_utils import get_secrets


def get_hive_conn():
    from pyhive import hive
    hiveserver2 = get_secrets(path='ye/hiveserver2')
    host = hiveserver2['ip']
    port = hiveserver2['port']
    user = hiveserver2['user']
    conn = hive.connect(host, port=port, username=user)
    return conn


def get_hdfs_conn():
    import os
    import pyarrow
    os.environ['ARROW_LIBHDFS_DIR'] = '/usr/hdp/3.0.1.0-187/usr/lib'
    conn = pyarrow.hdfs.connect(user='airflow')
    return conn


def get_spark(scale=0):
    import os
    from pyspark.sql import SparkSession
    os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = '1'
    if scale in [1, 2, 3, 4]:
        spark = SparkSession \
            .builder \
            .config('spark.driver.memory', f'{scale*8}g') \
            .config('spark.executor.memory', f'{scale*2}g') \
            .config('spark.executor.number', f'{scale*8}g') \
            .config('spark.driver.maxResultSize', f'{scale*4}g') \
            .config('spark.rpc.message.maxSize', '1024') \
            .config('spark.yarn.queue', 'airflow_job') \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        spark = SparkSession \
            .builder \
            .config('spark.driver.memory', '4g') \
            .config('spark.executor.memory', '4g') \
            .config('spark.shuffle.service.enabled', 'true') \
            .config('spark.dynamicAllocation.enabled', 'true') \
            .config('spark.dynamicAllocation.maxExecutors', '200') \
            .config('spark.yarn.queue', 'airflow_job') \
            .enableHiveSupport() \
            .getOrCreate()
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    return spark


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
    table_path = f'/warehouse/tablespace/managed/hive/dumbo.db/{tmp_id}/delta_0000001_0000001_0000'
    return parquet_to_pandas(table_path)


def parquet_to_pandas(hdfs_path):
    from pyarrow import parquet
    hdfs = get_hdfs_conn()
    df = parquet.read_table(hdfs_path, filesystem=hdfs).to_pandas()
    df.info()
    return df


def pandas_to_parquet(pandas_df, hdfs_path, spark):
    df = spark.createDataFrame(pandas_df)
    df.write.mode('overwrite').parquet(hdfs_path)
