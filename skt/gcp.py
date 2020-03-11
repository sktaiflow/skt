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


def get_spark_for_bigquery():
    from pyspark.sql import SparkSession
    spark = SparkSession \
        .builder \
        .config('spark.driver.memory', '32g') \
        .config('spark.executor.memory', '8g') \
        .config('spark.shuffle.service.enabled', 'true') \
        .config('spark.dynamicAllocation.enabled', 'true') \
        .config('spark.dynamicAllocation.maxExecutors', '200') \
        .config('spark.driver.maxResultSize', '16g') \
        .config('spark.rpc.message.maxSize', '2000') \
        .config('spark.executor.memoryOverhead', '2000') \
        .config('spark.sql.execution.arrow.enabled', 'true') \
        .config("spark.jars",
                "gs://external_libs/spark/jars/spark-bigquery-with-dependencies_2.11-0.13.1-beta.jar") \
        .config('spark.yarn.queue', 'airflow_job') \
        .getOrCreate()
    return spark


def gcp_credentials_decorator_for_spark_bigquery(func):
    def decorated(*args, **kwargs):
        import os.path
        import tempfile
        from skt.vault_utils import get_secrets
        try:
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                key_file_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
                if not os.path.isfile(key_file_name):
                    with open(key_file_name, 'wb') as key_file:
                        key = get_secrets('gcp/sktaic-datahub/dataflow')['config']
                        key_file.write(key.encode())
                        key_file.seek(0)
            else:
                key_file = tempfile.NamedTemporaryFile(delete=False)
                key = get_secrets('gcp/sktaic-datahub/dataflow')['config']
                key_file.write(key.encode())
                key_file.seek(0)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file.name
            result = func(*args, **kwargs)
        finally:
            if os.path.isfile(os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
                os.remove(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        return result
    return decorated


@gcp_credentials_decorator_for_spark_bigquery
def bq_table_to_pandas(dataset, table_name, col_list, partition=None, where=None):
    import base64
    from skt.vault_utils import get_secrets
    try:
        spark = get_spark_for_bigquery()
        key = get_secrets('gcp/sktaic-datahub/dataflow')['config']
        df = spark.read.format("bigquery") \
                .option("project", "sktaic-datahub") \
                .option("table", f"sktaic-datahub:{dataset}.{table_name}") \
                .option("credentials", base64.b64encode(key.encode()).decode())
        if partition:
            table = get_bigquery_client().get_table(f'{dataset}.{table_name}')
            if 'timePartitioning' in table._properties:
                partition_column_name = table._properties['timePartitioning']['field']
                filter = f"{partition_column_name} = '{partition}'"
            elif 'rangePartitioning' in table._properties:
                partition_column_name = table._properties['rangePartitioning']['field']
                filter = f"{partition_column_name} = {partition}"
            else:
                partition_column_name = None
            if partition_column_name:
                df = df.option("filter", filter)
        df = df.option("filter", filter)
        df = df.load().select(col_list)
        if where:
            df.where(where)
        pd_df = df.toPandas()
    finally:
        spark.stop()
    return pd_df


@gcp_credentials_decorator_for_spark_bigquery
def pandas_to_bq_table(df, dataset, table_name, partition=None):
    import base64
    from skt.vault_utils import get_secrets
    try:
        spark = get_spark_for_bigquery()
        key = get_secrets('gcp/sktaic-datahub/dataflow')['config']
        spark_df = spark.createDataFrame(df)
        table = f"{dataset}.{table_name}${partition}" if partition else f"{dataset}.{table_name}"
        spark_df.write.format("bigquery") \
            .option("project", "sktaic-datahub") \
            .option("credentials", base64.b64encode(key.encode()).decode()) \
            .option("table", table) \
            .option("temporaryGcsBucket", "mnoai-us") \
            .save(mode='overwrite')
    finally:
        spark.stop()
