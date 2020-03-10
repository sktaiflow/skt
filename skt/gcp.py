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


def bq_table_to_pandas(dataset, table_name, col_list, partition=None, where=None):
    import os
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/etc/hadoop/conf/google-access-key.json'
    spark = get_spark_for_bigquery()
    df = spark.read.format("bigquery") \
            .option("project", "sktaic-datahub") \
            .option("table", f"sktaic-datahub:{dataset}.{table_name}") \
            .option("credentialsFile", '/etc/hadoop/conf/google-access-key.json')
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
    df = df.load().select(col_list)
    if where:
        df.where(where)
    pd_df = df.toPandas()
    spark.stop()
    return pd_df


def pandas_to_bq_table(df, dataset, table_name, partition=None):
    import os
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/etc/hadoop/conf/google-access-key.json'
    spark = get_spark_for_bigquery()
    spark_df = spark.createDataFrame(df)
    table = f"{dataset}.{table_name}${partition}" if partition else f"{dataset}.{table_name}"
    spark_df.write.format("bigquery") \
        .option("project", "sktaic-datahub") \
        .option("credentialsFile", "/etc/hadoop/conf/google-access-key.json") \
        .option("table", table) \
        .option("temporaryGcsBucket", "mnoai-us") \
        .save(mode='overwrite')
    spark.stop()
