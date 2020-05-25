def _bq_cell_magic(line, query):
    from IPython.core import magic_arguments
    from google.cloud.bigquery.magics import _cell_magic
    import time

    start = time.time()
    args = magic_arguments.parse_argstring(_cell_magic, line)

    if args.params is not None:
        try:
            from google.cloud.bigquery.dbapi import _helpers
            import ast

            params = _helpers.to_query_parameters(ast.literal_eval("".join(args.params)))
            query_params = dict()
            for p in params:
                query_params[p.name] = p.value
            query = query.format(**query_params)
        except Exception:
            raise SyntaxError("--params is not a correctly formatted JSON string or a JSON " "serializable dictionary")
    result = _cell_magic(line, query)
    print(f"BigQuery execution took {int(time.time() - start)} seconds.")
    return result


def _load_bq_ipython_extension(ipython):
    ipython.register_magic_function(_bq_cell_magic, magic_kind="cell", magic_name="bq")


def _is_ipython():
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def set_gcp_credentials():
    import os
    import tempfile
    from skt.vault_utils import get_secrets

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    key_file_name = tempfile.mkstemp()[1]
    with open(key_file_name, "wb") as key_file:
        key_file.write(key.encode())
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file.name


def import_bigquery_ipython_magic():
    if _is_ipython():
        from IPython import get_ipython

        set_gcp_credentials()
        _load_bq_ipython_extension(get_ipython())
    else:
        raise Exception("Cannot import bigquery magic. Because execution is not on ipython.")


def get_bigquery_client():
    import os
    import tempfile
    from google.cloud import bigquery
    from skt.vault_utils import get_secrets

    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ and os.path.isfile(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
        return bigquery.Client()
    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    with tempfile.NamedTemporaryFile() as f:
        f.write(key.encode())
        f.seek(0)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
        client = bigquery.Client()
    return client


def bq_to_pandas(query):
    bq = get_bigquery_client()
    query_job = bq.query(query)
    return query_job.to_dataframe()


def get_spark_for_bigquery():
    set_gcp_credentials()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.config("spark.driver.memory", "32g")
        .config("spark.executor.memory", "16g")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", "200")
        .config("spark.driver.maxResultSize", "16g")
        .config("spark.rpc.message.maxSize", "2000")
        .config("spark.executor.memoryOverhead", "2000")
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.jars", "gs://external_libs/spark/jars/spark-bigquery-with-dependencies_2.11-0.13.1-beta.jar",)
        .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
        .config("spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
        .config("spark.yarn.queue", "airflow_job")
        .getOrCreate()
    )
    return spark


def gcp_credentials_decorator_for_spark_bigquery(func):
    def decorated(*args, **kwargs):
        import os.path
        import tempfile
        from skt.vault_utils import get_secrets

        is_key_temp = False
        try:
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                key_file_name = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
                if not os.path.isfile(key_file_name):
                    with open(key_file_name, "wb") as key_file:
                        key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
                        key_file.write(key.encode())
                        key_file.seek(0)
            else:
                key_file = tempfile.NamedTemporaryFile(delete=False)
                key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
                key_file.write(key.encode())
                key_file.seek(0)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file.name
                is_key_temp = True
            result = func(*args, **kwargs)
        finally:
            if os.path.isfile(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) and is_key_temp:
                os.remove(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        return result

    return decorated


def _bq_table_to_df(dataset, table_name, col_list, partition=None, where=None):
    import base64
    from skt.vault_utils import get_secrets

    spark = get_spark_for_bigquery()
    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    df = (
        spark.read.format("bigquery")
        .option("project", "sktaic-datahub")
        .option("table", f"sktaic-datahub:{dataset}.{table_name}")
        .option("credentials", base64.b64encode(key.encode()).decode())
    )
    if partition:
        table = get_bigquery_client().get_table(f"{dataset}.{table_name}")
        if "timePartitioning" in table._properties:
            partition_column_name = table._properties["timePartitioning"]["field"]
            filter = f"{partition_column_name} = '{partition}'"
        elif "rangePartitioning" in table._properties:
            partition_column_name = table._properties["rangePartitioning"]["field"]
            filter = f"{partition_column_name} = {partition}"
        else:
            partition_column_name = None
        if partition_column_name:
            df = df.option("filter", filter)
    df = df.load().select(col_list)
    if where:
        df.where(where)
    return df


@gcp_credentials_decorator_for_spark_bigquery
def bq_table_to_df(dataset, table_name, col_list, partition=None, where=None):
    return _bq_table_to_df(dataset, table_name, col_list, partition, where)


@gcp_credentials_decorator_for_spark_bigquery
def bq_table_to_pandas(dataset, table_name, col_list, partition=None, where=None):
    try:
        df = _bq_table_to_df(dataset, table_name, col_list, partition, where)
        pd_df = df.toPandas()
    finally:
        spark = get_spark_for_bigquery()
        spark.stop()
    return pd_df


def _df_to_bq_table(df, dataset, table_name, partition=None, mode="overwrite"):
    import base64
    from skt.vault_utils import get_secrets

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    table = f"{dataset}.{table_name}${partition}" if partition else f"{dataset}.{table_name}"
    df.write.format("bigquery").option("project", "sktaic-datahub").option(
        "credentials", base64.b64encode(key.encode()).decode()
    ).option("table", table).option("temporaryGcsBucket", "mnoai-us").save(mode=mode)


@gcp_credentials_decorator_for_spark_bigquery
def df_to_bq_table(df, dataset, table_name, partition=None, mode="overwrite"):
    _df_to_bq_table(df, dataset, table_name, partition, mode)


@gcp_credentials_decorator_for_spark_bigquery
def pandas_to_bq_table(df, dataset, table_name, partition=None, mode="overwrite"):
    try:
        spark = get_spark_for_bigquery()
        spark_df = spark.createDataFrame(df)
        _df_to_bq_table(spark_df, dataset, table_name, partition, mode)
    finally:
        spark.stop()


# decorator for rdd to pandas in mapPartitions in Spark
def rdd_to_pandas(func):
    def _rdd_to_pandas(rdd_):
        import pandas as pd
        from pyspark.sql import Row

        rows = (row_.asDict() for row_ in rdd_)
        pdf = pd.DataFrame(rows)
        result_pdf = func(pdf)
        return [Row(**d) for d in result_pdf.to_dict(orient="records")]

    return _rdd_to_pandas
