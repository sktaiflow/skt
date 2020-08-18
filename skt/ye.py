from skt.vault_utils import get_secrets


def get_hive_conn():
    from pyhive import hive

    hiveserver2 = get_secrets(path="ye/hiveserver2")
    host = hiveserver2["ip"]
    port = hiveserver2["port"]
    user = hiveserver2["user"]
    conn = hive.connect(host, port=port, username=user)
    return conn


def get_hdfs_conn():
    import os
    import pyarrow

    os.environ["ARROW_LIBHDFS_DIR"] = "/usr/hdp/3.0.1.0-187/usr/lib"
    conn = pyarrow.hdfs.connect(user="airflow")
    return conn


def get_sqlalchemy_engine():
    from sqlalchemy import create_engine

    hiveserver2 = get_secrets(path="ye/hiveserver2")
    host = hiveserver2["ip"]
    port = hiveserver2["port"]
    user = hiveserver2["user"]
    return create_engine(f"hive://{user}@{host}:{port}/tmp")


def get_pkl_from_hdfs(pkl_path):
    import pickle

    conn = get_hdfs_conn()
    byte_object = conn.cat(f"{pkl_path}")
    pkl_object = pickle.loads(byte_object)
    return pkl_object


def get_spark(scale=0, queue=None):
    import os
    import uuid
    import tempfile
    from pyspark.sql import SparkSession
    from pyspark.sql.readwriter import DataFrameWriter
    from skt.vault_utils import get_secrets
    from skt.data_lineage import publish_relation

    def add_hook(old_method):
        def new_method(self, path, **kwargs):
            query_execution = self._df._jdf.queryExecution().toString()
            source = "spark"
            destination = path
            context = {
                "query_execution": query_execution,
            }
            publish_relation(source, destination, context=context)
            return old_method(self, path, **kwargs)

        return new_method

    # Add hook for data relation
    if hasattr(DataFrameWriter, "hooked"):
        # Avoid recursive hook add
        pass
    else:
        DataFrameWriter.parquet = add_hook(DataFrameWriter.parquet)
        DataFrameWriter.text = add_hook(DataFrameWriter.text)
        DataFrameWriter.json = add_hook(DataFrameWriter.json)
        DataFrameWriter.csv = add_hook(DataFrameWriter.csv)
        DataFrameWriter.hooked = True

    tmp_uuid = str(uuid.uuid4())
    app_name = f"skt-{os.environ.get('USER', 'default')}-{tmp_uuid}"
    if not queue:
        if "JUPYTERHUB_USER" in os.environ:
            queue = "dmig_eda"
        else:
            queue = "airflow_job"
    os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    key_file_name = tempfile.mkstemp()[1]
    with open(key_file_name, "wb") as key_file:
        key_file.write(key.encode())
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file.name

    if scale in [1, 2, 3, 4]:
        spark = (
            SparkSession.builder.config("spark.app.name", app_name)
            .config("spark.driver.memory", f"{scale*8}g")
            .config("spark.executor.memory", f"{scale*3}g")
            .config("spark.executor.instances", f"{scale*8}")
            .config("spark.driver.maxResultSize", f"{scale*4}g")
            .config("spark.rpc.message.maxSize", "1024")
            .config("spark.yarn.queue", queue)
            .config("spark.ui.enabled", "false")
            .config("spark.port.maxRetries", "128")
            .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
            .config("spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
            .config("spark.jars", "gs://external_libs/spark/jars/spark-bigquery-with-dependencies_2.11-0.16.1.jar",)
            .enableHiveSupport()
            .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder.config("spark.app.name", app_name)
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "8g")
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.maxExecutors", "200")
            .config("spark.driver.maxResultSize", "6g")
            .config("spark.rpc.message.maxSize", "1024")
            .config("spark.yarn.queue", queue)
            .config("spark.ui.enabled", "false")
            .config("spark.port.maxRetries", "128")
            .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
            .config("spark.yarn.appMasterEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")
            .config("spark.jars", "gs://external_libs/spark/jars/spark-bigquery-with-dependencies_2.11-0.16.1.jar",)
            .enableHiveSupport()
            .getOrCreate()
        )
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    return spark


def hive_execute(query):
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute(query)
    c.close()
    conn.close()


def hive_get_result(query):
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute(query)
    result = c.fetchall()
    c.close()
    conn.close()
    return result


def hive_to_pandas(query, scale=0):
    if scale == 1:
        import pandas

        conn = get_hive_conn()
        df = pandas.read_sql(query, conn)
        df.info()
        conn.close()
        return df

    import uuid

    tmp_id = str(uuid.uuid4()).replace("-", "_")
    ctas = f"CREATE TABLE dumbo.{tmp_id} stored as parquet as {query}"
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute("set parquet.column.index.access=false")
    c.execute(ctas)
    hdfs = get_hdfs_conn()
    table_path = hdfs.ls(f"/warehouse/tablespace/managed/hive/dumbo.db/{tmp_id}")[0]
    hdfs.close()
    df = parquet_to_pandas(table_path)
    c.execute(f"DROP TABLE dumbo.{tmp_id}")
    c.close()
    conn.close()
    return df


def parquet_to_pandas(hdfs_path):
    from pyarrow import parquet

    hdfs = get_hdfs_conn()
    df = parquet.read_table(hdfs_path, filesystem=hdfs).to_pandas()
    df.info()
    return df


def pandas_to_parquet(pandas_df, hdfs_path, spark):
    df = spark.createDataFrame(pandas_df)
    df.write.mode("overwrite").parquet(hdfs_path)


def slack_send(
    text="This is default text",
    username="SKT",
    channel="#leavemealone",
    icon_emoji=":large_blue_circle:",
    blocks=None,
    dataframe=False,
):
    import requests
    from skt.vault_utils import get_secrets

    if dataframe:
        from tabulate import tabulate

        text = "```" + tabulate(text, tablefmt="simple", headers="keys") + "```"

    token = get_secrets("slack")["bot_token"]["airflow"]
    proxy = get_secrets("proxy")["proxy"]
    proxies = {
        "http": proxy,
        "https": proxy,
    }
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    json_body = {
        "username": username,
        "channel": channel,
        "text": text,
        "blocks": blocks,
        "icon_emoji": icon_emoji,
    }
    r = requests.post("https://www.slack.com/api/chat.postMessage", proxies=proxies, headers=headers, json=json_body,)
    r.raise_for_status()
    if not r.json()["ok"]:
        raise Exception(r.json())


def get_github_util():
    from skt.github_utils import GithubUtil

    github_token = get_secrets("github/sktaiflow")["token"]
    proxy = get_secrets("proxy")["proxy"]
    proxies = {
        "http": proxy,
        "https": proxy,
    }
    g = GithubUtil(github_token, proxies)
    return g


def _write_to_parquet_via_spark(pandas_df, hdfs_path):
    spark = get_spark()
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.write.mode("overwrite").parquet(hdfs_path)


def _write_to_parquet(pandas_df, hdfs_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Read Parquet INT64 timestamp issue:
    # https://issues.apache.org/jira/browse/HIVE-21215
    if "datetime64[ns]" in pandas_df.dtypes.tolist():
        _write_to_parquet_via_spark(pandas_df, hdfs_path)
        return

    pa_table = pa.Table.from_pandas(pandas_df)
    hdfs_conn = get_hdfs_conn()
    try:
        pq.write_to_dataset(pa_table, root_path=hdfs_path, filesystem=hdfs_conn)
    finally:
        hdfs_conn.close()


def _write_df(pandas_df, schema_name, table_name, hdfs_path, engine, cursor, tmp_table_name):
    import sqlalchemy.exc

    cursor.execute(f"drop table if exists {schema_name}.{tmp_table_name}")
    try:
        pandas_df.to_sql(tmp_table_name, engine, schema=schema_name, if_exists="replace", index=False)
    except sqlalchemy.exc.ProgrammingError:
        # Hive bulk insert issue:
        # https://github.com/dropbox/PyHive/issues/343
        pass

    cursor.execute(f"drop table if exists {schema_name}.{table_name}")
    if hdfs_path is None:
        cursor.execute(
            f"""create table {schema_name}.{table_name}
                           like {schema_name}.{tmp_table_name}
                           stored as parquet"""
        )
        cursor.execute(f"show create table {schema_name}.{table_name}")
        result = cursor.fetchall()
        managed_hdfs_path = list(filter(lambda row: row[0].strip().find("hdfs://") == 1, result))[0][0].strip()[1:-1]
        _write_to_parquet(pandas_df, managed_hdfs_path)
    else:
        cursor.execute(
            f"""create external table {schema_name}.{table_name}
                           like {schema_name}.{tmp_table_name}
                           stored as parquet
                           location '{hdfs_path}'"""
        )


def write_df_to_hive(pandas_df, schema_name, table_name, hdfs_path=None):
    """
    Exports a Panadas dataframe into a table in Hive.

    Example:
    write_df_to_hive(pandas_df1, "my_schema", "my_table1")
    write_df_to_hive(pandas_df2, "my_schema", "my_table2")
    write_df_to_hive(pandas_df1, "my_schema", "my_table3",
            hdfs_path="hdfs://.../my_schema.db/my_table1")

    Parameters
    ----------
    pandas_df : an ojbect of Pandas Dataframe
    schema_name : str
        A target schema name of Hive
    table_name : str
        A target table name of Hive
    hdfs_path : str, default None
        A path of Hadoop file system as an optional parameter.
        It will be used to create an external table. If hdfs_path
        is not None, data in the dataframe will not be converted.
        A metadata in the dataframe is just used to create a Hive
        table.
    """
    engine = get_sqlalchemy_engine()
    conn = get_hive_conn()
    cursor = conn.cursor()

    import hashlib

    tmp_table_name = hashlib.sha1(str(f"{schema_name}.{table_name}").encode("utf-8")).hexdigest()

    try:
        _write_df(pandas_df, schema_name, table_name, hdfs_path, engine, cursor, tmp_table_name)
    finally:
        cursor.execute(f"drop table if exists {schema_name}.{tmp_table_name}")
        cursor.close()
        conn.close()
