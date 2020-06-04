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


def get_pkl_from_hdfs(pkl_path):
    import pickle

    conn = get_hdfs_conn()
    byte_object = conn.cat(f"{pkl_path}")
    pkl_object = pickle.loads(byte_object)
    return pkl_object


def get_spark(scale=0, queue=None):
    import os
    from pyspark.sql import SparkSession
    import uuid

    tmp_uuid = str(uuid.uuid4())
    app_name = f"skt-{os.environ.get('USER', 'default')}-{tmp_uuid}"
    if not queue:
        if "JUPYTERHUB_USER" in os.environ:
            queue = "dmig_eda"
        else:
            queue = "airflow_job"
    os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
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


def hive_to_pandas(query):
    import uuid

    tmp_id = str(uuid.uuid4()).replace("-", "_")
    ctas = f"CREATE TABLE dumbo.{tmp_id} stored as parquet as {query}"
    conn = get_hive_conn()
    c = conn.cursor()
    c.execute("set parquet.column.index.access=false")
    c.execute(ctas)
    c.close()
    conn.close()
    table_path = f"/warehouse/tablespace/managed/hive/dumbo.db/{tmp_id}/delta_0000001_0000001_0000"
    return parquet_to_pandas(table_path)


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
