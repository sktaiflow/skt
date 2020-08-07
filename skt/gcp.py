from google.cloud.bigquery import RangePartitioning, PartitionRange, TimePartitioning

from skt.ye import get_spark
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.exceptions import NotFound


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


# deprecated
def import_bigquery_ipython_magic():
    load_bigquery_ipython_magic()


def load_bigquery_ipython_magic():
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


def bq_table_exists(table):
    try:
        get_bigquery_client().get_table(table)
    except NotFound:
        return False
    return True


def _get_partition_filter(dataset, table_name, partition):
    table = get_bigquery_client().get_table(f"{dataset}.{table_name}")
    if "timePartitioning" in table._properties:
        partition_column_name = table._properties["timePartitioning"]["field"]
        return f"{partition_column_name} = '{partition}'"
    elif "rangePartitioning" in table._properties:
        partition_column_name = table._properties["rangePartitioning"]["field"]
        return f"{partition_column_name} = {partition}"
    return ""


def bq_to_pandas(query, project_id="sktaic-datahub"):
    import pandas as pd

    set_gcp_credentials()
    configuration = {"query": {"useQueryCache": True}}
    return pd.read_gbq(
        query=query, project_id=project_id, dialect="standard", use_bqstorage_api=True, configuration=configuration
    )


def _bq_table_to_df(dataset, table_name, col_list, partition=None, where=None, spark_session=None):
    import base64
    from skt.vault_utils import get_secrets

    if not spark_session:
        spark_session = get_spark()
    spark_session.conf.set("spark.sql.execution.arrow.enabled", "false")
    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    df = (
        spark_session.read.format("bigquery")
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


def bq_table_to_df(dataset, table_name, col_list, partition=None, where=None, spark_session=None):
    return _bq_table_to_df(dataset, table_name, col_list, partition, where, spark_session)


def bq_table_to_parquet(dataset, table_name, output_dir, col_list="*", partition=None, where=None, mode="overwrite"):
    bq_table_to_df(dataset, table_name, col_list, partition=partition, where=where).write.mode(mode).parquet(output_dir)


def bq_table_to_pandas(dataset, table_name, col_list="*", partition=None, where=None):
    col_list_str = ", ".join(col_list) if isinstance(col_list, list) else col_list
    query = f"SELECT {col_list_str} FROM {dataset}.{table_name} "
    if partition or where:
        query += "WHERE "
        conditions = []
        if partition:
            conditions.append(f" {_get_partition_filter(dataset, table_name, partition)} ")
        if where:
            conditions.append(f" {where} ")
        query += " AND ".join(conditions)
    return bq_to_pandas(query=query)


def _df_to_bq_table(df, dataset, table_name, partition=None, mode="overwrite"):
    import base64
    from skt.vault_utils import get_secrets

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    table = f"{dataset}.{table_name}${partition}" if partition else f"{dataset}.{table_name}"
    df.write.format("bigquery").option("project", "sktaic-datahub").option(
        "credentials", base64.b64encode(key.encode()).decode()
    ).option("table", table).option("temporaryGcsBucket", "temp-seoul-7d").save(mode=mode)


def df_to_bq_table(df, dataset, table_name, partition=None, mode="overwrite"):
    _df_to_bq_table(df, dataset, table_name, partition, mode)


def parquet_to_bq_table(parquet_dir, dataset, table_name, partition=None, mode="overwrite"):
    try:
        spark = get_spark()
        df = spark.read.format("parquet").load(parquet_dir)
        _df_to_bq_table(df, dataset, table_name, partition, mode)
    finally:
        spark.stop()


def pandas_to_bq_table(pd_df, dataset, table_name, partition=None, mode="overwrite"):
    try:
        spark = get_spark()
        spark_df = spark.createDataFrame(pd_df)
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


def bq_to_df(query, spark_session=None):
    import time

    temp_table_name = f"bq_to_df__{str(int(time.time()))}"
    temp_dataset = "temp_1d"
    jc = QueryJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        destination=f"sktaic-datahub.{temp_dataset}.{temp_table_name}",
    )
    bq_client = get_bigquery_client()
    job = bq_client.query(query, job_config=jc)
    job.result()

    return _bq_table_to_df(temp_dataset, temp_table_name, "*", spark_session=spark_session)


def load_query_result_to_table(dest_table, query, part_col_name=None, clustering_fields=None):
    bq_client = get_bigquery_client()
    qjc = None
    print(query)
    if bq_table_exists(dest_table):
        table = bq_client.get_table(dest_table)
        qjc = QueryJobConfig(
            destination=dest_table,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=table.time_partitioning,
            range_partitioning=table.range_partitioning,
            clustering_fields=table.clustering_fields,
        )
        job = bq_client.query(query, job_config=qjc)
        job.result()

    else:
        import time

        temp_table_name = f"load_query_result_to_table__{str(int(time.time()))}"
        bq_client.query(f"CREATE OR REPLACE TABLE temp_1d.{temp_table_name} AS {query}").result()
        if part_col_name:
            schema = bq_client.get_table(f"temp_1d.{temp_table_name}").schema
            partition_type = [f for f in schema if f.name.lower() == part_col_name.lower()][0].field_type
            if partition_type == "DATE":
                qjc = QueryJobConfig(
                    destination=dest_table,
                    write_disposition="WRITE_TRUNCATE",
                    create_disposition="CREATE_IF_NEEDED",
                    time_partitioning=TimePartitioning(field=part_col_name),
                    clustering_fields=clustering_fields,
                )
            elif partition_type == "INTEGER":
                qjc = QueryJobConfig(
                    destination=dest_table,
                    write_disposition="WRITE_TRUNCATE",
                    create_disposition="CREATE_IF_NEEDED",
                    range_partitioning=RangePartitioning(
                        PartitionRange(start=200001, end=209912, interval=1), field=part_col_name
                    ),
                    clustering_fields=clustering_fields,
                )
            else:
                print(partition_type)
                raise Exception(f"Partition column[{part_col_name}] is neither DATE or INTEGER type.")
        job = bq_client.query(f"SELECT * FROM temp_1d.{temp_table_name}", job_config=qjc)
        job.result()


def get_max_part(table_name):
    from datetime import datetime

    bq_client = get_bigquery_client()
    parts = filter(lambda x: x != "__NULL__", get_bigquery_client().list_partitions(table_name))

    if not parts:
        raise Exception("Max partition value is invalid or null.")

    max_part_value = max(parts)

    table = bq_client.get_table(table_name)
    if table.time_partitioning:
        return datetime.strptime(max_part_value, "%Y%m%d").strftime("%Y-%m-%d")
    elif table.range_partitioning:
        return int(max_part_value)
    else:
        raise Exception("Partition column is neither DATE or INTEGER type.")
