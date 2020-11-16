from google.cloud.bigquery import RangePartitioning, PartitionRange, TimePartitioning

from skt.ye import get_spark
from google.cloud.bigquery.job import QueryJobConfig, LoadJobConfig
from google.cloud.exceptions import NotFound

PROJECT_ID = "sktaic-datahub"
TEMP_DATASET = "temp_1d"


def get_credentials():
    import json
    from google.oauth2 import service_account
    from skt.vault_utils import get_secrets

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    json_acct_info = json.loads(key)
    credentials = service_account.Credentials.from_service_account_info(json_acct_info)
    scoped_credentials = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])

    return scoped_credentials


def get_bigquery_storage_client(credentials=None):
    from google.cloud import bigquery_storage_v1beta1

    if credentials is None:
        credentials = get_credentials()

    return bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)


def _get_result_schema(sql, bq_client=None):
    from google.cloud.bigquery.job import QueryJobConfig

    if bq_client is None:
        bq_client = get_bigquery_client()
    job_config = QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
    )
    query_job = bq_client.query(sql, job_config=job_config)
    schema = query_job._properties["statistics"]["query"]["schema"]
    return schema


def _get_result_column_type(sql, column, bq_client=None):
    schema = _get_result_schema(sql, bq_client)
    fields = schema["fields"]
    r = [field["type"] for field in fields if field["name"] == column]
    if r:
        return r[0]
    else:
        raise ValueError(f"Cannot find column {column} in {sql}")


def _print_query_job_results(query_job):
    try:
        t = query_job.destination
        dest_str = f"{t.project}.{t.dataset_id}.{t.table_id}" if t else "no destination"
        print(
            f"query: {query_job.query}\n"
            f"destination: {dest_str}\n"
            f"total_rows: {query_job.result().total_rows}\n"
            f"slot_secs: {query_job.slot_millis/1000}\n"
        )
    except Exception as e:
        print("Warning: exception on print statistics")
        print(e)


def bq_insert_overwrite_table(sql, destination):
    bq = get_bigquery_client()
    table = bq.get_table(destination)
    if table.time_partitioning or table.range_partitioning:
        load_query_result_to_partitions(sql, destination)
    else:
        config = QueryJobConfig(
            destination=destination,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_NEVER",
            clustering_fields=table.clustering_fields,
        )
        job = bq.query(sql, config)
        job.result()
        _print_query_job_results(job)
        bq.close()


def bq_ctas(sql, destination=None, partition_by=None, clustering_fields=None):
    """
    create new table and insert results
    """
    from google.cloud.bigquery.job import QueryJobConfig

    bq = get_bigquery_client()
    if partition_by:
        partition_type = _get_result_column_type(sql, partition_by, bq_client=bq)
        if partition_type == "DATE":
            qjc = QueryJobConfig(
                destination=destination,
                write_disposition="WRITE_EMPTY",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning=TimePartitioning(field=partition_by),
                clustering_fields=clustering_fields,
            )
        elif partition_type == "INTEGER":
            qjc = QueryJobConfig(
                destination=destination,
                write_disposition="WRITE_EMPTY",
                create_disposition="CREATE_IF_NEEDED",
                range_partitioning=RangePartitioning(
                    PartitionRange(start=200001, end=209912, interval=1), field=partition_by
                ),
                clustering_fields=clustering_fields,
            )
        else:
            raise Exception(f"Partition column[{partition_by}] is neither DATE or INTEGER type.")
    else:
        qjc = QueryJobConfig(
            destination=destination,
            write_disposition="WRITE_EMPTY",
            create_disposition="CREATE_IF_NEEDED",
            clustering_fields=clustering_fields,
        )

    job = bq.query(sql, qjc)
    job.result()
    _print_query_job_results(job)
    bq.close()

    return job.destination


def _bq_query_to_new_table(sql, destination=None):
    return bq_ctas(sql, destination)


def _bq_query_to_existing_table(sql, destination):
    from google.cloud.bigquery.job import QueryJobConfig

    bq = get_bigquery_client()
    table = bq.get_table(destination)
    config = QueryJobConfig(
        destination=destination,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        time_partitioning=table.time_partitioning,
        range_partitioning=table.range_partitioning,
        clustering_fields=table.clustering_fields,
    )
    job = bq.query(sql, config)
    job.result()
    bq.close()

    return job.destination


def _bq_table_to_pandas(table):
    credentials = get_credentials()
    bq = get_bigquery_client(credentials=credentials)
    bqstorage_client = get_bigquery_storage_client(credentials=credentials)
    row_iterator = bq.list_rows(table)
    df = row_iterator.to_dataframe(bqstorage_client=bqstorage_client, progress_bar_type="tqdm")
    bq.close()

    return df


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


def get_bigquery_client(credentials=None, project_id=PROJECT_ID):
    from google.cloud import bigquery

    if credentials is None:
        credentials = get_credentials()

    return bigquery.Client(credentials=credentials, project=project_id)


def bq_table_exists(table):
    try:
        get_bigquery_client().get_table(table)
    except NotFound:
        return False
    return True


def get_unhidden_partitions(table_name):
    parts = filter(
        lambda x: x not in ["__NULL__", "__UNPARTITIONED__"], get_bigquery_client().list_partitions(table_name)
    )
    return parts


def _get_partition_filter(dataset, table_name, partition):
    table = get_bigquery_client().get_table(f"{dataset}.{table_name}")
    if "timePartitioning" in table._properties:
        partition_column_name = table._properties["timePartitioning"]["field"]
        return f"{partition_column_name} = '{partition}'"
    elif "rangePartitioning" in table._properties:
        partition_column_name = table._properties["rangePartitioning"]["field"]
        return f"{partition_column_name} = {partition}"
    return ""


def bq_to_pandas(sql, large=False):
    destination = None
    if large:
        destination = get_temp_table()
    destination = _bq_query_to_new_table(sql, destination)
    return _bq_table_to_pandas(destination)


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


def _df_to_bq_table(
    df, dataset, table_name, partition=None, partition_field=None, clustering_fields=None, mode="overwrite"
):
    import base64
    from skt.vault_utils import get_secrets

    key = get_secrets("gcp/sktaic-datahub/dataflow")["config"]
    table = f"{dataset}.{table_name}${partition}" if partition else f"{dataset}.{table_name}"
    df = (
        df.write.format("bigquery")
        .option("project", "sktaic-datahub")
        .option("credentials", base64.b64encode(key.encode()).decode())
        .option("table", table)
        .option("temporaryGcsBucket", "temp-seoul-7d")
    )
    if partition_field:
        df = df.option("partitionField", partition_field)
    if clustering_fields:
        df = df.option("clusteredFields", ",".join(clustering_fields))
    df.save(mode=mode)


def df_to_bq_table(
    df, dataset, table_name, partition=None, partition_field=None, clustering_fields=None, mode="overwrite"
):
    _df_to_bq_table(df, dataset, table_name, partition, partition_field, clustering_fields, mode)


def parquet_to_bq_table(
    parquet_dir, dataset, table_name, partition=None, partition_field=None, clustering_fields=None, mode="overwrite"
):
    try:
        spark = get_spark()
        df = spark.read.format("parquet").load(parquet_dir)
        _df_to_bq_table(df, dataset, table_name, partition, partition_field, clustering_fields, mode)
    finally:
        spark.stop()


def pandas_to_bq_table(
    pd_df, dataset, table_name, partition=None, partition_field=None, clustering_fields=None, mode="overwrite"
):
    try:
        spark = get_spark()
        spark_df = spark.createDataFrame(pd_df)
        _df_to_bq_table(spark_df, dataset, table_name, partition, partition_field, clustering_fields, mode)
    finally:
        spark.stop()


def pandas_to_bq(pd_df, destination, partition=None, clustering_fields=None, overwrite=True):
    range_partitioning = None
    time_partitioning = None
    bq = get_bigquery_client()
    if bq_table_exists(destination):
        target_table = bq.get_table(destination)
        range_partitioning = target_table.range_partitioning
        time_partitioning = target_table.time_partitioning
    else:
        if partition:
            from pandas.api.types import is_integer_dtype
            import datetime

            if is_integer_dtype(pd_df[partition][0]):
                range_partitioning = RangePartitioning(
                    PartitionRange(start=200001, end=209912, interval=1), field=partition
                )
            elif isinstance(pd_df[partition][0], datetime.date):
                time_partitioning = TimePartitioning(field=partition)
            else:
                raise Exception("Partition type must be either date or range.")

    if time_partitioning or range_partitioning:
        if time_partitioning:
            input_partitions = [(p.strftime("%Y%m%d"), p) for p in set(pd_df[partition])]
        elif range_partitioning:
            input_partitions = [(p, p) for p in set(pd_df[partition])]
        for part, part_val in input_partitions:
            bq.load_table_from_dataframe(
                dataframe=pd_df[pd_df[partition] == part_val],
                destination=f"{destination}${part}",
                job_config=LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_TRUNCATE" if overwrite else "WRITE_APPEND",
                    time_partitioning=time_partitioning,
                    range_partitioning=range_partitioning,
                    clustering_fields=clustering_fields,
                ),
            ).result()
    else:
        bq.load_table_from_dataframe(
            dataframe=pd_df,
            destination=destination,
            job_config=LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE" if overwrite else "WRITE_APPEND",
            ),
        ).result()
    bq.close()


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
    temp_table_name = get_temp_table()
    jc = QueryJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        destination=temp_table_name,
    )
    bq_client = get_bigquery_client()
    job = bq_client.query(query, job_config=jc)
    job.result()
    t = temp_table_name.split(".")

    return _bq_table_to_df(t[1], t[2], "*", spark_session=spark_session)


def load_query_result_to_table(dest_table, query, part_col_name=None, clustering_fields=None):
    bq_client = get_bigquery_client()
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
        temp_table_name = get_temp_table()
        bq_client.query(f"CREATE OR REPLACE TABLE {temp_table_name} AS {query}").result()
        if part_col_name:
            schema = bq_client.get_table(temp_table_name).schema
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
        else:
            qjc = QueryJobConfig(
                destination=dest_table,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
            )
        bq_client.query(f"SELECT * FROM {temp_table_name}", job_config=qjc).result()


def get_temp_table():
    import uuid

    table_id = str(uuid.uuid4()).replace("-", "_")
    full_table_id = f"{PROJECT_ID}.{TEMP_DATASET}.{table_id}"

    return full_table_id


def _get_partition_name(table):
    if table.range_partitioning:
        return table.range_partitioning.field
    elif table.time_partitioning:
        return table.time_partitioning.field
    else:
        raise Exception(f"Table[{table}] is not partitioned.")


def load_query_result_to_partitions(query, dest_table):
    from google.cloud.bigquery.table import TableReference
    from google.cloud.bigquery.dataset import DatasetReference

    bq = get_bigquery_client()
    table = bq.get_table(dest_table)

    """
    Destination 이 파티션일 때는 임시테이블 만들지 않고 직접 저장
    """
    if "$" in dest_table:
        qjc = QueryJobConfig(
            destination=table,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=table.time_partitioning,
            range_partitioning=table.range_partitioning,
            clustering_fields=table.clustering_fields,
        )
        job = bq.query(query, job_config=qjc)
        job.result()
        _print_query_job_results(job)
        return dest_table

    temp_table_id = get_temp_table()
    qjc = QueryJobConfig(
        destination=temp_table_id,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning=table.time_partitioning,
        range_partitioning=table.range_partitioning,
        clustering_fields=table.clustering_fields,
    )
    bq.query(query, job_config=qjc).result()
    partitions = bq.list_partitions(temp_table_id)
    for p in partitions:
        part_name = _get_partition_name(table)
        if p == "__NULL__":
            if table.range_partitioning:
                columns = ["NULL" if f.name.lower() == part_name.lower() else f.name for f in table.schema]
            elif table.time_partitioning:
                columns = ["DATE(NULL)" if f.name.lower() == part_name.lower() else f.name for f in table.schema]
            job = bq.query(
                f"""
                DELETE FROM `{dest_table}` WHERE {part_name} IS NULL
                ;
                INSERT INTO `{dest_table}`
                SELECT {', '.join(columns)}
                FROM   `{temp_table_id}`
                WHERE  {part_name} IS NULL
            """
            )
            job.result()
            _print_query_job_results(job)
        else:
            project_id, dataset_id, table_id = dest_table.split(".")
            ref = TableReference(DatasetReference(project_id, dataset_id), f"{table_id}${p}")
            qjc = QueryJobConfig(
                destination=ref,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning=table.time_partitioning,
                range_partitioning=table.range_partitioning,
                clustering_fields=table.clustering_fields,
            )
            if table.range_partitioning:
                query = f"select * from {temp_table_id} where {part_name}={p}"
            elif table.time_partitioning:
                partition = f"{p[:4]}-{p[4:6]}-{p[6:8]}"
                query = f"select * from {temp_table_id} where {part_name}='{partition}'"
            job = bq.query(query, job_config=qjc)
            job.result()
            _print_query_job_results(job)

    return partitions


def get_max_part(table_name):
    from datetime import datetime

    bq_client = get_bigquery_client()
    parts = get_unhidden_partitions(table_name)

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


def bq_insert_overwrite(sql, destination, suffixes=None, partition=None, clustering_fields=None):
    if suffixes:
        bq_insert_overwrite_with_suffixes(sql, destination, suffixes, partition, clustering_fields)
    else:
        bq_insert_overwrite_without_suffixes(sql, destination, partition, clustering_fields)


def bq_insert_overwrite_with_suffixes(sql, destination, suffixes, partition=None, clustering_fields=None):
    temp_table = get_temp_table()
    bq_ctas(sql, temp_table, partition_by=partition, clustering_fields=clustering_fields)
    bq = get_bigquery_client()
    r = bq.query(f"""select distinct {', '.join(suffixes)} from {temp_table}""")
    for cols in r:
        suffix = "".join([f"__{col}" for col in cols])
        select_clause = f"select * except({', '.join(suffixes)}) from {temp_table}"
        where_clause = " and ".join([f"{x[0]} = '{x[1]}'" for x in zip(suffixes, cols)])
        target = "".join([destination, suffix])
        sub_sql = f"{select_clause} where {where_clause}"
        bq_insert_overwrite_without_suffixes(sub_sql, target, partition=partition, clustering_fields=clustering_fields)
    bq.close()


def bq_insert_overwrite_without_suffixes(sql, destination, partition=None, clustering_fields=None):
    if bq_table_exists(destination):
        bq_insert_overwrite_table(sql, destination)
    else:
        bq_ctas(sql, destination, partition_by=partition, clustering_fields=clustering_fields)
