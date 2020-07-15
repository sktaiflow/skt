def skt_hash(ss, df, before, after, key_column=None):
    spark = ss
    if key_column is None:
        key = "svc_mgmt_num"
    else:
        key = key_column

    from skt.vault_utils import get_secrets
    import requests

    def get_access_token():
        secrets = get_secrets("lake/hash")
        url = secrets["auth_url"]
        client_id = secrets["client_id"]
        client_secret = secrets["client_secret"]
        data = {"grant_type": "client_credentials"}
        res = requests.post(url, auth=(client_id, client_secret), data=data)
        return res.json()["access_token"]

    access_token = get_access_token()
    hash_url = get_secrets("lake/hash")["hash_url"]
    unhash_url = get_secrets("lake/hash")["unhash_url"]

    from pyspark.sql.functions import pandas_udf, PandasUDFType, sha2
    from pyspark.sql.types import StructType, StructField, StringType
    import requests

    schema = StructType(
        [
            StructField("svc_mgmt_num", StringType(), True),
            StructField("lake_hash", StringType(), True),
            StructField("seed", StringType(), True),
        ]
    )

    unhash_schema = StructType(
        [
            StructField("svc_mgmt_num", StringType(), True),
            StructField("lake_unhash", StringType(), True),
            StructField("seed", StringType(), True),
        ]
    )

    def generate_pda_unhashing(unhash_url, access_token):
        @pandas_udf(unhash_schema, PandasUDFType.GROUPED_MAP)
        def pds_unhashing(pdf):
            import pandas as pd

            global unhash_url
            global access_token
            url = unhash_url
            header = {"Authorization": f"Bearer {access_token}"}

            size = 5000
            list_of_pdf = [pdf.loc[i : i + size - 1, :] for i in range(0, len(pdf), size)]

            result_list = []
            aggre_list = []
            for value in list_of_pdf:
                data = {"type": "s", "hashedValues": list(value.loc[:, "lake_unhash"])}
                r = requests.post(url, headers=header, json=data)
                if r.status_code != 200:
                    raise Exception(r.content.decode("utf8"))

                value.loc[:, "lake_unhash"] = [x["unhashedValue"] for x in r.json()["response"]]
                aggre_list.append(value)

            reslut_list = pd.concat(aggre_list)

            return reslut_list

        return pds_unhashing

    udf_pda_unhashing = generate_pda_unhashing(unhash_url, access_token)

    def generate_pda_hashing(hash_url, access_token):
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def pds_hashing(pdf):
            import pandas as pd

            url = hash_url
            header = {"Authorization": f"Bearer {access_token}"}

            size = 5000
            list_of_pdf = [pdf.loc[i : i + size - 1, :] for i in range(0, len(pdf), size)]

            result_list = []
            aggre_list = []
            for value in list_of_pdf:
                data = {"type": "s", "unhashedValues": list(value.loc[:, "lake_hash"])}
                r = requests.post(url, headers=header, json=data)
                if r.status_code != 200:
                    raise Exception(r.content.decode("utf8"))

                value.loc[:, "lake_hash"] = [x["hashedValue"] for x in r.json()["response"]]
                aggre_list.append(value)

            reslut_list = pd.concat(aggre_list)

            return reslut_list

        return pds_hashing

    udf_pda_hashing = generate_pda_hashing(hash_url, access_token)

    def get_file_list(path):
        import subprocess

        cmd = f"hdfs dfs -ls {path}".split()
        lines = subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode("utf8").strip().split("\n")
        files = [line.split()[-1] for line in lines if line.split()]
        return files

    def get_latest_dt(parent_path):
        prefix = f"{parent_path}dt="
        files = get_file_list(parent_path)
        dts = [path.split("dt=")[-1] for path in files if path.startswith(prefix)]
        return sorted(dts, reverse=True)[0]

    def mapping_table_load(ss):
        load_path = "/data/saturn/svc_mgmt_num_mapping/"
        latest_dt = get_latest_dt(load_path)
        return ss.read.parquet(f"{load_path}dt={latest_dt}")

    def select_hash_schema(df, key):
        select_schema = ""
        for col in df.columns:
            if col == f"lake_hashed_{key}":
                select_schema += (
                    f"case when a.lake_hashed_{key} is null then b.lake_hash else a.lake_hashed_{key} end as {key}"
                )
            elif col == key:
                pass
            else:
                select_schema += "a." + col + ","

        return select_schema

    def select_unhash_schema(df, key):
        select_schema = ""
        for col in df.columns:
            if col == f"raw_{key}":
                select_schema += f"case when a.raw_{key} is null then b.lake_unhash else a.raw_{key} end as {key}"
            elif col == key:
                pass
            else:
                select_schema += "a." + col + ","

        return select_schema

    def fill_for_mapping_table(ss, df):
        spark = ss
        mapping_path = "/data/saturn/svc_mgmt_num_mapping/"
        output_path = "/data/saturn/svc_mgmt_num_mapping_fill/"
        latest_dt = get_latest_dt(mapping_path)

        df.registerTempTable("fill_table")

        spark.sql(
            """
            select
                svc_mgmt_num as raw,
                sha2(svc_mgmt_num, 256) as ye_hashed,
                lake_hash as lake_hashed
            from
                fill_table
        """
        ).write.format("parquet").mode("append").save(f"{output_path}dt={latest_dt}")

    def lake_hashed_to_raw(ss, source_df, key):
        spark = ss

        source_df.registerTempTable("source_table")

        mapping_df = mapping_table_load(ss=spark)
        mapping_df.registerTempTable("mapping_table")

        joined_df = spark.sql(
            f"""
            select
                a.*,
                b.raw as raw_{key}
            from
                source_table a left outer join mapping_table b
                on a.{key} = b.lake_hashed
            """
        )
        joined_df.registerTempTable("joined_table")

        not_mapping = spark.sql(
            f"""
            select
                distinct
                {key} as svc_mgmt_num,
                {key} as lake_unhash,
                substr({key}, 9,3) as seed
            from
                joined_table
            where
                raw_{key} is null
                and {key} is not null
            """
        )

        if not_mapping.count() > 0:
            unhashed_df = not_mapping.groupby("seed").apply(udf_pda_unhashing)
            unhashed_df.registerTempTable("fill_table")

            schema = select_unhash_schema(joined_df, key)

            result_df = spark.sql(
                f"""
                select
                    {schema}
                from
                    joined_table a left outer join fill_table b
                    on a.{key} = b.svc_mgmt_num
            """
            )
        else:
            result_df = joined_df.drop(key).withColumnRenamed(f"raw_{key}", key)

        spark.catalog.dropTempView("source_table")
        spark.catalog.dropTempView("mapping_table")
        spark.catalog.dropTempView("joined_table")

        return result_df.drop("seed")

    def raw_to_lake_hash(ss, source_df, key):
        spark = ss

        source_df.registerTempTable("source_table")

        mapping_df = mapping_table_load(ss=spark)
        mapping_df.registerTempTable("mapping_table")

        joined_df = spark.sql(
            f"""
            select
                a.*,
                b.lake_hashed as lake_hashed_{key}
            from
                source_table a left outer join mapping_table b
                on a.{key} = b.raw
        """
        )
        joined_df.registerTempTable("joined_table")

        not_mapping = spark.sql(
            f"""
            select
                distinct
                {key} as svc_mgmt_num,
                {key} as lake_hash,
                substr({key}, 9,3) as seed
            from
                joined_table
            where
                lake_hashed_{key} is null
                and {key} is not null
        """
        )

        if not_mapping.count() > 0:
            hashed_df = not_mapping.groupby("seed").apply(udf_pda_hashing)
            hashed_df.registerTempTable("fill_table")

            schema = select_hash_schema(joined_df, key)
            result_df = spark.sql(
                f"""
                select
                    {schema}
                from
                    joined_table a left outer join fill_table b
                    on a.{key} = b.svc_mgmt_num
            """
            )

            # append mapping table
            if key == "svc_mgmt_num":
                fill_for_mapping_table(ss=spark, df=hashed_df)

        else:
            result_df = joined_df.drop(key).withColumnRenamed(f"lake_hashed_{key}", key)

        spark.catalog.dropTempView("source_table")
        spark.catalog.dropTempView("mapping_table")
        spark.catalog.dropTempView("joined_table")

        return result_df.drop("seed")

    def sha256_to_raw(ss, source_df, key):
        spark = ss

        source_df.registerTempTable("source_table")

        mapping_df = mapping_table_load(ss=spark)
        mapping_df.registerTempTable("mapping_table")

        joined_df = spark.sql(
            f"""
            select
                a.*,
                b.raw as raw_{key}
            from
                source_table a left outer join mapping_table b
                on a.{key} = b.ye_hashed
        """
        )

        result_df = joined_df.drop(key).withColumnRenamed(f"raw_{key}", key)

        spark.catalog.dropTempView("source_table")
        spark.catalog.dropTempView("mapping_table")

        return result_df

    def sha256_to_lake_hash(ss, source_df, key):
        spark = ss

        source_df.registerTempTable("source_table")

        mapping_df = mapping_table_load(ss=spark)
        mapping_df.registerTempTable("mapping_table")

        joined_df = spark.sql(
            f"""
            select
                a.*,
                b.lake_hashed as lake_hashed_{key}
            from
                source_table a left outer join mapping_table b
                on a.{key} = b.ye_hashed
        """
        )

        result_df = joined_df.drop(key).withColumnRenamed(f"lake_hashed_{key}", key)

        spark.catalog.dropTempView("source_table")
        spark.catalog.dropTempView("mapping_table")

        return result_df

    if before == "raw":
        if after == "sha256":
            result_df = df.withColumn(key, sha2(key, 256))
        # To-be
        elif after == "lake_hash":
            result_df = raw_to_lake_hash(ss=spark, source_df=df, key=key)
        else:
            print(f"ERROR : Could not convert data to raw to '{after}'. after value is sha256, lake_hash.")

    elif before == "sha256":
        if after == "raw":
            result_df = sha256_to_raw(ss=spark, source_df=df, key=key)

        elif after == "lake_hash":
            result_df = sha256_to_lake_hash(ss=spark, source_df=df, key=key)

        else:
            print(f"ERROR : Could not convert data to sha256 to '{after}'. after value is raw, sha256.")

    elif before == "lake_hash":
        if after == "sha256":
            unhash_df = lake_hashed_to_raw(ss=spark, source_df=df, key=key)
            result_df = unhash_df.withColumn(f"{key}", sha2(f"{key}", 256))

        elif after == "raw":
            result_df = lake_hashed_to_raw(ss=spark, source_df=df, key=key)
        else:
            print(f"ERROR : Could not convert data to lake_hash to '{after}'. after value is raw, sha256.")

    else:
        print(f"ERROR : Could not convert data to '{before}' to '{after}'. before value is raw, lake_hash, sha256.")

    return result_df
