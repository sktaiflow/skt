import datetime
import json
import os

import requests
from skt.vault_utils import get_secrets


headers = {
    "Catalog-User": os.environ.get("USER", "unknown_user"),
    "Catalog-Hostname": os.environ.get("HOSTNAME", "unknown_hostname"),
    "Catalog-NB-User": os.environ.get("NB_USER", None),
}


DATA_CATALOG_SECRETS_NAME = "data_catalog/client"
DATA_LINEAGE_SECRETS_NAME = "data_catalog/lineage"


def get_lineage(table):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}").json()


def get_lineage_network(table, sequence=True):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}/network?sequence={sequence}")


def get_sources():
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources").json()


def get_source(source):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}").json()


def get_table(table_id):
    return get_resource("tables", table_id)


def get_tables(source, limit=1000):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables?limit={limit}").json()


def get_table_detail(source, table_id):
    print("deprecated! use 'get_table(table_id)' ")
    return get_table(table_id)


def get_column(column_id):
    return get_resource("columns", column_id)


def get_columns(source, table_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables/{table_id}/columns").json()


def get_column_detail(source, column_id):
    print("deprecated! use 'get_column(column_id)' ")
    return get_column(column_id)


def get_resource(resource_name, resource_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/v1/resources/{resource_name}/{resource_id}").json()


def get_query(query_id):
    return get_resource("processes", query_id)


def get_queries(source, limit=100):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/processes?limit={limit}").json()


def get_query_detail(source, query_id):
    print("deprecated! use 'get_query(query_id)' ")
    return get_query(query_id)


def search_table_by_name(name, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    kwargs["name"] = name

    return requests.get(f"{secrets['url_prd']}/v1/search/tables", params=kwargs).json()


def search_table_by_dc(description, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    kwargs["description"] = description

    return requests.get(f"{secrets['url_prd']}/v1/search/tables", params=kwargs).json()


def search_column_by_dc(description, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    kwargs["description"] = description

    return requests.get(f"{secrets['url_prd']}/v1/search/columns", params=kwargs).json()


def search_column_by_name(name, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    kwargs["name"] = name

    return requests.get(f"{secrets['url_prd']}/v1/search/columns", params=kwargs).json()


def search_queries_by_table_id(table_id, **kwargs):
    limit = kwargs.get("limit", 100)
    fuzziness = kwargs.get("fuzziness", "AUTO")
    operator = kwargs.get("operator", "and")
    offset = kwargs.get("offset", None)
    fields = kwargs.get("fields", None)
    must = kwargs.get("must", None)
    sort = kwargs.get("sort", "desc")
    start_date = kwargs.get("start_date", None)
    end_date = kwargs.get("end_date", None)

    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)

    es_sort = [{"start_time": sort}]

    params = {
        "inputs": table_id,
        "outputs": table_id,
        "limit": limit,
        "fuzziness": fuzziness,
        "offset": offset,
        "operator": operator,
        "fields": fields,
        "must": must,
        "sort": json.dumps(es_sort),
    }

    if start_date or end_date:
        range_filter = {"range": {"start_time": {}}}

        if start_date:
            range_filter["range"]["start_time"]["gte"] = start_date

        if end_date:
            range_filter["range"]["start_time"]["lt"] = end_date

        params["range_filter"] = json.dumps(range_filter)

    return requests.get(secrets["url_prd"] + "/v1/search/processes", params=params).json()


def search_queries_by_column(table_id, column_name, **kwargs):
    limit = kwargs.get("limit", 100)
    fuzziness = kwargs.get("fuzziness", "AUTO")
    operator = kwargs.get("operator", "and")
    offset = kwargs.get("offset", None)
    must = kwargs.get("must", None)
    sort = kwargs.get("sort", "desc")
    start_date = kwargs.get("start_date", None)
    end_date = kwargs.get("end_date", None)

    catalog_secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    lineage_secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)

    dataset = None
    table_name = None
    result = []

    if "." in table_id:
        dataset, table_name = table_id.split(".")

    if dataset:
        column_id = ".".join([dataset, table_name, column_name])

        params = {
            "qualified_name": column_id,
            "fields": "qualified_name",
            "fuzziness": fuzziness,
            "operator": operator,
            "must": must,
        }

        response = requests.get(catalog_secrets["url_prd"] + "/v1/search/columns", params=params).json()

        column_list = response.get("qualified_name", {}).get("hits", [])

        if column_list:
            max_score_column_id = column_list[0]["_source"]["qualified_name"]

            params = {
                "offset": offset,
                "limit": limit,
                "sort_by_time": True,
                "order": sort,
                "start_date": start_date,
                "end_date": end_date,
            }

            relationship_list = requests.get(
                lineage_secrets["url_prd"] + "/relationships/queries/resource/columns/" + max_score_column_id,
                params=params,
            ).json()

            for each_relationship in relationship_list:
                query_id = each_relationship["source"]
                project_id = query_id.split("@")[1]
                response = requests.get(
                    catalog_secrets["url_prd"] + f"/sources/bigquery-{project_id}/processes/{query_id}"
                ).json()

                result.append(response)
    else:
        raise Exception("You should use [dataset].[table_name] style on table_id argument. Abort")

    return result


def get_user_queries(user_name, start_date=None, end_date=None, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)

    default_order = "asc" if (start_date or end_date) else "desc"
    order = kwargs.get("sort", default_order)
    limit = kwargs.get("limit", 100)

    es_sort = [{"start_time": order}]
    es_limit = min(100, limit)

    params = {"user_name": user_name, "limit": es_limit, "sort": json.dumps(es_sort)}

    gte = start_date or (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    lt = end_date or datetime.datetime.now().strftime("%Y-%m-%d")

    range_filter = {"start_time": {"gte": gte, "lt": lt}}

    params["range_filter"] = json.dumps(range_filter)

    total_queries = []

    response = requests.get(secrets["url_prd"] + "/v1/search/processes", params=params).json()

    total_queries.extend(response["user_name"]["hits"])
    total = response["user_name"]["total"]["value"]

    while total > len(total_queries) and limit < len(total_queries):
        params["offset"] = json.dumps(total_queries[-1]["sort"])

        response = requests.get(secrets["url_prd"] + "/v1/search/processes", params=params).json()
        total_queries.extend(response["user_name"]["hits"])

    return total_queries


def get_user_data_access(user_name, start_date=None, end_date=None, timeseries=False, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    lineage_secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)

    default_order = "asc" if (start_date or end_date) else "desc"
    order = kwargs.get("sort", default_order)
    limit = kwargs.get("limit", 1000)

    es_sort = [{"start_time": order}]
    es_limit = min(1000, limit)

    params = {
        "user_name": user_name,
        "sort": json.dumps(es_sort),
        "limit": es_limit,
        "fields": json.dumps(["inputs", "outputs"]),
    }

    gte = start_date or (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    lt = end_date or datetime.datetime.now().strftime("%Y-%m-%d")

    range_filter = {"start_time": {"gte": gte, "lt": lt}}

    params["range_filter"] = json.dumps(range_filter)

    total_queries = []

    response = requests.get(secrets["url_prd"] + "/v1/search/processes", params=params).json()

    total_queries.extend(response["user_name"]["hits"])
    total = response["user_name"]["total"]["value"]

    while total > len(total_queries) and limit < len(total_queries):
        params["offset"] = json.dumps(total_queries[-1]["sort"])

        response = requests.get(secrets["url_prd"] + "/v1/search/processes", params=params).json()
        total_queries.extend(response["user_name"]["hits"])

    result = []

    table_dict = {}
    column_dict = {}

    for each_query in total_queries:
        query_id = each_query["_id"]

        if timeseries:
            inputs = each_query["_source"].get("inputs", [])
            outputs = each_query["_source"].get("outputs", [])

            response = requests.get(
                lineage_secrets["url_prd"] + f"/relationships/queries/query/{query_id}/columns", params=params
            ).json()
            column_list = list(map(lambda each: each["target"], response))

            result.append(
                {
                    "inputs": inputs,
                    "outputs": outputs,
                    "columns": column_list,
                    "start_time": each_query["sort"][0],
                    "query_id": query_id,
                }
            )
        else:
            inputs = each_query["_source"].get("inputs", []) or []
            outputs = each_query["_source"].get("outputs", []) or []
            for each in inputs:
                if each not in table_dict:
                    table_dict[each] = 1

            for each in outputs:
                if each not in table_dict:
                    table_dict[each] = 1

            response = requests.get(
                lineage_secrets["url_prd"] + f"/relationships/queries/query/{query_id}/columns", params=params
            ).json()
            column_list = list(map(lambda each: each["target"], response))

            for each_column in column_list:
                if each_column not in column_dict:
                    column_dict[each_column] = 1

    if timeseries:
        return result
    else:
        return {"tables": list(table_dict.keys()), "columns": list(column_dict.keys())}


def get_table_top_n_tables(n, start_date=None, end_date=None):
    lineage_secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)

    params = {"top_n": n, "start_date": start_date, "end_date": end_date}

    response = requests.get(lineage_secrets["url_prd"] + "/relationships/queries/top_n/tables", params=params).json()

    return response


def get_table_top_n_columns(n, start_date=None, end_date=None):
    lineage_secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)

    params = {"top_n": n, "start_date": start_date, "end_date": end_date}

    response = requests.get(lineage_secrets["url_prd"] + "/relationships/queries/top_n/columns", params=params).json()

    return response
