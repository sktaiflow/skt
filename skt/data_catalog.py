import json
import os

import requests
from skt.vault_utils import get_secrets


headers = {
    "Catalog-User": os.environ.get("USER", "unknown_user"),
    "Catalog-Hostname": os.environ.get("HOSTNAME", "unknown_hostname"),
    "Catalog-NB-User": os.environ.get("NB_USER", None),
}


DATA_CATALOG_SECRETS_NAME = "data_catalog"
DATA_LINEAGE_SECRETS_NAME = "data_lineage"


def get_sources():
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources").json()


def get_source(source):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}").json()


def get_tables(source, limit=1000):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables?limit={limit}").json()


def get_table_detail(source, table_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables/{table_id}").json()


def get_columns(source, table_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables/{table_id}/columns").json()


def get_column_detail(source, column_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/columns/{column_id}").json()


def get_queries(source, limit=100):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/processes?limit={limit}").json()


def get_query_detail(source, query_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/processes/{query_id}").json()


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


def search_bigquery_queries_by_table_id(table_id, **kwargs): 
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
        "fields":  fields,
        "must": must,
        "sort": json.dumps(es_sort)
    }

    if start_date or end_date:
        range_filter = {
            "range": {
                "start_time": { }
            }
        }

        if start_date:
            range_filter["range"]["start_time"]["gte"] = start_date

        if end_date:
            range_filter["range"]["start_time"]["lt"] = end_date

        params["range_filter"] = json.dumps(range_filter)

    return requests.get(secrets['url_prd'] + "/v1/search/processes", params=params).json()


def search_bigquery_queries_by_column(table_id, column_name, **kwargs): 
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
            "must": must
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
                "end_date": end_date
            }

            relationship_list = requests.get(lineage_secrets["url_prd"] + "/relationships/bigquery/queries/columns/" + max_score_column_id, params=params ).json()

            for each_relationship in relationship_list:
                query_id = each_relationship["source"]
                project_id = query_id.split("@")[1]
                response = requests.get(catalog_secrets["url_prd"] + f"/sources/bigquery-{project_id}/processes/{query_id}").json()

                result.append(response)
    else:
        raise Exception("You should use [dataset].[table_name] style on table_id argument. Abort")

    return result


def search_bigquery_queries_by_user_name(user_name, **kwargs): 
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
        "user_name": user_name,
        "limit": limit,
        "fuzziness": fuzziness,
        "offset": offset,
        "operator": operator,
        "fields":  fields,
        "must": must,
        "sort": json.dumps(es_sort)
    }

    if start_date or end_date:
        range_filter = {
            "range": {
                "start_time": { }
            }
        }

        if start_date:
            range_filter["range"]["start_time"]["gte"] = start_date

        if end_date:
            range_filter["range"]["start_time"]["lt"] = end_date

        params["range_filter"] = json.dumps(range_filter)

    return requests.get(secrets['url_prd'] + "/v1/search/processes", params=params).json()


def get_user_data_access_in_bigquery(username, start_date, end_date, timeseries=False, **kwargs):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    lineage_secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)

    es_sort = [{"start_time": "asc"}]

    params = {
        "user_name": username,
        "sort": json.dumps(es_sort),
        "fields": json.dumps(["inputs","outputs"])
    }

    if start_date or end_date:
        range_filter = {
            "range": {
                "start_time": { }
            }
        }

        if start_date:
            range_filter["range"]["start_time"]["gte"] = start_date

        if end_date:
            range_filter["range"]["start_time"]["lt"] = end_date

        params["range_filter"] = json.dumps(range_filter)
    
    total_queries = []

    response = requests.get(secrets['url_prd'] + "/v1/search/processes", params=params).json()
    
    total_queries.extend(response["user_name"]["hits"])
    total = response["user_name"]["total"]["value"]

    while total > len(total_queries):
        params["offset"] = json.dumps(total_queries[-1]["sort"])

        response = requests.get(secrets['url_prd'] + "/v1/search/processes", params=params).json()
        total_queries.extend(response["user_name"]["hits"])

    result = []

    table_dict = {}
    column_dict = {}

    for each_query in total_queries:
        query_id = each_query["_id"]

        if timeseries:
            table_list = []
            table_list.extend(each_query["_source"]["inputs"])
            table_list.extend(each_query["_source"]["outputs"])

            response = requests.get(lineage_secrets["url_prd"] + f"/relationships/bigquery/queries/query/{query_id}/columns", params=params ).json()
            column_list = list(map(lambda each: each["target"], response))

            result.append({
                "inputs": each_query["_source"]["inputs"], 
                "outputs": each_query["_source"]["outputs"],
                "columns": column_list,
                "start_time": each_query["sort"][0]
            })
        else:
            for each in each_query["_source"]["inputs"]:
                if each not in table_dict:
                    table_dict[each] = 1
            
            for each in each_query["_source"]["outputs"]:
                if each not in table_dict:
                    table_dict[each] = 1

            response = requests.get(lineage_secrets["url_prd"] + f"/relationships/bigquery/queries/query/{query_id}/columns", params=params ).json()
            column_list = list(map(lambda each: each["target"], response))

            for each_column in column_list:
                if each_column not in column_dict:
                    column_dict[each_column] = 1
    
    if timeseries:
        return result
    else:
        return { "tables": list(table_dict.keys()), "columns": list(column_dict.keys()) }
