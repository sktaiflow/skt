from pathlib import Path
from typing import Dict, Any
from enum import Enum

import requests
import pandas as pd
import os

from skt.vault_utils import get_secrets


MLS_MODEL_DIR = os.path.join(Path.home(), "mls_temp_dir")
MODEL_BINARY_NAME = "model.joblib"
MODEL_TAR_NAME = "model.tar.gz"
MODEL_META_NAME = "model.json"
S3_DEFAULT_PATH = get_secrets("mls")["s3_model_registry_path"]

EDD_OPTIONS = get_secrets("mls")["edd_options"]

MLS_COMPONENTS_API_URL = "/api/v1/components"
MLS_META_API_URL = "/api/v1/meta_tables"
MLS_MLMODEL_API_URL = "/api/v1/models"


def set_model_name(comm_db, params, user="reco", edd: bool = False):
    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)
    if comm_db[-3:] == "dev":  # stg
        url = secret["ab_onprem_stg_url"] if edd else secret["ab_stg_url"]
        url = f"{url}{MLS_COMPONENTS_API_URL}"
    else:  # prd
        url = secret["ab_onprem_prd_url"] if edd else secret["ab_prd_url"]
        url = f"{url}{MLS_COMPONENTS_API_URL}"
    requests.post(
        url, json=params, headers={"Authorization": f"Basic {{{token}}}"},
    )


def get_all_recent_model_path(comm_db, user="reco", edd: bool = False):
    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)
    if comm_db[-3:] == "dev":  # stg
        url = secret["ab_onprem_stg_url"] if edd else secret["ab_stg_url"]
        url = f"{url}{MLS_COMPONENTS_API_URL}"
    else:  # prd
        url = secret["ab_onprem_prd_url"] if edd else secret["ab_prd_url"]
        url = f"{url}{MLS_COMPONENTS_API_URL}"

    response = requests.get(url, headers={"Authorization": f"Basic {{{token}}}"}).json().get("results")

    results = {component.get("name"): component.get("info") for component in response if component.get("is_latest")}

    return results


def get_recent_model_path(comm_db, model_key, user="reco", edd: bool = False):
    results = get_all_recent_model_path(comm_db, user, edd)
    return results.get(model_key)


def get_model_name(key, user="reco", edd: bool = False):
    results = get_all_recent_model_path("prd", user, edd)
    return results.get(key)


class ModelLibrary(Enum):
    LIGHTGBM = "lightgbm"
    XGBOOST = "xgboost"


class AWSENV(Enum):
    STG = "stg"
    PRD = "prd"
    DEV = "dev"


class MLSModelError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


def get_meta_table(
    meta_table: str, aws_env: AWSENV = AWSENV.STG.value, user="reco", edd: bool = False
) -> Dict[str, Any]:
    """
    Get a meta_table information
    Args. :
        - meta_table   :   (str) the name of meta_table
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - Dictionary value of meta_table (id / name / description / schema / items / created_at / updated_at)
    """
    assert type(meta_table) == str
    assert type(aws_env) == str

    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}"

    response = requests.get(url, headers={"Authorization": f"Basic {{{token}}}"}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))
    else:
        return results


def create_meta_table_item(
    meta_table: str,
    item_name: str,
    item_dict: Dict[str, Any],
    aws_env: AWSENV = AWSENV.STG.value,
    user="reco",
    edd: bool = False,
) -> None:
    """
    Create a meta_item
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - item_dict    :   (dict) A dictionary type (item-value) value to upload to or update of the item
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(item_dict) == dict
    assert type(aws_env) == str

    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)

    meta_table_info = get_meta_table(meta_table, aws_env, user, edd)

    values_data = dict()
    for field_name, field_spec in meta_table_info["schema"].items():
        values_data[field_name] = item_dict.get(field_name)

    request_data = dict()
    request_data["name"] = item_name
    request_data["values"] = values_data

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/meta_items"

    response = requests.post(url, json=request_data, headers={"Authorization": f"Basic {{{token}}}"}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))


def update_meta_table_item(
    meta_table: str,
    item_name: str,
    item_dict: Dict[str, Any],
    aws_env: AWSENV = AWSENV.STG.value,
    user="reco",
    edd: bool = False,
) -> None:
    """
    Update a meta_item
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - item_dict    :   (dict) A dictionary type (item-value) value to upload to or update of the item
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(item_dict) == dict
    assert type(aws_env) == str

    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)

    meta_table_info = get_meta_table(meta_table, aws_env, user, edd)

    values_data = dict()
    for field_name, field_spec in meta_table_info["schema"].items():
        values_data[field_name] = item_dict.get(field_name)

    request_data = dict()
    request_data["name"] = item_name
    request_data["values"] = values_data

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/meta_items/{item_name}"

    response = requests.put(url, json=request_data, headers={"Authorization": f"Basic {{{token}}}"}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))


def get_meta_table_item(
    meta_table: str, item_name: str, aws_env: AWSENV = AWSENV.STG.value, user="reco", edd: bool = False
) -> Dict[str, Any]:
    """
    Get a meta_table information
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - A dictionary type (item-value) value of the item_meta
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(aws_env) == str

    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/meta_items/{item_name}"

    response = requests.get(url, headers={"Authorization": f"Basic {{{token}}}"}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))
    else:
        return results


def meta_table_to_pandas(meta_table: str, aws_env: AWSENV = AWSENV.STG.value, user="reco", edd: bool = False) -> Any:
    """
    Get a meta_table as pandas dataframe
    Args. :
        - meta_table   :   (str) the name of meta_table
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - A Pandas dataframe type of the item_meta
    """
    assert type(meta_table) == str
    assert type(aws_env) == str

    secret = get_secrets("mls")
    token = secret.get("user_token").get(user)

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}"

    response = requests.get(url, headers={"Authorization": f"Basic {{{token}}}"}).json()

    if not response.get("results"):
        raise MLSModelError(f"No meta_table '{meta_table}' exists on AWS {aws_env}")

    items = response["results"]["items"]
    key = pd.DataFrame.from_records(items)["name"]
    values = pd.DataFrame.from_records(pd.DataFrame.from_records(items)["values"])

    df = pd.concat([key, values], axis=1)

    return df


def pandas_to_meta_table(
    method: str,
    meta_table: str,
    df: pd.DataFrame,
    key: str,
    values: list,
    aws_env: AWSENV = AWSENV.STG.value,
    user="reco",
    edd: bool = False,
) -> None:
    """
    Create or Update items of a meta_table from Pandas Dataframe
    Args. :
        - method       :   (str) requests method 'create' or 'update'
        - meta_table   :   (str) MLS meta table name
        - df           :   (pd.DataFrame) input table
        - key          :   (str) key column in dataframe
        - values       :   (list) Dataframe columns for input
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - user         :   (str) the name of user (default is 'reco')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(aws_env) == str
    assert method in ["create", "update"]
    assert type(meta_table) == str
    assert type(df) == pd.core.frame.DataFrame
    assert type(key) == str
    assert type(values) == list

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/meta_items"

    def to_json(x):
        insert_dict = {}
        insert_dict["name"] = x[key]
        insert_dict["values"] = {}

        for value in values:
            insert_dict["values"][value] = x[value]

        return insert_dict

    json_series = df.apply(lambda x: to_json(x), axis=1)

    for meta in json_series:
        if method == "create":
            create_meta_table_item(meta_table, meta.get("name"), meta.get("values"), aws_env, user)
        else:
            update_meta_table_item(meta_table, meta.get("name"), meta.get("values"), aws_env, user)


def get_ml_model(
    user: str, model_name: str, model_version: str, aws_env: AWSENV = AWSENV.STG.value, edd: bool = False
) -> Dict[str, Any]:
    """
    Get an MLModel
    Args. :
        - user           :   (str) the name of a MLModel user
        - model_name     :   (str) the name of MLModel
        - model_version  :   (str) the version of MLModel
        - aws_env        :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd            :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - Dictionary value of MLModel
    """
    assert type(user) == str
    assert type(model_name) == str
    assert type(model_version) == str
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_MLMODEL_API_URL}/{model_name}/versions/{model_version}"

    response = requests.get(url, params={"user": user}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(f"No MLModel for user: {user} / model_name: {model_name} / model_version: {model_version}")
    else:
        return results[0]


def get_ml_model_meta(
    user: str, model_name: str, model_version: str, aws_env: AWSENV = AWSENV.STG.value, edd: bool = False
) -> Dict[str, Any]:
    """
    Get a list of MLModel meta
    Args. :
        - user           :   (str) the name of a MLModel user
        - model_name     :   (str) the name of MLModel
        - model_version  :   (str) the version of MLModel
        - aws_env        :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - Dictionary value of model_meta
    """
    assert type(user) == str
    assert type(model_name) == str
    assert type(model_version) == str
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_MLMODEL_API_URL}/{model_name}/versions/{model_version}/meta"

    response = requests.get(url, params={"user": user}).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(f"No MLModel for user: {user} / model_name: {model_name} / model_version: {model_version}")
    else:
        return results[0].get("model_meta")


def update_ml_model_meta(
    user: str,
    model_name: str,
    model_version: str,
    model_meta_dict: Dict[str, Any],
    aws_env: AWSENV = AWSENV.STG.value,
    edd: bool = False,
) -> None:
    """
    Update(or Create) model_meta
    Args. :
        - user            :   (str) the name of a MLModel user
        - model_name      :   (str) the name of MLModel
        - model_version   :   (str) the version of MLModel
        - model_meta_dict :   (dict) the version of MLModel
        - aws_env         :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd             :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(model_name) == str
    assert type(model_version) == str
    assert type(model_meta_dict) == dict
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_MLMODEL_API_URL}/{model_name}/versions/{model_version}/meta"

    request_data = dict()
    request_data["user"] = user
    request_data["model_meta"] = model_meta_dict

    requests.patch(url, json=request_data).json()
