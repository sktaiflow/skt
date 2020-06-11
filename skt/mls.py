from pathlib import Path
from typing import Dict, Any
from enum import Enum

import requests
import json
import pandas as pd
import os

from skt.vault_utils import get_secrets


MLS_MODEL_DIR = os.path.join(Path.home(), "mls_temp_dir")
MODEL_BINARY_NAME = "model.joblib"
MODEL_TAR_NAME = "model.tar.gz"
MODEL_META_NAME = "model.json"
S3_DEFAULT_PATH = get_secrets("mls")["s3_model_registry_path"]

EDD_OPTIONS = get_secrets("mls")["edd_options"]

HEADER = {"Content-Type": "application/json"}
MLS_COMPONENTS_API_URL = "/v1/components"
MLS_META_API_URL = "/api/v1/meta"
MLS_MLMODEL_API_URL = "/api/v1/models"


def set_model_name(comm_db, params):
    secret = get_secrets("mls")
    if comm_db[-3:] == "dev":  # stg
        url = f"{secret['ab_stg_url']}{MLS_COMPONENTS_API_URL}"
    else:  # prd
        url = f"{secret['ab_prd_url']}{MLS_COMPONENTS_API_URL}"
    requests.post(url, data=json.dumps(params))


def get_recent_model_path(comm_db, model_key, user="reco"):
    secret = get_secrets("mls")
    if comm_db[-3:] == "dev":  # stg
        url = f"{secret['ab_stg_url']}{MLS_COMPONENTS_API_URL}"
    else:  # prd
        url = f"{secret['ab_prd_url']}{MLS_COMPONENTS_API_URL}"
    return requests.get(f"{url}/latest?user={user}").json()[model_key]


def get_model_name(key, user="reco"):
    secret = get_secrets("mls")
    url = f"{secret['ab_prd_url']}{MLS_COMPONENTS_API_URL}"
    return requests.get(f"{url}/latest?user={user}").json()[key]


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


def get_meta_table(meta_table: str, aws_env: AWSENV = AWSENV.STG.value, edd: bool = False) -> Dict[str, Any]:
    """
    Get a meta_table information
    Args. :
        - meta_table   :   (str) the name of meta_table
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - Dictionary value of meta_table (id / name / description / schema / items / created_at / updated_at)
    """
    assert type(meta_table) == str
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}"

    response = requests.get(url).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))
    else:
        return results


def create_meta_table_item(
    meta_table: str, item_name: str, item_dict: Dict[str, Any], aws_env: AWSENV = AWSENV.STG.value, edd: bool = False
) -> None:
    """
    Create a meta_item
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - item_dict    :   (dict) A dictionary type (item-value) value to upload to or update of the item
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - force        :   (bool) Force to overwrite(update) the item_meta value if already exists
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(item_dict) == dict
    assert type(aws_env) == str

    meta_table_info = get_meta_table(meta_table, aws_env, edd)

    values_data = dict()
    for field_name, field_spec in meta_table_info["schema"].items():
        values_data[field_name] = item_dict.get(field_name)

    request_data = dict()
    request_data["name"] = item_name
    request_data["values"] = values_data

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/items"

    response = requests.post(url, headers=HEADER, data=json.dumps(request_data)).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))


def update_meta_table_item(
    meta_table: str, item_name: str, item_dict: Dict[str, Any], aws_env: AWSENV = AWSENV.STG.value, edd: bool = False
) -> None:
    """
    Update a meta_item
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - item_dict    :   (dict) A dictionary type (item-value) value to upload to or update of the item
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(item_dict) == dict
    assert type(aws_env) == str

    meta_table_info = get_meta_table(meta_table, aws_env, edd)

    values_data = dict()
    for field_name, field_spec in meta_table_info["schema"].items():
        values_data[field_name] = item_dict.get(field_name)

    request_data = dict()
    request_data["name"] = item_name
    request_data["values"] = values_data

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/items/{item_name}"

    response = requests.put(url, headers=HEADER, data=json.dumps(request_data)).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))


def get_meta_table_item(
    meta_table: str, item_name: str, aws_env: AWSENV = AWSENV.STG.value, edd: bool = False
) -> Dict[str, Any]:
    """
    Get a meta_table information
    Args. :
        - meta_table   :   (str) the name of meta_table
        - item_name    :   (str) the name of meta_item to be added
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - A dictionary type (item-value) value of the item_meta
    """
    assert type(meta_table) == str
    assert type(item_name) == str
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}/items/{item_name}"

    response = requests.get(url).json()
    results = response.get("results")

    if not results:
        raise MLSModelError(response.get("error"))
    else:
        return results


def meta_table_to_pandas(meta_table: str, aws_env: AWSENV = AWSENV.STG.value, edd: bool = False) -> Any:
    """
    Get a meta_table as pandas dataframe
    Args. :
        - meta_table   :   (str) the name of meta_table
        - aws_env      :   (str) AWS ENV in 'stg / prd' (default is 'stg')
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
    Returns :
        - A Pandas dataframe type of the item_meta
    """
    assert type(meta_table) == str
    assert type(aws_env) == str

    url = get_secrets("mls")[f"ab_{'onprem_' if edd else ''}{aws_env}_url"]
    url = f"{url}{MLS_META_API_URL}/{meta_table}"

    response = requests.get(url).json()

    if not response.get("results"):
        raise MLSModelError(f"No meta_table '{meta_table}' exists on AWS {aws_env}")

    items = response["results"]["items"]
    key = pd.DataFrame.from_records(items)["name"]
    values = pd.DataFrame.from_records(pd.DataFrame.from_records(items)["values"])

    df = pd.concat([key, values], axis=1)

    return df


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
        - edd          :   (bool) True if On-prem env is on EDD (default is False)
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
        - force           :   (bool) Force to overwrite existing model_meta (default : False)
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

    requests.patch(url, headers=HEADER, data=json.dumps(request_data)).json()
