from pathlib import Path
from typing import Dict, Any
from enum import Enum

import requests
import pandas as pd
import os

from skt.vault_utils import get_secrets

from sktmls.meta_tables.meta_table import MetaTableClient


MLS_MODEL_DIR = os.path.join(Path.home(), "mls_temp_dir")
MODEL_BINARY_NAME = "model.joblib"
MODEL_TAR_NAME = "model.tar.gz"
MODEL_META_NAME = "model.json"
S3_DEFAULT_PATH = get_secrets("mls")["s3_model_registry_path"]

EDD_OPTIONS = get_secrets("mls")["edd_options"]

MLS_COMPONENTS_API_URL = "/api/v1/components"
MLS_META_API_URL = "/api/v1/meta_tables"
MLS_MLMODEL_API_URL = "/api/v1/models"


def check_client_config(config):
    client = MetaTableClient(**config)
    client.list_meta_tables()
    return config


def generate_configs(env, user):
    import pkg_resources
    from packaging import version
    from skt.vault_utils import get_secrets
    from sktmls import MLSENV

    mlsenv = MLSENV.STG
    if env == "prd":
        mlsenv = MLSENV.PRD
    secrets = get_secrets(path="mls")
    config = dict(
        env=mlsenv,
        username=secrets.get(f"{user}_id"),
        password=secrets.get(f"{user}_pass"),
    )

    # sktmls version upgrade 후 수정
    mls_v = pkg_resources.get_distribution("sktmls").version
    if version.parse(mls_v) > version.parse("2020.8.29"):
        from sktmls import MLSRuntimeENV

        return [{**config, "runtime_env": r} for r in MLSRuntimeENV.list_items()]

    return [config]


def get_mls_config(env, user):
    import concurrent.futures

    configs = generate_configs(env=env, user=user)
    e = concurrent.futures.ThreadPoolExecutor(max_workers=len(configs) + 1)
    fs = [e.submit(check_client_config, conf) for conf in configs]
    for f in concurrent.futures.as_completed(fs):
        if f.exception() is None:
            config = f.result()
            break
    e.shutdown(wait=False)
    return config


def get_mls_meta_table_client(env="stg", user="reco"):
    config = get_mls_config(env, user)
    return MetaTableClient(**config)


def get_mls_component_client(env="stg", user="reco"):
    from sktmls.components import ComponentClient

    config = get_mls_config(env, user)
    return ComponentClient(**config)


def get_mls_dimension_client(env="stg", user="reco"):
    from sktmls.dimensions import DimensionClient

    config = get_mls_config(env, user)
    return DimensionClient(**config)


def get_mls_experiment_client(env="stg", user="reco"):
    from sktmls.experiments import ExperimentClient

    config = get_mls_config(env, user)
    return ExperimentClient(**config)


def get_mls_ml_model_client(env="stg", user="reco"):
    from sktmls.models import MLModelClient

    config = get_mls_config(env, user)
    return MLModelClient(**config)


def get_mls_automl_batch_prediction_client(env="stg", user="reco"):
    from sktmls.models.automl.automl_prediction import AutoMLBatchPredictionClient

    config = get_mls_config(env, user)
    return AutoMLBatchPredictionClient(**config)


def get_mls_model_registry(env="stg", user="reco"):
    from sktmls import ModelRegistry

    config = get_mls_config(env, user)
    return ModelRegistry(env=config["env"], runtime_env=config["runtime_env"])


def get_channel_client(env="stg", user="reco"):
    from sktmls.channels import ChannelClient

    config = get_mls_config(env, user)
    return ChannelClient(**config)


def get_mls_profile_api_client(env="stg", user="reco"):
    from sktmls.apis.profile_api import MLSProfileAPIClient

    config = dict({conf for conf in get_mls_config(env, user).items() if conf[0] in ["env", "runtime_env"]})
    return MLSProfileAPIClient(**config, **get_secrets("mls").get("reco_api"))


def get_mls_recommendation_api_client(env="stg", user="reco"):
    from sktmls.apis.recommendation_api import MLSRecommendationAPIClient

    config = dict({conf for conf in get_mls_config(env, user).items() if conf[0] in ["env", "runtime_env"]})
    return MLSRecommendationAPIClient(**config, **get_secrets("mls").get("reco_api"))


def create_or_update_meta_table(table_name, schema=None, env="stg", user="reco"):
    c = get_mls_meta_table_client(env=env, user=user)
    if c.meta_table_exists(name=table_name):
        t = c.get_meta_table(name=table_name)
        if schema:
            c.update_meta_table(meta_table=t, schema=schema)
    else:
        c.create_meta_table(name=table_name, schema=schema)


def upsert_meta_table(table_name, items_dict, env="stg", user="reco"):
    c = get_mls_meta_table_client(env=env, user=user)
    t = c.get_meta_table(name=table_name)
    items = c.create_meta_items(meta_table=t, items_dict=items_dict)
    return len(items)


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
        url,
        json=params,
        headers={"Authorization": f"Basic {{{token}}}"},
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
