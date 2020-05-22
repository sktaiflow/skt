import requests
from skt.vault_utils import get_secrets


DATA_CATALOG_SECRETS_NAME = "data_catalog"


def get_sources():
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources").json()


def get_source(source):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}").json()


def get_tables(source, limit=1000):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables?limit={limit}").json()


def get_columns(source, table_id):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/sources/{source}/tables/{table_id}/columns").json()


def search_table_by_name(name):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/v1/search/tables?name={name}").json()


def search_table_by_dc(description):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/v1/search/tables?description={description}").json()


def search_column_by_dc(description):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/v1/search/columns?description={description}").json()


def search_column_by_name(name):
    secrets = get_secrets(DATA_CATALOG_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/v1/search/columns?name={name}&fuzziness=0").json()
