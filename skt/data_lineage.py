import requests
from skt.vault_utils import get_secrets


DATA_LINEAGE_SECRETS_NAME = "data_lineage"


def get_lineage(table):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}").json()


def get_lineage_network(table, sequence=True):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}/network?sequence={sequence}")
