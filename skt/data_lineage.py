import requests
from skt.vault_utils import get_secrets


DATA_LINEAGE_SECRETS_NAME = "data_catalog/lineage"


def get_lineage(table):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}").json()


def get_lineage_network(table, sequence=True):
    secrets = get_secrets(DATA_LINEAGE_SECRETS_NAME)
    return requests.get(f"{secrets['url_prd']}/relationships/lineage/node/{table}/network?sequence={sequence}")


def publish_relation(source, destination, context=None):
    from datetime import datetime

    msg = {
        "source": source,
        "destination": destination,
        "timestamp": round(datetime.utcnow().timestamp() * 1000),
        "context": context,
    }
    proxies = get_secrets(path="proxies")
    url = get_secrets(path="data_lineage")["url"]

    return requests.post(url, proxies=proxies, json=msg)
