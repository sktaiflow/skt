import requests
import json
from skt.vault_utils import get_secrets


def set_model_name(comm_db, params):
    secret = get_secrets('mls')
    if comm_db[-3:] == 'dev':  # stg
        url = secret['dev_url']
    else:  # prd
        url = secret['prd_url']
    requests.post(url, data=json.dumps(params))


def get_recent_model_path(comm_db, model_key):
    secret = get_secrets('mls')
    if comm_db[-3:] == 'dev':  # stg
        url = secret['dev_url']
    else:  # prd
        url = secret['prd_url']
    return requests.get(f'{url}/latest').json()[model_key]


def get_model_name(key):
    secret = get_secrets('mls')
    url = secret['prd_url']
    return requests.get(f'{url}/latest').json()[key]
