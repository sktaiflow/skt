import requests
import json
from skt.vault_utils import get_secrets


def set_model_name(COMM_DB, params):
    secret = get_secrets('mls')
    if COMM_DB[-3:] == 'dev':  # stg
        URL = secret['dev_url']
    else:  # prd
        URL = secret['prd_url']
    requests.post(URL, data=json.dumps(params))


def get_recent_model_path(COMM_DB, model_key):
    secret = get_secrets('mls')
    if COMM_DB[-3:] == 'dev':  # stg
        URL = secret['dev_url']
    else:  # prd
        URL = secret['prd_url']
    return requests.get(f'{URL}/latest').json()[model_key]


def get_model_name(key):
    secret = get_secrets('mls')
    URL = secret['prd_url']
    return requests.get(f'{URL}/latest').json()[key]
