import requests
from collections import namedtuple

from skt.vault_utils import get_secrets


def get_access_token():
    secrets = get_secrets("lake/hash")
    url = secrets["auth_url"]
    client_id = secrets["client_id"]
    client_secret = secrets["client_secret"]
    data = {"grant_type": "client_credentials"}
    res = requests.post(url, auth=(client_id, client_secret), data=data)
    return res.json()["access_token"]


Task = namedtuple("Task", ["url_key", "input_key", "output_key"])
hash_task = Task("hash", "unhashedValues", "hashedValue")
unhash_task = Task("unhash", "hashedValues", "unhashedValue")


class Hash:
    access_token = get_access_token()
    url = {
        "hash": get_secrets("lake/hash")["hash_url"],
        "unhash": get_secrets("lake/hash")["unhash_url"],
    }

    @classmethod
    def renew_token(cls):
        cls.access_token = get_access_token()

    @classmethod
    def make_headers(cls):
        return {
            "Authorization": f"Bearer {cls.access_token}",
        }

    @classmethod
    def map_s(cls, values, unhash=False):
        task = hash_task
        if unhash:
            task = unhash_task
        url = cls.url[task.url_key]
        data = {"type": "s", task.input_key: values}
        r = requests.post(url, headers=cls.make_headers(), json=data)
        if r.status_code == 401:
            cls.renew_token()
            r = requests.post(url, headers=cls.make_headers(), json=data)
        if r.status_code != 200:
            raise Exception(r.content.decode("utf8"))
        return [x[task.output_key] for x in r.json()["response"]]


def hash_s(values):
    return Hash.map_s(values, unhash=False)


def unhash_s(values):
    return Hash.map_s(values, unhash=True)
