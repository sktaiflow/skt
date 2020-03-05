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


def get_recent_model_path(COMM_DB, model_name):
    secret = get_secrets('mls')
    if COMM_DB[-3:] == 'dev':  # stg
        URL = secret['dev_url']
    else:  # prd
        URL = secret['prd_url']
    response = requests.get(f'{URL}/latest').text
    return json.loads(response)[model_name]


def get_model_name(key):
    secret = get_secrets('mls')
    URL = secret['prd_url']
    return requests.get(f'{URL}/latest').json()[key]

# example of load recent model
# os.environ['ARROW_LIBHDFS_DIR'] = '/usr/hdp/3.0.1.0-187/usr/lib'
# connection = pyarrow.hdfs.connect(user='airflow')

# # Load model
# model_name = f'crystal_lgbm_{RECO_TYPE}_{LAST_TRAINED_DT}'
# byte_object = connection.cat(f'/data/{COMM_DB}/models/{RECO_TYPE}/{AGE_BAND}/{model_name}.pkl')
# model = pickle.loads(byte_object)
# print(model_name)
