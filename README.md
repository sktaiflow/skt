# SKT Package


[![Actions Status](https://github.com/sktaiflow/skt/workflows/release/badge.svg)](https://github.com/sktaiflow/skt/actions)

This is highly site dependent package.
Resources are abstracted into package structure.


## Usage


Execute hive query without fetch result
```python
from skt.ye import hive_execute
hive_execute(ddl_or_ctas_query)
```


Fetch resultset from hive query
```python
from skt.ye import hive_get_result
result_set = hive_get_result(select_query)
```


Get pandas dataframe from hive qeruy resultset
```python
from skt.ye import hive_to_pandas
pandas_df = hive_to_pandas(hive_query)
```


Get pandas dataframe from parquet file in hdfs
```python
from skt.ye import parquet_to_pandas
pandas_df = parquet_to_pandas(hdfs_path)
```


Save pandas dataframe as parquet in hdfs
```python
from skt.ye import get_spark
from skt.ye import pandas_to_parquet
spark = get_spark()
pandas_to_parquet(pandas_df, hdfs_path, spark) # we need spark for this operation
spark.stop()
```


Work with spark
```python
from skt.ye import get_spark
spark = get_spark()
# do with spark session
spark.stop()
```


Work with spark-bigquery-connector
```python
# SELECT
from skt.gcp import bq_table_to_pandas 
pandas_df = bq_table_to_pandas('dataset', 'table_name', ['col_1', 'col_2'], '2020-01-01', 'svc_mgmt_num is not null')
# INSERT 
from skt.gcp import pandas_to_bq_table
pandas_to_bq_table(pandas_df, 'dataset', 'table_name', '2020-03-01')
```


Send slack message
```python
from skt.ye import slack_send
text = 'Hello'
username = 'airflow'
channel = '#leavemealone'
slack_send(text=text, username=username, channel=channel)
```


Get bigquery client
```python
from skt.gcp import get_bigquery_client
bq = get_bigquery_client()
bq.query(query)
```


Access MLS
```python
from skt.mls import set_model_name
from skt.mls import get_recent_model_path
from skt.ye import get_pkl_from_hdfs

set_model_name(COMM_DB, params)
path = get_recent_model_path(COMM_DB, model_key)
model = get_pkl_from_hdfs(f'{path})
```


Use NES CLI
```bas
nes input_notebook_url -p k1 v1 -p k2 v2 -p k3 v3
```


Use github util
```python
from skt.ye import get_github_util
g = get_github_util
# query graphql
res = g.query_gql(graph_ql)
# get file in github repository
byte_object = g.download_from_git(github_url_path)
```


## Installation

```sh
$ pip install skt --upgrade
```

If you would like to install submodules for AIR

```sh
$ pip install skt[air] --upgrade
```

## Develop

Create issue first and follow the GitHub flow
https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/github-flow


# AIPS EDA tools

## OVERVIEW

- **Modeling EDA** 시 활용할 수 있는 기능의 공통 module
- **Modules**
    - 1) EDA (Nuemric / Categorical variable)
<br>
<br>

## 1) EDA
#### 1. Numeric variable EDA
- **def** *numeric_eda_plot*
    
```
    Numeric feature에 대한 EDA Plot function
    
    Args. :
        - df           :   Pandas DataFrame 형태의 EDA대상 데이터
        - feature_list :   EDA 대상 feature list (df의 columns)
        - label_col    :   Label(or Hue) column
        - cols         :   Multi-plot 시 grid column 개수 (row 개수는 feature_list에 따라 자동으로 결정 됨)
        - n_samples    :   Label 별 sampling 할 개수 (default = -1(전수 데이터로 EDA할 경우))
        - plot_type    :   density or box (default = 'density')
        - stat_yn      :   기초 통계량 출력여부 (mean / min / max / 1q / 3q) (default : False)
        - figsize      :   (default : (7,4))
    
    Returns : 
        matplotlib.pyplot object

    Example : 
        fig = numeric_eda_plot(df, ['age'], 'answer', cols = 1, n_samples = 10000, plot_type='density', stat_yn=True, figsize = (7,4))
        fig
        
        if want to Save the EDA images,
        fig.savefig('filename')
```


#### 2. Categorical variable EDA
- **def** *categorical_eda_plot*
    
```
    Categorical feature에 대한 EDA Plot function
    
    Args. :
        - df           :   Pandas DataFrame 형태의 EDA대상 데이터
        - feature_list :   EDA 대상 feature list (df의 columns)
        - label_col    :   Label(or Hue) column
        - cols         :   Multi-plot 시 grid column 개수 (row 개수는 feature_list에 따라 자동으로 결정 됨)
        - n_samples    :   Label 별 sampling 할 개수 (default = -1(전수 데이터로 EDA할 경우))
        - figsize      :   (default : (7,4))
    
    Returns : 
        matplotlib.pyplot object


    Example : 
        Example : 
        fig = categorical_eda_plot(df, ['sex_cd'], 'answer', cols = 1, n_samples = 10000, figsize = (7,4))
        fig
        
        if want to Save the EDA images,
        fig.savefig('filename')
    
```
