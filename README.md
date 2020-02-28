# SKT Package
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
        df = load_from_hive(conn, "select * from dumbo.aips_eda_samples")
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
        df = load_from_hive(conn, "select * from dumbo.aips_eda_samples")
        fig = categorical_eda_plot(df, ['sex_cd'], 'answer', cols = 1, n_samples = 10000, figsize = (7,4))
        fig
        
        if want to Save the EDA images,
        fig.savefig('filename')
    
```
