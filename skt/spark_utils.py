def map_column(input_df, replace_column, mapping_df, _from, _to):
    """
    input_df 의 replace_column 의 값을 mapping_df 의 _from -> _to 로 map.
    mapping 된 row 과 mapping 되지 않은 row 들로 이루어진 dataframe 2 개를 리턴

    """
    mapper = mapping_df.select(_from, _to)
    output_columns = input_df.columns
    mapped = (
        input_df.join(mapper, input_df[replace_column] == mapping_df[_from], how="inner")
        .drop(replace_column)
        .withColumnRenamed(_to, replace_column)
        .select(output_columns)
    )
    remainder = input_df.join(mapper, input_df[replace_column] == mapping_df[_from], how="left_anti")
    return mapped, remainder
