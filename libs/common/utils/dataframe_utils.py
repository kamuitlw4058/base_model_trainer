def dataframe_position_total(df,key_col_index,value_col_index):
    total_value = 0
    total_key = 0
    i = 0
    for v in df.iloc[:, value_col_index].values:
        total_value += v
        total_key += df.iloc[i, key_col_index] * v
        i += 1

    return total_key, total_value


def dataframe_position(df, position, key_col, value_col, percent_total=None, percent_type ='value'):
    if percent_total is None:
        key,value = dataframe_position_total(df,key_col,value_col)
        if percent_type == 'value':
            percent_total = value
        elif percent_type == 'key':
            percent_total = key

    position_key = 0
    position_value = 0
    i = 0
    values=0
    for v in df.iloc[:,value_col].values:
        position_value += v
        values = df.iloc[i, key_col]
        position_key += df.iloc[i, key_col] * v
        if percent_type == 'value':
            if position_value > percent_total * position:
                break
        elif percent_type == 'key':
            if position_key > percent_total * position:
                break
        i += 1
    return values
    #
    # print(f"uv:{zid_per90} uv: {position * 100}%  uv_index:{i} pv_value:{values} user_pv_total:{pv_per90} pv_total:{total_pv} pv_percent: {pv_per90/ total_pv:.3f} ")
    # return values


def dataframe_position_list(df, position_list, key_col, value_col, percent_total=None, percent_type ='value'):
    result = []
    for p in position_list:
        result.append(dataframe_position(p))


    return  result
