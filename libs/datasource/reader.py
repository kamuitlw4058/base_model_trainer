import  json
from libs.common.database.reader import get_mysql_reader
reader =get_mysql_reader()


def get_format_str(data)->str:
    if data is None:
        return ""
    return str(data)

def get_col_list(keys_values) ->list:
    col_list_str = get_format_str(keys_values)
    if col_list_str == "" or col_list_str == "null":
        ret=[]
    else:
        ret =json.loads(col_list_str)
    return ret


def get_col_dict(keys_values)->dict:
    col_list_str = get_format_str(keys_values)
    if col_list_str == "" or col_list_str == "null":
        ret={}
    else:
        ret =json.loads(col_list_str)
    return ret

def get_featues_meta_dict(row) ->dict:
    d={}
    default_args = get_col_dict(row['default_args'])
    d['default_args'] = default_args
    d['feature_name'] = get_format_str(row['feature_name'])
    d['feature_type'] = row['feature_type']
    d['feature_class'] = row['feature_class']
    d['feature_context'] = get_format_str(row['feature_context'])
    d['keys'] = get_col_list(row['keys'])
    d['join_type'] = row['join_type']
    d['features_process'] = row['features_process']
    d['depend'] = row['depend']
    return d



def get_features_meta_by_name(name:str):
    df = reader.read_sql(f"select * from features_meta where feature_name ='{name}'")
    if df is not None and df.iloc[:,0].size> 0:
        return get_featues_meta_dict(df.iloc[0,:])
    return None

# from datetime import  datetime
# feaute_name = 'av_ctr_day_interval{interval}'
# feature_meta = get_features_meta_by_name(feaute_name)
#
# #print(feature_meta)
# print(json.dumps(feature_meta,indent=4))