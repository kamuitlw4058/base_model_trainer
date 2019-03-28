import re


def get_simple_str(**kwargs):
    output_str = ""
    for k, v in kwargs.items():
        output_str += f"_{k}{v}"

    return output_str


def get_simple_str_by_template(str_template:str, not_format=[],**kwargs):
    return get_simple_str_by_template([str_template],not_format,**kwargs)

def get_simple_str_by_template(str_template_list:list, not_format=[],**kwargs):
    r = r"(?<=\{)[^}]*(?=\})"
    output_str=""
    total_key_list = []
    for s in  str_template_list:
        key_list = re.findall(r, s)
        total_key_list += key_list

    total_key_list = list(set(total_key_list))
    total_key_list.sort()
    for k  in total_key_list:
        key_split = str(k).split(":")
        if len(key_split)> 1:
            k = key_split[0]
        if k not in not_format:
            output_str += f"_{k}{kwargs.get(k,'')}"

    return output_str

from functools import reduce

#
# list = [{'imp': 'onehot', 'col_name': 'Device_OsVersion'},{'imp': 'onehot', 'col_name': 'Device_OsVersion'}, {'imp': 'onehot', 'col_name': 'Device_Network'}, {'imp': 'onehot', 'col_name': 'Age'}, {'imp': 'onehot', 'col_name': 'Gender'}, {'imp': 'onehot', 'col_name': 'Education'}, {'imp': 'onehot', 'col_name': 'Profession'}, {'imp': 'onehot', 'col_name': 'ConsumptionLevel'}, {'imp': 'onehot', 'col_name': 'Adb_Device_Platform'}, {'imp': 'onehot', 'col_name': 'Adb_Device_Type'}, {'imp': 'onehot', 'col_name': 'Adb_Device_Brand'}, {'imp': 'onehot', 'col_name': 'Adb_Device_Model'}, {'imp': 'onehot', 'col_name': 'Adb_Device_PriceLevel'}, {'imp': 'onehot', 'col_name': 'Time_Hour'}, {'imp': 'onehot', 'col_name': 'weekday'}, {'imp': 'onehot', 'col_name': 'Slot_Id'}, {'imp': 'onehot', 'col_name': 'Slot_Type'}, {'imp': 'onehot', 'col_name': 'Media_Domain'}, {'imp': 'onehot', 'col_name': 'geo_county'}, {'imp': 'onehot', 'col_name': 'geo_city'}, {'imp': 'onehot', 'col_name': 'geo_province'}, {'imp': 'onehot', 'col_name': 'cap_PayAction'}, {'imp': 'onehot', 'col_name': 'cap_House'}, {'imp': 'onehot', 'col_name': 'cap_Car'}, {'imp': 'onehot', 'col_name': 'cap_CPI'}, {'imp': 'onehot', 'col_name': 'a20_v24_last30_imp'}, {'imp': 'onehot', 'col_name': 'a20_v24_last30_clk'}, {'imp': 'onehot', 'col_name': 'a20_v24_last30_ctr'}]
# print(len(list))
def list_dict_duplicate_removal(data_list):
    run_function = lambda x, y: x if y in x else x + [y]
    return reduce(run_function, [[], ] + data_list)
# print(len(list_dict_duplicate_removal(list)))




