import re  #python的正则表达式模块



def parse_str(input_str,re_str,output_types,list_split=','):
    result_list = []
    re_result =  re.search(re_str, input_str).groups()
    i = 0
    for t in output_types:
        if isinstance(t, tuple):
            value_str_list = str(re_result[i]).split(list_split)
            value_list =[]
            for v in value_str_list:
                if v.strip():
                    value_list.append(t[1](v))
            result_list.append(value_list)
        else:
            result_list.append(t(re_result[i]))
        i+= 1
    return result_list


