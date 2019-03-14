import re


def get_simple_str(**kwargs):
    output_str = ""
    for k, v in kwargs.items():
        output_str += f"_{k}{v}"

    return output_str


def get_simple_str_by_template(str_template:str, **kwargs):
    r = r"(?<=\{)[^}]*(?=\})"
    output_str=""
    key_list = re.findall(r, str_template)
    key_list = list(set(key_list))

    for k  in key_list:
        key_split = str(k).split(":")
        if len(key_split)> 1:
            k = key_split[0]
        output_str += f"_{k}{kwargs.get(k,'')}"

    return output_str

def get_simple_str_by_template(str_template_list:list, **kwargs):
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
        output_str += f"_{k}{kwargs.get(k,'')}"

    return output_str

#print(get_simple_str_by_template("123l{123}sadflkj{aa}a{ccd}"))