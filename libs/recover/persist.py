def parse_desc(file_name):
    f = open(file_name, 'r', encoding='utf-8')
    info = {}
    for line in f:
        k, v = line.split('\t')
        info[k.strip()] = v.strip()
    return info

