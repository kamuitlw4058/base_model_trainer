from zamplus_common.utils.sys_cmd import exec_sync
from zamplus_common.conf.clickhouse import CH_HOSTS
from datetime import datetime
import random
import os

clickhouse_cmd_template = 'clickhouse-client -h {} -q "{}" > {}'


def get_host():
    return random.choice(CH_HOSTS)



def cmd_sql(sql_str,read_output=False,name=None,delete_output=True):
    if name is None:
        output_file_path = "sql_result_{ts:%y%m%d_%H%M%S}.txt".format(ts=datetime.now())
    else:
        output_file_path = "{name}_sql_result_{ts:%y%m%d_%H%M%S}.txt".format(name=name,ts=datetime.now())
    result =None
    run_cmd = clickhouse_cmd_template.format(get_host(),sql_str,output_file_path)
    status,logs = exec_sync(run_cmd)
    for log in logs:
        print(log)
    if read_output:
        try:
            f = open(output_file_path, 'r')
            result =  f.read().splitlines()

        finally:
            if f:
                f.close()
                if delete_output:
                    os.remove(output_file_path)
    if delete_output:
        return  result
    else:
        return result,delete_output




