#!/data/anaconda3/bin/python

import json
import os
import sys
from script.utils import run_cmd
from script.utils import getenv
from libs.env.hdfs import  hdfs

from datetime import time
from script.emr import EMR
from datetime import datetime,timedelta

import os
from libs.env.hadoop.init import init
init()

#
# emr = EMR()
# cluster_id = emr.active_cluster_id(datetime.today() - timedelta(days=2))
# print(cluster_id)
#
#
#
# create_script="""create-cluster.sh"""
#
#
# argv_len=len(sys.argv)
# if argv_len >= 2:
#     print("create script:" + sys.argv[1])
#     create_script=sys.argv[1]
#
#
# ZAMPDA_HOME=getenv("ZAMPDA_HOME","/data/scripts/",True)
#
# print("Start Create Cluster")
# emr_cluster_cmd = """
# aws emr list-clusters --active
# """
#
#
#
#
#
#
# ZAMPDA_HOME=getenv("ZAMPDA_HOME")
#
# emr_create_base_cmd = "create-emr-cluster.sh"
#
#
# emr_list_clusters="""
# %s/emr-list-clusters
# """%(ZAMPDA_HOME)
#
#
# acitve_clusters = str(run_cmd(emr_list_clusters))
# print("Active clusters:"+ acitve_clusters)
#
#
# emr_cluster_id_cmd = """
# cat %s/emr-cluster-id
# """%(ZAMPDA_HOME)
#
# print(emr_cluster_id_cmd)
# res = str(run_cmd(emr_cluster_id_cmd))
# print("Last cluster id:"+ res)
#
#
#
# if res in acitve_clusters:
#     print("Cluster has startup!")
#     sys.exit(0)

print("Start create cluster...")
#print("Create cluster cmd:" + emr_create_cmd)

def write_cluster_id(cluster_name,cluster_id):
    cluster_id_base = "/data/scripts/"
    f=  open(cluster_id_base + cluster_name, "w")
    f.write(cluster_id)
    f.close()


def create_cluster(cluster_name,worker_num):
    emr_create_cmd =  f"{emr_create_base_cmd} {cluster_name} {worker_num}"
    res = str(run_cmd(emr_create_cmd))
    obj = json.loads(res.replace('\n',''))
    cluster_id = obj["ClusterId"]
    print("cluster:"+cluster_id)
    write_cluster_id(cluster_name=cluster_name,cluster_id=cluster_id)

    # emr_init_cmd="""nohup %s/emr-init.py &"""%(ZAMPDA_HOME)
    # print(emr_init_cmd)
    # os.system(emr_init_cmd)


def upload_extend_data_dir(base_path,hdfs_base_path):

    if os.path.isdir(base_path):
        base_dir_name = os.path.basename(base_path)
        path_list = os.listdir(base_path)
        for path in path_list:
            local_path = os.path.join(base_path, path)
            hdfs_dir_path = os.path.join(hdfs_base_path, base_dir_name)
            if os.path.isfile(os.path.join(base_path, path)):
                if not hdfs.exists(hdfs_dir_path):
                    hdfs.mkdir(hdfs_dir_path)
                hdfs.put(local_path, hdfs_dir_path)
                print(f"put file: {local_path} to {hdfs_dir_path}")
            else:
                upload_extend_data_dir(local_path,hdfs_dir_path)
    else:
        print(f"base path is not dir:{base_path}")


HDFS_BASE_DATA_ROOT= 'hdfs:///user/model/'
def upload_extend_data(base_path):
    hdfs_base_path = HDFS_BASE_DATA_ROOT
    if os.path.isdir(base_path):
        path_list = os.listdir(base_path)
        for path in path_list:
            local_path = os.path.join(base_path, path)
            if os.path.isdir(local_path):
                upload_extend_data_dir(local_path,hdfs_base_path)
    else:
        print(f"base path is not dir:{base_path}")



upload_extend_data("/data/tool/env/hadoop_data/")