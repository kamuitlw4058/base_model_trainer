#!/data/anaconda3/bin/python

import json
import os
import sys
from script.utils import run_cmd
from script.utils import getenv

from datetime import time
from script.emr import EMR
from datetime import datetime,timedelta


emr = EMR()
cluster_id = emr.active_cluster_id(datetime.today() - timedelta(days=2))
print(cluster_id)



create_script="""create-cluster.sh"""


argv_len=len(sys.argv)
if argv_len >= 2:
    print("create script:" + sys.argv[1])
    create_script=sys.argv[1]


ZAMPDA_HOME=getenv("ZAMPDA_HOME","/data/scripts/",True)

print("Start Create Cluster")
emr_cluster_cmd = """
aws emr list-clusters --active
"""






ZAMPDA_HOME=getenv("ZAMPDA_HOME")

emr_create_base_cmd = "create-emr-cluster.sh"


emr_list_clusters="""
%s/emr-list-clusters
"""%(ZAMPDA_HOME)


acitve_clusters = str(run_cmd(emr_list_clusters))
print("Active clusters:"+ acitve_clusters)


emr_cluster_id_cmd = """
cat %s/emr-cluster-id
"""%(ZAMPDA_HOME)

print(emr_cluster_id_cmd)
res = str(run_cmd(emr_cluster_id_cmd))
print("Last cluster id:"+ res)



if res in acitve_clusters:
    print("Cluster has startup!")
    sys.exit(0)

print("Start create cluster...")
print("Create cluster cmd:" + emr_create_cmd)

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

    emr_init_cmd="""nohup %s/emr-init.py &"""%(ZAMPDA_HOME)
    print(emr_init_cmd)
    os.system(emr_init_cmd)