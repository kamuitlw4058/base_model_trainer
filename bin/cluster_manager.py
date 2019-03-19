import sys
sys.path.append('.')

from datetime import datetime, timedelta
from libs.env.shell import run_cmd
from script.emr import EMR
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager


def update_hadoop_conf(cluster_name=None):
    from libs.env.shell import run_cmd
    from script.emr import EMR

    emr = EMR()
    cluster_id = emr.active_cluster_id(cluster_name=cluster_name)
    if not  cluster_id:
        print("hasn't active cluster...")
        return None

    print(f'cluster id = {cluster_id}')

    master_ip = emr.master_public_ip(cluster_id)
    print(f'master host = {master_ip}')

    from conf.hadoop import HADOOP_HOME
    from conf.spark import SPARK_HOME

    for i in ['core-site.xml', 'hdfs-site.xml', 'mapred-site.xml', 'yarn-site.xml']:
        cmd = ' '.join([
            'scp -o StrictHostKeyChecking=no -i /home/ubuntu/emr-key.txt',
            f'-r hadoop@{master_ip}:/usr/lib/hadoop/etc/hadoop/{i}',
            f'{HADOOP_HOME}/etc/hadoop/{i}',
        ])
        run_cmd(cmd)

    for i in ['spark-defaults.conf', 'spark-env.sh']:
        cmd = ' '.join([
            'scp -o StrictHostKeyChecking=no -i /home/ubuntu/emr-key.txt',
            f'-r hadoop@{master_ip}:/usr/lib/spark/conf/{i}',
            f'{SPARK_HOME}/conf/{i}',
        ])
        run_cmd(cmd)

    # set SPARK_PUBLIC_DNS in spark-env.sh for spark Web UI
    with open(f'{SPARK_HOME}/conf/spark-env.sh', 'r') as f:
        content = [line for line in f]

    with open(f'{SPARK_HOME}/conf/spark-env.sh', 'w') as f:
        for line in content:
            if line.startswith('export SPARK_PUBLIC_DNS='):
                f.write('export SPARK_PUBLIC_DNS=`hostname -i`\n')
            else:
                f.write(line)

    return cluster_id


def create_cluster(cluster_name=None):
    pass

cluster_active_dict = {}

def is_active_cluster(cluster_name="zampda_model_test"):
    emr = EMR()
    cluster_id = emr.active_cluster_id(cluster_name=cluster_name)
    if not  cluster_id:
        print("hasn't active cluster...")
        return False
    print(f"active cluster_id: {cluster_id}")

    master_ip = emr.master_public_ip(cluster_id)
    print(f'master host = {master_ip}')

    running_job = get_yarn_job_by_state(master_ip)
    if len(running_job) > 0:
        return True

    return False



def get_yarn_job_by_state(master_ip,not_state=['finished','failed','killed'], state=None)->list:
    ret = []
    rm = ResourceManager(address=master_ip, port=8088)
    app_list = rm.cluster_applications().data
    for app in app_list['apps']["app"]:
        if not_state is not None:
            if str(app['state']).lower() not in not_state:
                ret.append(app)
        elif state is not None:
            if str(app['state']).lower()  in state:
                ret.append(app)
    return ret


def check_cluster(cluster_name="zampda_model_test"):
    active = is_active_cluster(cluster_name=cluster_name)
    if active:
        cluster_active_dict[cluster_name] = datetime.now()
        print("cluster is active")
    else:
        active_time = cluster_active_dict.get(cluster_name)
        if active_time is None:
            cluster_active_dict[cluster_name] = datetime.strptime("2017-1-1", "%Y-%m-%d")
        print("cluster is not active")



def check_cluster_timeout():
    for k,v in cluster_active_dict.items():
        if (datetime.now() - v).seconds > 60 * 60:
            print(f"{datetime.now()}->cluster[{k}] timeout we will close it")
        else:
            print(f"{datetime.now()}->cluster[{k}] is active. active time:{(datetime.now() - v).seconds/60}m")

def main():
    check_cluster()
    check_cluster_timeout()

main()




