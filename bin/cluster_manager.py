import sys
sys.path.append('.')

from datetime import datetime, timedelta




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

def emr():
    from libs.env.shell import run_cmd
    from script.emr import EMR

    emr = EMR()
    cluster_id = emr.active_cluster_id(cluster_name=None)
    if not  cluster_id:
        print("hasn't active cluster...")
        sys.exit(-1)
    print(cluster_id)

    # print(f'cluster id = {cluster_id}')
    #
    # master_ip = emr.master_public_ip(cluster_id)
    # print(f'master host = {master_ip}')
    #
    # from conf.hadoop import HADOOP_HOME
    # from conf.spark import SPARK_HOME
    #
    # for i in ['core-site.xml', 'hdfs-site.xml', 'mapred-site.xml', 'yarn-site.xml']:
    #     cmd = ' '.join([
    #         'scp -o StrictHostKeyChecking=no -i /home/ubuntu/emr-key.txt',
    #         f'-r hadoop@{master_ip}:/usr/lib/hadoop/etc/hadoop/{i}',
    #         f'{HADOOP_HOME}/etc/hadoop/{i}',
    #     ])
    #     run_cmd(cmd)
    #
    # for i in ['spark-defaults.conf', 'spark-env.sh']:
    #     cmd = ' '.join([
    #         'scp -o StrictHostKeyChecking=no -i /home/ubuntu/emr-key.txt',
    #         f'-r hadoop@{master_ip}:/usr/lib/spark/conf/{i}',
    #         f'{SPARK_HOME}/conf/{i}',
    #     ])
    #     run_cmd(cmd)
    #
    # # set SPARK_PUBLIC_DNS in spark-env.sh for spark Web UI
    # with open(f'{SPARK_HOME}/conf/spark-env.sh', 'r') as f:
    #     content = [line for line in f]
    #
    # with open(f'{SPARK_HOME}/conf/spark-env.sh', 'w') as f:
    #     for line in content:
    #         if line.startswith('export SPARK_PUBLIC_DNS='):
    #             f.write('export SPARK_PUBLIC_DNS=`hostname -i`\n')
    #         else:
    #             f.write(line)
        #f.write(f"{}")

emr()