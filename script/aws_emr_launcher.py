import sys
sys.path.append('.')

from datetime import datetime, timedelta


def update_hadoop_conf():
    from libs.env.shell import run_cmd
    from script.emr import EMR

    emr = EMR()
    cluster_id = emr.active_cluster_id(datetime.today()-timedelta(days=2))
    if not  cluster_id:
        print("hasn't active cluster...")
        return

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


if __name__ == '__main__':
    update_hadoop_conf()

    import libs.env.hadoop
    from libs.env.hdfs import hdfs
    from conf.hadoop import PYTHON_ENV_CACHE, HDFS_CACHE_ROOT
    from conf.spark import HDFS_SPARK_JARS

    hdfs.mkdir(HDFS_CACHE_ROOT)

    hdfs.put('/data/tool/python3.zip', PYTHON_ENV_CACHE)
    hdfs.put('/data/tool/env/spark-2.2.0.zip', HDFS_SPARK_JARS)

    for l in hdfs.ls(HDFS_CACHE_ROOT):
        print(l)
