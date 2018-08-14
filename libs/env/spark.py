#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf.spark import HDFS_SPARK_JARS, SPARK_CONFIG, SPARK_HOME, WORKER_PYTHON, DRIVER_PYTHON
from datetime import datetime
from conf.conf import JOB_ROOT_DIR
from conf import clickhouse


def provide_spark_session(func):
    def build_spark_session(*args,**kwargs):
        session = kwargs.get("session")
        if not session:
            job_id = "spark_session_" + datetime.now().isoformat()
            executor_num = 3
            local_dir =  os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, job_id)
            session = spark_session(job_id,executor_num,local_dir)
            logger.info("job_id:" + job_id)
            logger.info("executor_num:" + str(executor_num))
            logger.info("local_dir:" + str(local_dir))
            kwargs["session"] = session

        return func(*args,**kwargs)
    return build_spark_session




def spark_session(spark_id, executor_num, local_dir):
    logger.info('[%s] init spark session', spark_id)

    # spark
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = SPARK_HOME

    os.environ['PYSPARK_PYTHON'] = WORKER_PYTHON
    os.environ['PYSPARK_DRIVER_PYTHON'] = DRIVER_PYTHON

    if not local_dir:
        local_dir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, spark_id)

    spark_conf = SparkConf()
    conf_details = [
        # ('spark.yarn.jars', ''),
        # ('spark.executorEnv.PATH', SPARK_CONFIG['WORKER_PATH']),
        # ('spark.eventLog.dir', 'hdfs://TS-CLICKH011:8020/spark/history'),
        # ('spark.yarn.historyServer.address', 'http://ts-clickh09:18080/'),
        # ('spark.executorEnv.PATH', './python3/bin/:$PATH'),
        # ('spark.appMasterEnv.PATH', SparkConfig['WORKER_PATH']),
        # ('spark.yarn.appMasterEnv.PYSPARK_PYTHON', './python3/bin/python3'),
        # ('spark.executorEnv.PYSPARK_PYTHON', './python3/bin/python3'),
        # ('spark.driver.host', '172.22.16.57'),
        # ('spark.pyspark.python', './python3/bin/python3'),
        # ('spark.pyspark.python', './python3/bin/python3'),
        # ('spark.pyspark.driver.python', '/data/anaconda3/bin/python'),
        ('spark.yarn.archive', HDFS_SPARK_JARS),
        ('spark.yarn.dist.archives', SPARK_CONFIG['SPARK_ARCHIVES']),
        ('spark.eventLog.enabled', 'true'),
        ('spark.eventLog.compress', 'true'),
        ('spark.driver.memory', '2G'),
        ('spark.driver.extraJavaOptions', f'-Duser.timezone=UTC+0800 -Djava.io.tmpdir={os.path.join(local_dir, "tmp")} -Dderby.system.home={os.path.abspath(local_dir)}'),
        ('spark.executor.extraJavaOptions', '-Duser.timezone=UTC+0800 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps'),
        ('spark.executor.instances', executor_num),
        ('spark.executor.memory', '4G'),
        ('spark.executor.cores', 4),
        ('spark.sql.shuffle.partitions', executor_num),
        ('spark.sql.warehouse.dir', os.path.join(local_dir, 'metastore_db')),
        ('spark.local.dir', os.path.join(local_dir, 'tmp'))
    ]

    for k, v in conf_details:
        logger.info('[%s] spark config %s = %s', spark_id, k, v)

    spark_conf.setAll(conf_details)
    spark_conf.setAppName(f'{spark_id}')
    spark_conf.setMaster('yarn')

    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

    return spark

class SparkClickhouseReader:
    def __init__(self, spark,url):
        self._spark = spark
        self._url = url

    def read_sql_parallel(self,sql,repartition=None):
        df = self._spark.read.jdbc(self._url, sql, properties=clickhouse.CONF)
        if repartition:
            df = df.repartition(repartition)
        return df

    def read_sql(self,sql):
        df = self._spark.read.jdbc(self._url, sql, properties=clickhouse.ONE_HOST_CONF)
        return df
