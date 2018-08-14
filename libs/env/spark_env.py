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


def get_executor_num(estimated_samples):
    if estimated_samples < 40 * 10000:
        return 8
    elif estimated_samples < 80 * 10000:
        return 16
    elif estimated_samples < 160 * 10000:
        return 32
    elif estimated_samples < 320 * 10000:
        return 48
    else:
        return 64


def init_spark(job_id, estimated_samples, runtime):
    logger.info('[%s] init spark session', job_id)

    # spark
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = SPARK_HOME

    os.environ['PYSPARK_PYTHON'] = WORKER_PYTHON
    os.environ['PYSPARK_DRIVER_PYTHON'] = DRIVER_PYTHON

    executor_num = get_executor_num(estimated_samples)
    runtime.executor_num = executor_num

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
        ('spark.driver.extraJavaOptions', f'-Duser.timezone=UTC+0800 -Djava.io.tmpdir={os.path.join(runtime.local_dir, "tmp")} -Dderby.system.home={os.path.abspath(runtime.local_dir)}'),
        ('spark.executor.extraJavaOptions', '-Duser.timezone=UTC+0800 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps'),
        ('spark.executor.instances', executor_num),
        ('spark.executor.memory', '4G'),
        ('spark.executor.cores', 4),
        ('spark.sql.shuffle.partitions', executor_num),
        ('spark.sql.warehouse.dir', os.path.join(runtime.local_dir, 'metastore_db')),
        ('spark.local.dir', os.path.join(runtime.local_dir, 'tmp'))
    ]

    for k, v in conf_details:
        logger.info('[%s] spark config %s = %s', job_id, k, v)

    spark_conf.setAll(conf_details)
    spark_conf.setAppName(f'FE-{job_id}')
    spark_conf.setMaster('yarn')

    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

    return spark
