#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import os
from conf.hadoop import HDFS_CACHE_ROOT, HDFS_CODE_CACHE, PYTHON_ENV_CACHE,HDFS_EXTNED_DATA_ROOT,HDFS_TEST_CACHE,get_hadoop_code_cache


SPARK_HOME = '/data/tool/env/spark'
HDFS_SPARK_JARS = os.path.join(HDFS_CACHE_ROOT, 'spark-2.2.0.zip')
WORKER_PYTHON = './python3/bin/python3'
DRIVER_PYTHON = '/data/anaconda3/bin/python'

SPARK_CONFIG = dict(
    SPARK_ARCHIVES=','.join([
        f'{PYTHON_ENV_CACHE}#python3',
        f'{HDFS_TEST_CACHE}#libs',
        f'{HDFS_EXTNED_DATA_ROOT}/image.zip/#image',
        f'{HDFS_EXTNED_DATA_ROOT}/weights.zip#weights',
    ]),
)



def get_spark_config(job_name):
    hadoop_code_cache =  get_hadoop_code_cache(job_name)
    return dict(
    SPARK_ARCHIVES=','.join([
        f'{PYTHON_ENV_CACHE}#python3',
        f'{hadoop_code_cache}#libs',
        f'{HDFS_EXTNED_DATA_ROOT}/image.zip/#image',
        f'{HDFS_EXTNED_DATA_ROOT}/weights.zip#weights',
    ]),
)