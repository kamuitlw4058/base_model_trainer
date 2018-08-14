#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import os
from conf.hadoop import HDFS_CACHE_ROOT, HDFS_CODE_CACHE, PYTHON_ENV_CACHE


SPARK_HOME = '/data/tool/env/spark'
HDFS_SPARK_JARS = os.path.join(HDFS_CACHE_ROOT, 'spark-2.2.0.zip')
WORKER_PYTHON = './python3/bin/python3'
DRIVER_PYTHON = '/data/anaconda3/bin/python'

SPARK_CONFIG = dict(
    SPARK_ARCHIVES=','.join([
        f'{PYTHON_ENV_CACHE}#python3',
        f'{HDFS_CODE_CACHE}#libs'
    ]),
)
