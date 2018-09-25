#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import os
from conf.conf import JOB_ROOT_DIR


JAVA_HOME = '/data/tool/jdk1.8.0_171'
HADOOP_HOME = '/data/tool/env/hadoop-2.7.3'
HADOOP_LZO_HOME = '/data/tool/env/hadoop-lzo'
HADOOP_CONF_DIR = os.path.join(HADOOP_HOME, 'etc', 'hadoop')

EXTNED_DATA_DIR='extend_data'

HDFS_CACHE_ROOT = 'hdfs:///user/model/env'
PYTHON_ENV_CACHE = os.path.join(HDFS_CACHE_ROOT, 'python3.zip')
HDFS_CODE_CACHE = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, 'libs.zip')
HDFS_FEATURE_ROOT= 'hdfs:///user/model/feature'
HDFS_EXTNED_DATA_ROOT= 'hdfs:///user/model/extend_data'