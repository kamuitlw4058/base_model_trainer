#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)

import os
from conf.hadoop import JAVA_HOME, HADOOP_HOME

def init():
    for k, v in [
        ('JAVA_HOME', lambda: JAVA_HOME),
        ('HADOOP_HOME', lambda: HADOOP_HOME),
        ('HADOOP_CONF_DIR', lambda: os.path.join(os.environ['HADOOP_HOME'], 'etc', 'hadoop')),
        ('LD_LIBRARY_PATH', lambda: os.path.join(os.environ['HADOOP_HOME'], 'lib', 'native')),
        ('LIBHDFS_OPTS', lambda: '-Xmx48m'),
        ('HADOOP_USER_NAME', lambda: 'hadoop'),
    ]:
        if k not in os.environ:
            os.environ[k] = v()

    os.environ['PATH'] = f'{os.environ["HADOOP_HOME"]}/bin:{os.environ["PATH"]}'

    logger.info('success init hadoop environment.')
