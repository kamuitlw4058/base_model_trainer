#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import os
from datetime import timedelta

_ftp_address = '172.22.41.102::brain01'

FTP_BACKUP_DIR = f'{_ftp_address}/pctr_backup'
FTP_DIR = f'{_ftp_address}/pctr'

MAX_SAMPLE_DAY = 30
CLK_LIMIT = 33 * MAX_SAMPLE_DAY
IMP_LIMIT = CLK_LIMIT * 1.1

MAX_POS_SAMPLE = 666 * 10e3
MIN_POS_SAMPLE = 40 * 10e3
WINDOW = timedelta(days=MAX_SAMPLE_DAY)

CURRENT_WORK_DIR = os.getcwd()
PACKAGE_NAME = os.path.basename(CURRENT_WORK_DIR)

JOB_FILE_NAME = 'job.json'

RTB_ALL_TABLE_NAME = 'rtb_all2'

CLICKHOUSE_URL_TEMPLATE = 'clickhouse://{}/zampda'

PYCHARM = False


class JOB_ROOT_DIR:
    HDFS_ROOT = f'hdfs:///user/{os.environ.get("CTR_USER_NAME", "test")}/pctr_tflr'
    LOCAL_ROOT = os.path.abspath('./cache')
