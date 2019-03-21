#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from   conf.clickhouse import hosts
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

RTB_ALL_TABLE_NAME = 'rtb_all'

RTB_LOCAL_TABLE_NAME = 'rtb_local2'

CLICKHOUSE_URL_TEMPLATE = 'clickhouse://{}/{}'


ZAMPLUS_ZAMPDA_DATABASE = 'zampda'

ZAMPLUS_ZAMPDA_LOCAL_DATABASE = 'zampda_local'

ZAMPLUS_RTB_ALL_URL = f'clickhouse://{random.choice(hosts)}/{ZAMPLUS_ZAMPDA_DATABASE}'

ZAMPLUS_RTB_ALL_JDBC_URL = f'jdbc:clickhouse://{random.choice(hosts)}/{ZAMPLUS_ZAMPDA_DATABASE}'
ZAMPLUS_RTB_LOCAL_JDBC_URL = f'jdbc:clickhouse://{random.choice(hosts)}/{ZAMPLUS_ZAMPDA_LOCAL_DATABASE}'

CLICKHOUSE_DAILY_SQL_DATE_COL='target_day'


PYCHARM = False

IMAGES_OUTPUT_BASE_DIR='cache/images/'

FEATURES_NOT_FORMAT_LIST =['train_end_date','test_start_date','test_end_date','train_start_date']





class JOB_ROOT_DIR:
    HDFS_ROOT = f'hdfs:///user/{os.environ.get("CTR_USER_NAME", "test")}/pctr_feature'
    LOCAL_ROOT = os.path.abspath('./cache/jobs')


