#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'kimi'
__email__ = 'kimi@zamplus.com'


import sys
sys.path.append('.')

# prepare logging
import os
if not os.path.exists('./log'):
    os.mkdir('./log')

import json
import logging.config
logging.config.dictConfig(json.load(open('conf/logging.json')))
logger = logging.getLogger(__name__)



import libs.env.hadoop


import argparse
from concurrent.futures import ProcessPoolExecutor
from libs.job.job_parser import parser
from libs.task.task_runner import run_job
from libs.process_tool import LockMe
from libs.pack import pack_libs
from libs.task.feature_task import run as feature_run


def feature_tasks(options):
    locker = LockMe()
    try:
        if locker.try_lock():
            pack_libs()
            jobs = parser(options)

            with ProcessPoolExecutor(max_workers=1) as executor:
                executor.map(run_job, [(feature_run, job) for job in jobs])

            logger.info('finish all tasks.')
    except Exception as e:
        logger.exception(e)
    finally:
        locker.unlock()



if __name__ == '__main__':
    logger.info('begin to feature process.')

    from conf.conf import JOB_ROOT_DIR
    if not os.path.exists(JOB_ROOT_DIR.LOCAL_ROOT):
        os.makedirs(JOB_ROOT_DIR.LOCAL_ROOT)


    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--account', dest='account', type=int, required=True,default=12,help='the account (eg: 12 )')
    parser.add_argument('-v', '--vendor', dest='vendor', type=int, default=24,help='the vendor (eg: 24 )')
    parser.add_argument('-n', '--feature_name', dest='feature_name', type=str, help='the name (eg. account_vender_last30_ctr)')
    parser.add_argument('-f', '--filters', dest='filters', type=str, help='the filters sql select from clickhouse ')
    parser.add_argument('-c', '--conf_file', dest='conf_file', type=str, help='job conf file path')
    options = parser.parse_args()

    feature_tasks(options)


    logger.info('finish all jobs')
