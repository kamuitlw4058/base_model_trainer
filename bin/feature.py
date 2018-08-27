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

from conf import conf
from datetime import datetime

import libs.env.hadoop


import argparse
from concurrent.futures import ProcessPoolExecutor
from libs.job.job_parser import parser as job_parser
from libs.task.task_runner import run_job
from libs.process_tool import LockMe
from libs.pack import pack_libs
from libs.task.feature_task import run as feature_run


def feature_tasks(options):
    locker = LockMe()
    try:
        if locker.try_lock():
            pack_libs(False)
            jobs = job_parser(options)

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
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-a', '--account', dest='account', type=int,  default=None, help='the account (eg: 12 )')
    args_parser.add_argument('-v', '--vendor', dest='vendor', type=int,default=None, help='the vendor (eg: 24 )')
    args_parser.add_argument('-n', '--feature_name', dest='feature_name', required=True,type=str, help='the name (eg. account_vender_last30_ctr)')
    args_parser.add_argument('-j', '--job_name', dest='job_name', type=str, help='the name (eg. job_123)')
    args_parser.add_argument('-f', '--filter-file', dest='filters', type=str,required=True,help=' filter file path list of sql(eg: ')
    args_parser.add_argument('-c', '--conf_file', dest='conf_file', type=str, help='job conf file path')
    args_parser.add_argument('-P', '--pos_proportion', dest='pos_proportion', type=int, default=1)
    args_parser.add_argument('-N', '--neg_proportion', dest='neg_proportion', type=int, default=2)
    args_parser.add_argument('-s', '--train-start-date', dest='start_date', type=str, default=datetime.now().strftime("%Y-%m-%d"))
    args_parser.add_argument('-e', '--train-end-date', dest='end_date', type=str, default=None)
    args_parser.add_argument('-S', '--test-start-date', dest='test_start_date', type=str, default=None)
    args_parser.add_argument('-E', '--test-end-date', dest='test_end_date', type=str, default=None)
    args_parser.add_argument('-d', '--debug-pycharm', dest='pycharm', action="store_true", help='pycharm runner')
    args_parser.add_argument('-F', '--use-new-feature', dest='new_features', type=str,  help='new feature path',default=None)
    args_parser.add_argument('-l', '--learning_rate', dest='learning_rate',  type=int, default=0.001,help='learning_rate')
    args_parser.add_argument('-L', '--l2', dest='l2',  type=int,default=0.001,help='l2')
    options = args_parser.parse_args()

    if options.pycharm:
        logger.info("use pycharm mode")
        conf.PYCHARM = True

    feature_tasks(options)


    logger.info('finish all jobs')
