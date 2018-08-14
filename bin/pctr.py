#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


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
from libs.job.job_manager import gen_jobs
from libs.task.task_runner import run
from libs.recover.bring_back import bring_back
from libs.process_tool import LockMe
from libs.pack import pack_libs


def recover_mode(options):
    job_id = options.job_id
    version = options.version

    if job_id and version:
        bring_back(job_id, version)
    else:
        logging.error('please input -id and -version')


def training_tasks(options):
    locker = LockMe()
    try:
        if options.mode == 'overwrite' or locker.try_lock():
            pack_libs()
            all_tasks = gen_jobs(options.mode)
            logger.info('total tasks %d', len(all_tasks))

            with ProcessPoolExecutor(max_workers=6) as executor:
                executor.map(run, [(job, options) for job in all_tasks])

            logger.info('finish all tasks.')
    except Exception as e:
        logger.exception(e)
    finally:
        if options.mode != 'overwrite':
            locker.unlock()




def str2bool(v):
    v1 = v.lower()
    if v1 in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v1 in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f'Boolean value expected, but got "{v}"')


if __name__ == '__main__':
    logger.info('begin to train models.')

    from conf.conf import JOB_ROOT_DIR
    if not os.path.exists(JOB_ROOT_DIR.LOCAL_ROOT):
        os.makedirs(JOB_ROOT_DIR.LOCAL_ROOT)

    cmd = {
        'recover': recover_mode,
        'merge': training_tasks,
        'manual': training_tasks,
        'feature': feature_tasks,
     }

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', dest='mode', type=str, choices=list(cmd.keys()), required=True, help='select running mode')
    parser.add_argument('-a', '--account', dest='account', type=int, default=12,help='the account (eg: 12 )')
    parser.add_argument('-V', '--vendor', dest='vendor', type=int, default=24,help='the vendor (eg: 24 )')
    parser.add_argument('-i', '--job_id', dest='job_id', type=str, help='the job id (eg. 109_24_2_1)')
    parser.add_argument('-v', '--version', dest='version', type=str, help='the model version (eg. 2017-11-07T16:34)')
    parser.add_argument('-s', '--send', dest='send', type=str2bool, default=False, help='send to ftp server')
    parser.add_argument('-d', '--debug', dest='debug', type=str2bool, default=False, help='debug mode, do not commit log and delete model files')
    options = parser.parse_args()

    cmd[options.mode](options)

    logger.info('finish all jobs')
