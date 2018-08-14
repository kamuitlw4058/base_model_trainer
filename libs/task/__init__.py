#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import os
import shutil
from libs.env.hdfs import hdfs


def clean_task_dir(runtime):
    if hdfs.exists(runtime.hdfs_dir):
        hdfs.rm(runtime.hdfs_dir)

    if os.path.exists(runtime.local_dir):
        shutil.rmtree(runtime.local_dir)


def init_task_dir(runtime):
    clean_task_dir(runtime)
    os.makedirs(runtime.local_dir)


def init_hdfs_dir(hdfs_dir,clean_old = True):
    if hdfs.exists(hdfs_dir) and clean_old:
        hdfs.rm(hdfs_dir)


def init_local_dir(local_dir,clean_old = True):
    if os.path.exists(local_dir) and clean_old:
        shutil.rmtree(local_dir)

    os.makedirs(local_dir)

def init_task_dir(local_dir,hdfs_dir):
    init_local_dir(local_dir)
    init_hdfs_dir(hdfs_dir)




def get_worker_num(data_size):
    if data_size < 30_0000:
        return 1
    elif data_size < 100_0000:
        return 2
    elif data_size < 500_0000:
        return 4
    elif data_size < 1000_0000:
        return 8
    elif data_size < 4000_0000:
        return 16
    else:
        return 64

