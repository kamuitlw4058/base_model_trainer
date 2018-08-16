#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import os
# import pyarrow as pa
from libs.env.hdfs import hdfs
import numpy as np
from libs.model.linear_model import LogisticRegression
from conf.conf import CURRENT_WORK_DIR
from conf.hadoop import HDFS_CODE_CACHE
from conf.spark import PYTHON_ENV_CACHE, WORKER_PYTHON
from conf import xlearning
from libs.env.shell import run_cmd
from libs.env.hdfs import hdfs
from libs.model.trainer.trainer import Trainer

_training_log_dir = 'eventlog'


class TFLRTrainer(Trainer):

    def __init__(self, job_id,hdfs_dir,local_dir,learning_rate,l2):
        self._job_id = job_id
        self._hdfs_dir = hdfs_dir
        self._local_dir = local_dir
        self._learning_rate = learning_rate
        self._l2 = l2


    @staticmethod
    def get_model_name():
        return "tflr"

    def train(self, epoch,batch_size,worker_num,input_dim, data_name):
        logger.info('[%s] start train...', self._job_id)

        self._xlearning_submit(data_name)

        hdfs_path = os.path.join(self._hdfs_dir, self.get_model_name())
        local_ckpt_dir = os.path.join(self._local_dir, self.get_model_name())
        hdfs.download_checkpoint(hdfs_path, local_ckpt_dir)

        lr = LogisticRegression(input_dim=input_dim)
        lr.from_checkpoint(local_ckpt_dir)
        return lr

    @staticmethod
    def get_worker_entrance():
        from libs.model.trainer.tflr.worker import main_fun_name
        main_file = os.path.relpath(main_fun_name(), CURRENT_WORK_DIR)
        logger.info('worker file: %s', main_file)
        return main_file

    def _xlearning_submit(self, epoch,batch_size,worker_num,input_dim,data_name):
        output_path = os.path.join(self._hdfs_dir, self.get_model_name())
        if hdfs.exists(output_path):
            hdfs.rm(output_path)

        entrance = self.get_worker_entrance()
        logger.info('[%s] start xlearning submit ...', self._job_id)

        worker_cmd = ' '.join([
            f'{WORKER_PYTHON} {entrance}',
            f'--job_id={self._job_id}',
            f'--hdfs_dir={self._hdfs_dir}',
            f'--data={data_name}',
            f'--model={self.get_model_name()}',
            f'--log_dir={_training_log_dir}',
            f'--training_epochs={epoch}',
            f'--input_dim={input_dim}',
            f'--learning_rate={self._learning_rate}',
            f'--batch_size={batch_size}',
            f'--l2={self._l2}',
        ])

        driver_cmd = ' '.join([
            f'{xlearning.XL_SUBMIT}',
            f'--app-type "tensorflow"',
            f'--app-name "CTR-{self._job_id}"',
            f'--launch-cmd "{worker_cmd}"',
            f'--input {self._hdfs_dir}/{data_name}#{data_name}',
            f'--output {self._hdfs_dir}/{self.get_model_name()}#{self.get_model_name()}',
            f'--board-logdir {_training_log_dir}',
            f'--cacheArchive {HDFS_CODE_CACHE}#libs,{PYTHON_ENV_CACHE}#python3',
            f'--worker-memory {xlearning.WORKER_MEMORY}',
            f'--worker-num {worker_num}',
            f'--worker-cores {xlearning.WORKER_CORES}',
            f'--ps-memory {xlearning.PS_MEMORY}',
            f'--ps-num {xlearning.PS_NUM}',
            f'--ps-cores {xlearning.PS_CORES}',
            f'--queue default',
            f'--user-path ./python3/bin',
            f'--jars {xlearning.JARS}',
            # '-Duser.timezone=UTC+0800',
            ])
        logger.info(driver_cmd)

        run_cmd(driver_cmd)
        logger.info('finish training process successful.')

