#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import os
from libs.model.linear_model import LogisticRegression
from conf.conf import CURRENT_WORK_DIR
from conf.hadoop import HDFS_CODE_CACHE
from conf.spark import PYTHON_ENV_CACHE, WORKER_PYTHON
from conf import xlearning
from libs.env.shell import run_cmd
from libs.env.hdfs import hdfs

TRAINING_LOG_DIR = 'eventlog'


class Trainer:
    def __init__(self,**kwargs):
        self._job_id = kwargs.get("job_id")
        self._model_name = kwargs.get("model_name")
        self._data_file = kwargs.get("data_file")
        self._hdfs_dir = kwargs.get("hdfs_dir")
        self._local_dir = kwargs.get("local_dir")
        self._input_dim = kwargs.get("input_dim")
        self._worknum = kwargs.get("worknum")
        self._epoch = kwargs.get("epoch")
        self._batch_size = kwargs.get("batch_size")
        self._learning_rate = kwargs.get("learning_rate")
        self._training_log_dir = kwargs.get("training_log_dir")
        self._l2 =kwargs.get("l2")

    def __init__(self, job_id, model_name,
                 data_file,hdfs_dir,
                 local_dir,input_dim,
                 worknum,
                 learning_rate,
                 l2,
                 batch_size = 32,
                 epoch = 10,
                 training_log_dir = TRAINING_LOG_DIR):
        self._job_id = job_id
        self._model_name = model_name
        self._data_file = data_file
        self._hdfs_dir = hdfs_dir
        self._local_dir = local_dir
        self._input_dim = input_dim
        self._worknum = worknum
        self._epoch = epoch
        self._batch_size = batch_size
        self._learning_rate = learning_rate
        self._training_log_dir = training_log_dir
        self._l2 =l2


    def train(self):
        logger.info('[%s] train conf: %s', self._job_id, self._model_name)
        self._xlearning_submit()
        hdfs_path = os.path.join(self._hdfs_dir, self._model_name)
        local_ckpt_dir = os.path.join(self._local_dir, self._model_name)
        hdfs.download_checkpoint(hdfs_path, local_ckpt_dir)

        lr = LogisticRegression(input_dim=self._input_dim)
        lr.from_checkpoint(local_ckpt_dir)
        return lr

    @staticmethod
    def get_worker_entrance():
        from libs.distributed.train.worker import main_fun_name
        main_file = os.path.relpath(main_fun_name(), CURRENT_WORK_DIR)
        logger.info('worker file: %s', main_file)
        return main_file

    def _xlearning_submit(self):
        output_path = os.path.join(self._hdfs_dir, self._model_name)
        if hdfs.exists(output_path):
            hdfs.rm(output_path)

        entrance = self.get_worker_entrance()
        logger.info('model conf: %s', self._model_name)



        worker_cmd = ' '.join([
            f'{WORKER_PYTHON} {entrance}',
            f'--job_id={self._job_id}',
            f'--hdfs_dir={self._hdfs_dir}',
            f'--data={self._data_file}',
            f'--model={self._model_name}',
            f'--log_dir={self._training_log_dir}',
            f'--training_epochs={self._epoch}',
            f'--input_dim={self._input_dim}',
            f'--learning_rate={self._learning_rate}',
            f'--batch_size={self._batch_size}',
            f'--l2={self._l2}',
        ])

        driver_cmd = ' '.join([
            f'{xlearning.XL_SUBMIT}',
            f'--app-type "tensorflow"',
            f'--app-name "{self._job_id}"',
            f'--launch-cmd "{worker_cmd}"',
            f'--input {self._hdfs_dir}/{self._data_file}#{self._data_file}',
            f'--output {self._hdfs_dir}/{self._model_name}#{self._model_name}',
            f'--board-logdir {self._training_log_dir}',
            f'--cacheArchive {HDFS_CODE_CACHE}#libs,{PYTHON_ENV_CACHE}#python3',
            f'--worker-memory {xlearning.WORKER_MEMORY}',
            f'--worker-num {self._worknum}',
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

