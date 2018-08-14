#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

import os
from sklearn.metrics import roc_auc_score
# import pyarrow as pa
import numpy as np
from conf.conf import CURRENT_WORK_DIR, PACKAGE_NAME
from conf.hadoop import PYTHON_ENV_CACHE, HDFS_CODE_CACHE
from conf import xlearning
from libs.env.shell import run_cmd
from libs.env.hdfs import hdfs


class Predictor:
    def __init__(self, job_id, model_conf, runtime_conf):
        self._job_id = job_id
        self._runtime = runtime_conf
        self._model = model_conf
        self._tmp_local_dir = []

    def predict(self, data_names):
        entrance = self.get_worker_entrance()

        job_cmd = ' '.join([
            f'./python3/bin/python3 {entrance}',
            f'--job_id {self._job_id}',
            f'--hdfs_dir {self._runtime.hdfs_dir}',
            f'--model={self._model.name}',
            f'--input_dim={self._runtime.input_dim}',
            f'--data={",".join(data_names)}'
            ])
        self._xlearning_submit(job_cmd, data_names)

        # get result to local dir
        results = []
        for name in data_names:
            result_name = f'{name}_pred'
            hdfs_path = os.path.join(self._runtime.hdfs_dir, result_name)
            local_path = os.path.join(self._runtime.local_dir, result_name)
            hdfs.get_dir(hdfs_path, local_path)
            results.append(result_name)
            logger.info('[%s] success predict %s data', self._job_id, name)

        return results

    def evaluate_auc(self, data_names):
        return [self._evaluate_auc(i) for i in data_names]

    def _evaluate_auc(self, data_name):
        root_dir = os.path.join(self._runtime.local_dir, data_name)

        y_true = []
        y_pred = []
        batch_size = 256
        y_true_cache = np.empty([batch_size, 1], dtype=np.int8)
        y_pred_cache = np.empty([batch_size, 1], dtype=np.float32)
        idx = 0

        for filename in os.listdir(root_dir):
            with open(os.path.join(root_dir, filename), 'r') as f:
                for l in f:
                    if idx >= batch_size:
                        idx = 0
                        y_true.append(y_true_cache)
                        y_pred.append(y_pred_cache)
                        y_true_cache = np.empty([batch_size, 1], dtype=np.int8)
                        y_pred_cache = np.empty([batch_size, 1], dtype=np.float32)
                    tokens = l.split('\t')
                    y_true_cache[idx, 0] = int(tokens[0])
                    y_pred_cache[idx, 0] = float(tokens[1])
                    idx += 1
        y_true.append(y_true_cache[:idx])
        y_pred.append(y_pred_cache[:idx])
        return roc_auc_score(np.concatenate(y_true), np.concatenate(y_pred))

    @staticmethod
    def get_worker_entrance():
        from libs.distributed.prediction.worker import main_fun_name
        main_file = os.path.relpath(main_fun_name(), CURRENT_WORK_DIR)
        logger.info('worker file: %s', main_file)
        return main_file

    def _xlearning_submit(self, job_cmd, data_names):
        # prepare result hdfs data path
        for name in data_names:
            path = os.path.join(self._runtime.hdfs_dir, f'{name}_pred')
            if hdfs.exists(path):
                hdfs.rm(path)
            hdfs.mkdir(path)

        inputs = [f'--input {self._runtime.hdfs_dir}/{i}#{i}' for i in data_names]

        cmd = ' '.join([
                           f'{xlearning.XL_SUBMIT}',
                           f'--app-type "tensorflow"',
                           f'--app-name "prediction-{self._job_id}"',
                           f'--cacheArchive {HDFS_CODE_CACHE}#libs,{PYTHON_ENV_CACHE}#python3',
                           f'--launch-cmd "{job_cmd}"',
                           f'--worker-memory {xlearning.WORKER_MEMORY}',
                           f'--worker-num {self._runtime.worker_num}',
                           # f'--worker-cores {XLearningConfig["worker_cores"]}',
                           f'--ps-num {xlearning.PS_NUM}',
                           f'--queue default',
                           f'--user-path ./python3/bin',
                           f'--board-enable false',
                           f'--jars {xlearning.JARS}',
                       ] + inputs)

        logger.info(cmd)
        run_cmd(cmd)
