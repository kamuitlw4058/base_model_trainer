#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

import os
from collections import namedtuple
import numpy as np
from datetime import datetime
from conf.conf import JOB_FILE_NAME
from libs.env.utils import init_spark
from libs.task import init_task_dir, get_worker_num, clean_task_dir
from libs.dataio.persist import write_desc
from libs.dataio.spark_reader import SparkDataReader
from libs.model.histogram_equalization import HistogramEqualization
from task.feature_engineer import FeatureEngineer
from libs.distributed.train.driver import Trainer
from libs.distributed.prediction.driver import Predictor
from libs.dataio.deploy import sender_all
from libs.env.hdfs import hdfs
from libs.task.base_task import Task


NEED_PREPARE_DATA = True

Writer = namedtuple('Writer', ['func', 'suffix', 'args'])


class WinPriceTask(Task):
    def __init__(self, job):
        super().__init__(job)

    def prepare_data(self):
        spark = None
        try:
            runtime = self._job.runtime

            spark = init_spark(self._job_id)
            logger.info('success init spark session.')

            # # read training & testing data
            reader = SparkDataReader(self._job_id, spark, self._tracker)
            raw = reader.read(runtime)

            fe = FeatureEngineer(self._job_id, spark, self._tracker)
            fe.transform(runtime, raw)

            # serialize job data to hdfs
            job_path = os.path.join(runtime.local_dir, JOB_FILE_NAME)
            self._job.to_file(job_path)
            hdfs.put(job_path, os.path.join(runtime.hdfs_dir, JOB_FILE_NAME))
            logger.info('[%s] finish storing train and test data to HDFS.', self._job_id)
            return fe
        except Exception as e:
            logger.error('[%s] exception: %s', self._job_id, e)
            raise e
        finally:
            if spark:
                spark.stop()

    def run(self, options):
        try:
            runtime_conf = self._job.runtime
            self._tracker.status = 'init'
            self._tracker.job_id = self._job_id
            logger.info('[%s] job info: %s', self._job_id, self._job)

            start_time = datetime.now()
            runtime_conf.start_time = f'{start_time:%Y-%m-%d %H:%M:%S}'
            self._tracker.start_time = runtime_conf.start_time

            ######################
            # prepare data
            ######################
            if NEED_PREPARE_DATA:
                init_task_dir(runtime_conf)
                fe = self.prepare_data()
            else:
                hdfs_filename = os.path.join(runtime_conf.hdfs_dir, JOB_FILE_NAME)
                local_filename = os.path.join(runtime_conf.local_dir, JOB_FILE_NAME)
                if os.path.exists(local_filename):
                    os.remove(local_filename)
                hdfs.get(hdfs_filename, local_filename)
                self._job.from_file(local_filename)
                job_id, meta, model_conf, runtime_conf = self._job.id, self._job.meta, self._job.model, self._job.runtime

            runtime_conf.worker_num = get_worker_num(runtime_conf.imp_sample_num)
            data_names = ['train', 'test']

            #######################################
            # train model
            #######################################
            self._tracker.status = 'train'
            runtime_conf.model = 'model'
            trainer = Trainer(self._job_id, model_conf, runtime_conf)
            model = trainer.train(data_names[0])

            #######################################
            # evaluate model performance
            #######################################
            self._tracker.status = 'auc'
            predictor = Predictor(self._job_id, model_conf, runtime_conf)
            pred_results = predictor.predict(data_names)

            runtime_conf.train_auc, runtime_conf.test_auc = predictor.evaluate_auc(pred_results)
            self._tracker.train_auc, self._tracker.test_auc = runtime_conf.train_auc, runtime_conf.test_auc
            logger.info('[%s] train auc %.3f, test auc %.3f', self._job_id, runtime_conf.train_auc, runtime_conf.test_auc)

            #######################################
            # histogram equalization transform
            #######################################
            self._tracker.status = 'histogram_equalization'
            he = HistogramEqualization()
            he_data_dir = os.path.join(runtime_conf.local_dir, pred_results[0])
            for basename in os.listdir(he_data_dir):
                file_path = os.path.join(he_data_dir, basename)
                pred = np.genfromtxt(file_path, delimiter='\t', usecols=(1,))
                he.fit(pred)

            # treat finish time as version
            end_time = datetime.now()
            runtime_conf.end_time = f'{end_time:%Y-%m-%d %H:%M:%S}'
            self._tracker.end_time = runtime_conf.end_time

            version = end_time
            self._tracker.version = end_time

            # write data
            base_name = f'{version:%Y%m%d%H%M}.{job_id}'
            status = 'ok' if runtime_conf.test_auc > 0.5 else 'unripe'
            runtime_conf.status = status

            file_list = []
            for w in [
                Writer(func=fe.save_feature_index_map, suffix='index', args={}),
                Writer(func=fe.save_feature_opts, suffix='feature', args={}),
                Writer(func=he.save, suffix='he', args={}),
                Writer(func=write_desc, suffix='desc', args={'job': self._job}),
                Writer(func=model.save, suffix='pb', args={})
            ]:
                self._tracker.status = f'write_{w.suffix}'
                file_name = os.path.join(runtime_conf.local_dir, f'{base_name}.{w.suffix}')
                w.func(file_name, **w.args)
                file_list.append(file_name)

            # send out
            if options.send and status == 'ok':
                self._tracker.status = 'send_out'
                sender_all(job_id, version, file_list)
                self._tracker.status = status
                logger.info('[%s] finish send all', job_id)
            else:
                self._tracker.status = status

            logger.info('[%s] finished, elapsed %s', self._job_id, str(end_time - start_time))
        except Exception as e:
            logger.exception('[%s] %s', self._job_id, e)
            raise e
        finally:
            if not options.debug:
                clean_task_dir(runtime_conf)

        try:
            if not options.debug:
                self._tracker.commit()
        except Exception as e:
            logger.exception(e)
