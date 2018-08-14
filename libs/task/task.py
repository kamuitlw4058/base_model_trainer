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
from libs.env.spark_env import init_spark
from libs.task import init_task_dir, get_worker_num, clean_task_dir
from libs.tracker import Tracker
from libs.dataio.db import DataInfo
from libs.dataio.persist import write_desc
from libs.dataio.spark_reader import SparkDataReader
from libs.model.histogram_equalization import HistogramEqualization
from libs.task.feature_engineer import FeatureEngineer
from libs.distributed.train.driver import Trainer
from libs.distributed.prediction.driver import Predictor
from libs.dataio.deploy import sender_all
from libs.env.hdfs import hdfs
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT
from libs.utilis.time_profile import TimeMonitor

NEED_PREPARE_DATA = True

Writer = namedtuple('Writer', ['func', 'suffix', 'args'])


def get_sample_ratio(runtime):
    pos_ratio = runtime.pos_proportion
    neg_ratio = runtime.neg_proportion * runtime.clk_num / (runtime.imp_num - runtime.clk_num)

    if runtime.clk_num > MAX_POS_SAMPLE:
        pos_ratio = runtime.pos_proportion * MAX_POS_SAMPLE / runtime.clk_num
        neg_ratio = runtime.neg_proportion * MAX_POS_SAMPLE / (runtime.imp_num - runtime.clk_num)

    # ensure ratio not larger than 1
    pos_ratio = min(pos_ratio, 1)
    neg_ratio = min(neg_ratio, 1)

    return pos_ratio, neg_ratio


def prepare_data(job, tracker):
    spark = None
    job_id = job.id

    try:
        runtime = job.runtime

        timer = TimeMonitor()

        # get total clk and imp number for this job
        db = DataInfo(job_id=job_id)
        runtime.clk_num, runtime.imp_num = db.get_clk_and_imp_num(runtime.filters)
        tracker.clk_sample_num = runtime.clk_num
        tracker.imp_sample_num = runtime.imp_num
        tracker.sample_num = runtime.imp_num

        if runtime.clk_num < CLK_LIMIT:
            raise RuntimeError(f'[{job_id}] too fewer clk({runtime.clk_num} < {CLK_LIMIT})')

        runtime.pos_ratio, runtime.neg_ratio = get_sample_ratio(runtime)
        logger.info('[%s] pos_ratio = %.4f neg_ratio = %.4f', job_id, runtime.pos_ratio, runtime.neg_ratio)

        estimated_samples = int(runtime.clk_num * runtime.pos_ratio
                                + (runtime.imp_num - runtime.clk_num) * runtime.neg_ratio)
        logger.info('[%s] estimated samples = %d', job_id, estimated_samples)
        spark = init_spark(job_id, estimated_samples, runtime)

        # # read training & testing data
        reader = SparkDataReader(job_id, spark, tracker)
        raw = reader.read(runtime)

        fe = FeatureEngineer(job_id, spark, tracker)
        fe.transform(runtime, raw)


        # serialize job data to hdfs
        job_path = os.path.join(runtime.local_dir, JOB_FILE_NAME)
        job.to_file(job_path)
        hdfs.put(job_path, os.path.join(runtime.hdfs_dir, JOB_FILE_NAME))
        logger.info('[%s] finish to prepare data, time elapsed %.1f s.',
                    job_id, timer.elapsed_seconds())
        return fe
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        if spark:
            spark.stop()
            logger.info('[%s] spark stopped', job_id)


def run(job, options):
    ##################################
    # init job context
    ##################################
    job_id, meta, model_conf, runtime_conf = job.id, job.meta, job.model, job.runtime
    tracker = Tracker(job_id=job_id,
                      os=meta.os,
                      audience=meta.get('audience', None),
                      account=meta.get('account', None),
                      vendor=meta.vendor,
                      tag='tflr')
    try:
        tracker.status = 'init'
        tracker.job_id = job_id
        logger.info('[%s] job info: %s', job_id, job)

        start_time = datetime.now()
        runtime_conf.start_time = f'{start_time:%Y-%m-%d %H:%M:%S}'
        tracker.start_time = runtime_conf.start_time

        ######################
        # prepare data
        ######################
        if NEED_PREPARE_DATA:
            init_task_dir(runtime_conf)
            fe = prepare_data(job, tracker)
        else:
            hdfs_filename = os.path.join(runtime_conf.hdfs_dir, JOB_FILE_NAME)
            local_filename = os.path.join(runtime_conf.local_dir, JOB_FILE_NAME)
            if os.path.exists(local_filename):
                os.remove(local_filename)
            hdfs.get(hdfs_filename, local_filename)
            job.from_file(local_filename)
            job_id, meta, model_conf, runtime_conf = job.id, job.meta, job.model, job.runtime

        runtime_conf.worker_num = min(get_worker_num(runtime_conf.sample_num),
                                      runtime_conf.executor_num)
        data_names = ['train', 'test']

        #######################################
        # train model
        #######################################
        tracker.status = 'train'
        runtime_conf.model = 'model'
        trainer = Trainer(job_id, model_conf, runtime_conf)
        model = trainer.train(data_names[0])

        #######################################
        # evaluate model performance
        #######################################
        tracker.status = 'auc'
        predictor = Predictor(job_id, model_conf, runtime_conf)
        pred_results = predictor.predict(data_names)

        runtime_conf.train_auc, runtime_conf.test_auc = predictor.evaluate_auc(pred_results)
        tracker.train_auc, tracker.test_auc = runtime_conf.train_auc, runtime_conf.test_auc
        logger.info('[%s] train auc %.3f, test auc %.3f', job_id, runtime_conf.train_auc, runtime_conf.test_auc)

        #######################################
        # histogram equalization transform
        #######################################
        tracker.status = 'histogram_equalization'
        he = HistogramEqualization()
        he_data_dir = os.path.join(runtime_conf.local_dir, pred_results[0])
        for basename in os.listdir(he_data_dir):
            file_path = os.path.join(he_data_dir, basename)
            pred = np.genfromtxt(file_path, delimiter='\t', usecols=(1,))
            he.fit(pred)

        # treat finish time as version
        end_time = datetime.now()
        runtime_conf.end_time = f'{end_time:%Y-%m-%d %H:%M:%S}'
        tracker.end_time = runtime_conf.end_time

        version = end_time
        tracker.version = end_time

        # write data
        base_name = f'{version:%Y%m%d%H%M}.{job_id}'
        status = 'ok' if runtime_conf.test_auc > 0.5 else 'unripe'
        runtime_conf.status = status

        file_list = []
        for w in [
            Writer(func=fe.save_feature_index_map, suffix='index', args={}),
            Writer(func=fe.save_feature_opts, suffix='feature', args={}),
            Writer(func=he.save, suffix='he', args={}),
            Writer(func=write_desc, suffix='desc', args={'job': job}),
            Writer(func=model.save, suffix='pb', args={})
        ]:
            tracker.status = f'write_{w.suffix}'
            file_name = os.path.join(runtime_conf.local_dir, f'{base_name}.{w.suffix}')
            w.func(file_name, **w.args)
            file_list.append(file_name)

        # send out
        if options.send and status == 'ok':
            tracker.status = 'send_out'
            sender_all(job_id, version, file_list)
            tracker.status = status
            logger.info('[%s] finish send all', job_id)
        else:
            tracker.status = status

        logger.info('[%s] finished, elapsed %s', job_id, str(end_time - start_time))
    except Exception as e:
        logger.exception('[%s] %s', job_id, e)
    finally:
        if not options.debug:
            clean_task_dir(runtime_conf)
            logger.info('[%s] clean task dir', job_id)

    try:
        if not options.debug or options.send:
            tracker.commit()
    except Exception as e:
        logger.exception(e)
