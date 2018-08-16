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
from libs.task import init_task_dir, get_worker_num, clean_task_dir
from libs.dataio.persist import write_desc
from libs.model.histogram_equalization import HistogramEqualization
from libs.distributed.train.driver import Trainer
from libs.distributed.prediction.driver import Predictor
from libs.dataio.deploy import sender_all
from libs.env.hdfs import hdfs
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT
from libs.utilis.time_profile import TimeMonitor
from libs.dataio.rtb_reader import RTBReader
from libs.env.spark import spark_session
from libs.feature.feature_encoder import FeatureEncoder

NEED_PREPARE_DATA = True

Writer = namedtuple('Writer', ['func', 'suffix', 'args'])


def init_job(job):
    start_time = datetime.now()
    job.start_time = start_time
    init_task_dir(job.local_dir, job.hdfs_dir)



def prepare_data(job):
    job_id = job.job_name
    job_manager = job.get_job_manager()
    try:
        timer = TimeMonitor()

        datasource = job_manager.get_datasource()

        raw,features,multi_value_feature = datasource.get_feature_datas()

        #获取特征编码工厂
        feature_encoder = job_manager.get_feature_encoder()

        train_res, test_res = feature_encoder.encoder(raw, features,multi_value_feature)


        dataoutput = job_manager.get_dataoutput()

        for df, subdir in [(train_res, 'train'), (test_res, 'test')]:
            dataoutput.write_hdfs(df, os.path.join(job.hdfs_dir, subdir),feature_encoder.get_features_name())

        logger.info('[%s] finish to prepare data, time elapsed %.1f s.',
                    job_id, timer.elapsed_seconds())
        job.prepare_data_elapsed = timer.elapsed_seconds()
        return
    except Exception as e:
        logger.exception(e)
        raise e



def run(job):
    ##################################
    # init job context
    ##################################
    job_id = job.job_name
    job_manager = job.get_job_manager()


    try:
        job.status = 'init'
        logger.info('[%s] job info: %s', job.job_name,job)
        init_job(job)

        ######################
        # prepare data
        ######################

        if NEED_PREPARE_DATA:
            prepare_data(job)
        else:
            pass
            # hdfs_filename = os.path.join(runtime_conf.hdfs_dir, JOB_FILE_NAME)
            # local_filename = os.path.join(runtime_conf.local_dir, JOB_FILE_NAME)
            # if os.path.exists(local_filename):
            #     os.remove(local_filename)
            # hdfs.get(hdfs_filename, local_filename)
            # job.from_file(local_filename)
            # job_id, meta, model_conf, runtime_conf = job.id, job.meta, job.model, job.runtime


        # runtime_conf.worker_num = min(get_worker_num(runtime_conf.sample_num),
        #                               runtime_conf.executor_num)


        #######################################
        # train model
        #######################################

        data_names = ['train', 'test']

        job.status = 'train'

        epoch, batch_size, worker_num, input_dim = job_manager.get_trainer_params()

        trainer = job_manager.get_trainer()
        trainer.train(epoch,batch_size,worker_num,input_dim,data_names[0])

        model = trainer.train(data_names[0])

        #######################################
        # evaluate model performance
        #######################################
        job.status = 'auc'

        predictor = job_manager.get_predictor()
        pred_results = predictor.predict(data_names)

        train_auc, test_auc = predictor.evaluate_auc(pred_results)
        logger.info('[%s] train auc %.3f, test auc %.3f', job_id, train_auc, test_auc)

        #######################################
        # histogram equalization transform
        #######################################
        # tracker.status = 'histogram_equalization'
        # he = HistogramEqualization()
        # he_data_dir = os.path.join(runtime_conf.local_dir, pred_results[0])
        # for basename in os.listdir(he_data_dir):
        #     file_path = os.path.join(he_data_dir, basename)
        #     pred = np.genfromtxt(file_path, delimiter='\t', usecols=(1,))
        #     he.fit(pred)
        #
        # # treat finish time as version
        # end_time = datetime.now()
        # runtime_conf.end_time = f'{end_time:%Y-%m-%d %H:%M:%S}'
        # tracker.end_time = runtime_conf.end_time
        #
        # version = end_time
        # tracker.version = end_time
        #
        # # write data
        # base_name = f'{version:%Y%m%d%H%M}.{job_id}'
        # status = 'ok' if runtime_conf.test_auc > 0.5 else 'unripe'
        # runtime_conf.status = status
        #
        # file_list = []
        # for w in [
        #     Writer(func=fe.save_feature_index_map, suffix='index', args={}),
        #     Writer(func=fe.save_feature_opts, suffix='feature', args={}),
        #     Writer(func=he.save, suffix='he', args={}),
        #     Writer(func=write_desc, suffix='desc', args={'job': job}),
        #     Writer(func=model.save, suffix='pb', args={})
        # ]:
        #     tracker.status = f'write_{w.suffix}'
        #     file_name = os.path.join(runtime_conf.local_dir, f'{base_name}.{w.suffix}')
        #     w.func(file_name, **w.args)
        #     file_list.append(file_name)
        #
        # # send out
        # if options.send and status == 'ok':
        #     tracker.status = 'send_out'
        #     sender_all(job_id, version, file_list)
        #     tracker.status = status
        #     logger.info('[%s] finish send all', job_id)
        # else:
        #     tracker.status = status
        #
        # logger.info('[%s] finished, elapsed %s', job_id, str(end_time - start_time))
    except Exception as e:
        logger.exception('[%s] %s', job_id, e)
    finally:
        pass
        # if not options.debug:
        #     clean_task_dir(runtime_conf)
        #     logger.info('[%s] clean task dir', job_id)

    # try:
    #     if not options.debug or options.send:
    #         tracker.commit()
    # except Exception as e:
    #     logger.exception(e)
