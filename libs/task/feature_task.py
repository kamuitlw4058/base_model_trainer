#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

import os
from collections import namedtuple
from datetime import datetime
from libs.utilis.time_profile import TimeMonitor
from libs.job.feature_job_manager_imp import FeatureJobManger
from libs.task import init_task_dir
import matplotlib.pyplot as plt
from conf import conf
from libs.env.plot import set_matplot_zh_font

NEED_PREPARE_DATA = True

Writer = namedtuple('Writer', ['func', 'suffix', 'args'])


def init_job(job):
    start_time = datetime.now()
    job.start_time = start_time
    init_task_dir(job.local_dir, job.hdfs_dir)


def feature_analysis_proc(raw,feature,plt_index):
    clk_price = raw.where('is_clk = 1').select(feature).collect()
    not_clk_price = raw.where('is_clk = 0').select(feature).collect()

    plt.figure(plt_index)
    plt.title(feature)
    plt.hist(not_clk_price)
    plt.hist(clk_price)
    fig = plt.gcf()
    fig.savefig(f"{conf.IMAGES_OUTPUT_BASE_DIR}/{feature}.png")

def feature_analysis(raw):
    set_matplot_zh_font()

    feature_analysis_proc(raw, 'Education', 1)
    feature_analysis_proc(raw,'Adb_Device_PriceLevel',2)
    feature_analysis_proc(raw, 'weekday', 3)
    #feature_analysis_proc(raw, 'Device_Brand', 4)
    #feature_analysis_proc(raw, 'Adb_Device_Type', 5)
    #feature_analysis_proc(raw, 'Adb_Device_Platform', 6)
    #feature_analysis_proc(raw, 'Device_Brand', 7)
    #feature_analysis_proc(raw, 'Device_Network', 8)
    #feature_analysis_proc(raw, 'Device_OsVersion', 9)
    #feature_analysis_proc(raw, 'Media_Domain', 10)
    feature_analysis_proc(raw, 'geo_city', 11)
    feature_analysis_proc(raw, 'Age', 12)
    feature_analysis_proc(raw, 'Gender', 13)
    feature_analysis_proc(raw, 'is_weekend', 14)






def prepare_data(job,job_manager):
    job_id = job.job_name
    datasource = None
    try:
        timer = TimeMonitor()

        datasource = job_manager.get_datasource()

        raw,test,features,multi_value_feature,number_features = datasource.get_feature_datas()

        # feature_analysis(raw)
        # import sys
        # sys.exit(0)

        executor_num = datasource.get_executor_num()
        #获取特征编码工厂
        feature_encoder = job_manager.get_feature_encoder()

        train_res, test_res = feature_encoder.encoder(raw,test, features,multi_value_feature,number_features = number_features)
        raw.unpersist()
       # train_res.repartition(32)

        dataoutput = job_manager.get_dataoutput()

        for df, subdir in [(train_res, 'train'), (test_res, 'test')]:
            dataoutput.write_hdfs(df, os.path.join(job.hdfs_dir, subdir),feature_encoder.get_feature_names())

        logger.info('[%s] finish to prepare data, time elapsed %.1f s.',
                    job_id, timer.elapsed_seconds())
        job.prepare_data_elapsed = timer.elapsed_seconds()
        return
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        datasource.close()



def run(job):
    ##################################
    # init job context
    ##################################
    job_id = job.job_name
    job_manager = FeatureJobManger(job)


    try:
        job.start_time = datetime.now()

        job.set_status("init")
        logger.info('[%s] job info: %s', job.job_name,job)
        init_job(job)

        ######################
        # prepare data
        ######################

        if NEED_PREPARE_DATA:
            prepare_data(job,job_manager)
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

        job.set_status("train")

        epoch, batch_size, worker_num, input_dim = job_manager.get_trainer_params()

        logger.info("trainer params:")
        logger.info(f"epoch:{epoch}")
        logger.info(f"batch_size:{batch_size}")
        logger.info(f"worker_num:{worker_num}")
        logger.info(f"input_dim:{input_dim}")


        trainer = job_manager.get_trainer()
        trainer.train(epoch,batch_size,worker_num,input_dim,data_names[0])

        #######################################
        # train result
        #######################################

        features= job_manager.get_feature_encoder().get_feature_list()
        trainer.print_features_weight(features)

        #######################################
        # evaluate model performance
        #######################################
        job.set_status("auc")

        predictor = job_manager.get_predictor()
        pred_results = predictor.predict( worker_num,input_dim, data_names)

        train_auc, test_auc = predictor.evaluate_auc(pred_results)
        logger.info('[%s] train auc %.3f, test auc %.3f', job_id, train_auc, test_auc)

        job.train_auc = train_auc
        job.test_auc = test_auc


        job.end_time = datetime.now()
        job.set_status("finish")

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
        # write data
        base_name = f'{job_id}'
        # status = 'ok' if runtime_conf.test_auc > 0.5 else 'unripe'
        # runtime_conf.status = status
        #
        #file_list = []

        for w in [
            Writer(func=job_manager.get_feature_encoder().save_feature_index_map, suffix='index', args={}),
            Writer(func=job_manager.get_feature_encoder().save_feature_opts, suffix='feature', args={}),
            Writer(func=job_manager.get_trainer().save_features_weight, suffix='weight', args={}),
            #Writer(func=he.save, suffix='he', args={}),
            #Writer(func=write_desc, suffix='desc', args={'job': job}),
            #Writer(func=model.save, suffix='pb', args={})
        ]:
            #tracker.status = f'write_{w.suffix}'

            file_name = os.path.join(job.local_dir, f'{base_name}.{w.suffix}')
            if w.suffix == "weight":
                job.features_weight = file_name
            w.func(file_name, **w.args)
            #file_list.append(file_name)
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
        job.end_time = datetime.now()
        job.set_status("failed")
        logger.exception('[%s] %s', job_id, e)
    finally:
        pass
        # if not options.debug:
        #     clean_task_dir(runtime_conf)
        #     logger.info('[%s] clean task dir', job_id)

    try:
            job.commit()
    except Exception as e:
        logger.exception(e)
