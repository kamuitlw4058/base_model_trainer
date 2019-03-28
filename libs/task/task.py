import os
import json
import logging.config
logging.config.dictConfig(json.load(open('conf/logging.json')))
import libs.env.hadoop
from conf.conf import JOB_ROOT_DIR
from pyspark.sql import SparkSession
from libs.feature_datasource.reader import get_features_meta_by_name
from datetime import datetime,timedelta
from libs.feature_datasource.imp.clickhouse_sql import ClickHouseSQLDataSource
from libs.feature_datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.feature_datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.feature_datasource.imp.ad_image import  AdImage
from libs.feature_datasource.imp.adid_vec import  AdidVecDataSource
from libs.env.spark import spark_session
from libs.pack import  pack_libs
from libs.feature.feature_proessing import processing
from libs.feature_dataoutput.hdfs_output import HdfsOutput
from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.predictor.predictor_factory import PredictorFactory
from libs.feature.define import user_feature,context_feature,user_cap_feature,other_feature
from libs.utilis.dict_utils import list_dict_duplicate_removal
from libs.job.tracker import Tracker
from sqlalchemy import create_engine
from libs.common.utils.DatetimeUtils import get_human_timestamp


def get_rtb_processing():
    cols = user_feature + context_feature + user_cap_feature + other_feature
    cols_list =[]
    for col in cols:
        if col not in [
            'AppCategory',
            'segment'
        ]:
            cols_list.append({'processing': 'onehot', 'col_name': col})
        else:
            cols_list.append({'processing': 'multi_value', 'col_name': col})
    return cols_list


def get_date_str(date_args):
    if isinstance(date_args,int):
        return (datetime.now() + timedelta(days =date_args)).strftime("%Y-%m-%d")
    if isinstance(date_args,str):
        return date_args
    return ""


def data_split(mode,**kwargs)->dict:
    d={}
    if mode == "date":
        d["train_start_date"] = get_date_str(kwargs.get('train_start_date',-8))
        d["train_end_date"] = get_date_str(kwargs.get('train_end_date',-2))
        d["test_start_date"] = get_date_str(kwargs.get('test_start_date',-1))
        d["test_end_date"] = get_date_str(kwargs.get('test_end_date',-1))

    return d


def get_data_split_args(data_split_option):
    data_split_args = data_split(data_split_option['mode'], **data_split_option.get('args', {}))
    print(f"data split args:{data_split_args}")
    return data_split_args


def update_dict(job_args:dict,model_args):
    if job_args is None:
        apply_dict = {}
    else:
        apply_dict = job_args.copy()
    apply_dict.update(**model_args)
    return apply_dict


def get_epoch_num(data_size):
    epoch = 0
    if data_size < 30000:
        epoch = 10
    elif data_size < 30 * 10000:
        epoch = 3
    elif data_size < 60 * 10000:
        epoch = 2
    elif data_size < 120 * 10000:
        epoch = 2
    else:
        epoch = 2

    return epoch

def get_batch_size(data_size):
    if data_size < 30000:
        batch_size = 32
    elif data_size < 30 * 10000:
        batch_size = 64
    elif data_size < 60 * 10000:
        batch_size = 64
    elif data_size < 120 * 10000:
        batch_size = 128
    else:
        batch_size = 128

    return batch_size

def get_worker_num(data_size):
    if data_size < 90_0000:
        return 1
    elif data_size < 300_0000:
        return 2
    elif data_size < 1500_0000:
        return 4
    elif data_size < 3000_0000:
        return 8
    elif data_size < 12000_0000:
        return 16
    else:
        return 64

def get_executor_num(estimated_samples):
    if estimated_samples < 10 * 10000:
        return 2
    elif estimated_samples < 20 * 10000:
        return 4
    elif estimated_samples < 40 * 10000:
        return 8
    elif estimated_samples < 80 * 10000:
        return 16
    elif estimated_samples < 160 * 10000:
        return 32
    elif estimated_samples < 320 * 10000:
        return 48
    else:
        return 64


def get_train_params(task_dict):
    data_size = task_dict['base_size']
    feature_dim = task_dict['feature_dim']
    return get_epoch_num(data_size), get_batch_size(data_size), get_worker_num(data_size), feature_dim

def get_feature_list(vocabulary: list):
    l = []
    for i in vocabulary:
        l.append(i["name"])
    return l

def get_features_opts(vocabulary: list) -> list:
    l = []
    for fe in vocabulary:
        d = {}
        d["name"] = fe["name"]
        d["opt"] = fe["opt"]
        l.append(d)
    return l

def get_features_vocabulary(vocabulary: list) -> list:
    l = []
    idx = 0
    for fe in vocabulary:
        name = fe['name']
        for v in fe['value']:
            d = {}
            d["name"] = f'{name}_{v}'
            d["index"] = idx
            d["opt"] = fe["opt"]
            l.append(d)
            idx += 1
    return l



class Task():
    def __init__(self,task_dict,spark:SparkSession=None):
        self.task_dict = task_dict
        self.spark = spark

    def run(self):
        self.run_task(self.task_dict,self.spark)


    def run_task(self,task_dict,spark:SparkSession=None):
        task_name = task_dict['name']
        task_dict['result'] = {}
        task_start_time = datetime.now()
        pack_libs(overwrite=True,job_name=task_name)

        if spark is None:
            task_spark = spark_session(task_name, 20)
        else:
            task_spark = spark

        data_split_args = get_data_split_args(task_dict['data_split'])
        task_args = task_dict['task_args']
        if data_split_args is not None:
            task_args.update(**data_split_args)

        datasource_list = []

        features_base = task_dict['features_base']
        features_base_name =features_base['name']
        if features_base['type'] == 'RTBModelBaseDataSource':
            task_dict['result']['global_filter'] = features_base.get('global_filter', [])
            task_dict['result']['base_overwrite'] = features_base.get('overwrite', False)
            apply_args = update_dict(task_args, features_base.get('train_args', {}))
            train_ds = RTBModelBaseDataSource(features_base['name'],
                                              task_args['train_start_date'],
                                              task_args['train_end_date'],
                                              spark=task_spark,
                                              global_filter=features_base.get('global_filter', []),
                                              is_train=True,
                                              **apply_args)
            ds_dict = {}
            ds_dict['type'] = 'base'
            ds_dict['name'] = features_base['name']
            ds_dict['dataset'] = 'train'
            ds_dict['datasouce'] = train_ds
            ds_dict['overwrite'] = features_base.get("overwrite", False)
            ds_dict['processing'] = get_rtb_processing()
            ds_dict['join_type'] = features_base.get('join_type', 'left')
            datasource_list.append(ds_dict)

            apply_args = update_dict(task_args, features_base.get('test_args', {}))
            test_ds = RTBModelBaseDataSource(features_base['name'],
                                             task_args['test_start_date'],
                                             task_args['test_end_date'],
                                             global_filter=features_base.get('global_filter', []),
                                             spark=task_spark,
                                             is_train=False,
                                             **apply_args)
            ds_dict = {}
            ds_dict['type'] = 'base'
            ds_dict['dataset'] = 'test'
            ds_dict['name'] = features_base['name']
            ds_dict['datasouce'] = test_ds
            ds_dict['overwrite'] = features_base.get("overwrite", False)
            ds_dict['join_type'] = features_base.get('join_type', 'left')

            datasource_list.append(ds_dict)

        features_extend = task_dict.get("features_extend", [])
        features_extend_name_list =[]

        for feature in features_extend:
            feature_name = feature.get("features_name", "")
            features_class = ""
            features_extend_name_list.append(feature_name)
            if feature_name != "":
                feature_meta = get_features_meta_by_name(feature_name)
                if feature_meta is not None:
                    default_args = feature_meta.get("default_args", {})
                    features_class = feature_meta.get("feature_class", "")
                else:
                    default_args = {}
                    features_class = feature.get("features_class", "")
                    if features_class != "AdImage" and features_class != "AdidVecDataSource":
                        continue

                feature_args = feature.get("args", {})
                apply_args = {}
                apply_args.update(**default_args)
                apply_args.update(**task_args)
                apply_args.update(**feature_args)

                if features_class == "null" or features_class == "":
                    continue

                if features_class == 'RTBModelBaseDataSource':
                    ds = None
                    pass
                    # kwargs = datasource.get('args')
                    # if kwargs is None:
                    #     kwargs = {}
                    # ds = RTBModelBaseDataSource(datasource['name'], datasource['job_dict'], datasource['job_dict'], spark=spark,
                    #                             **kwargs)
                elif features_class == 'ClickHouseDailySQLDataSource':
                    # print(apply_args)
                    # print(feature_meta['feature_context'])
                    ds = ClickHouseDailySQLDataSource(feature_name, apply_args['train_start_date'],
                                                      apply_args['test_end_date'],
                                                      sql_template=feature_meta['feature_context'],
                                                      batch_cond=feature_meta.get('batch_cond', None), spark=task_spark,
                                                      **apply_args)
                elif features_class == 'ClickHouseSQLDataSource':
                    ds = ClickHouseSQLDataSource(feature_name, sql_template=feature_meta['feature_context'],
                                                 spark=task_spark, **apply_args)
                elif features_class == 'AdImage':
                    ds = AdImage(feature_name, apply_args['train_start_date'], apply_args['test_end_date'],
                                 spark=task_spark, **apply_args)
                elif features_class == "AdidVecDataSource":
                    ds = AdidVecDataSource(feature_name,spark=task_spark)
                else:
                    ds = None

                if ds is not None:
                    ds_dict = {}
                    ds_dict['name'] = feature_name
                    ds_dict['type'] = 'extend'
                    ds_dict['datasouce'] = ds
                    ds_dict['keys'] = feature.get("keys", [])
                    ds_dict['overwrite'] = feature.get("overwrite", False)
                    ds_dict['join_type'] = feature.get('join_type','left')
                    processing_list = feature.get("processing", [])
                    for p in processing_list:
                        p['col_name'] = p['col_name'].format(**apply_args)
                    ds_dict['processing'] = processing_list
                    datasource_list.append(ds_dict)

        # print(datasource_list)

        for ds_item in datasource_list:
            ds_item['datasouce'].produce_data(overwrite=ds_item['overwrite'])
            task_dict['result'][f"{ds_item['name']}_overwrite"] = ds_item['overwrite']

        train_df = None
        test_df = None

        for ds_item in datasource_list:
            ds = ds_item['datasouce']
            processing_list = ds_item.get('processing', [])
            task_dict['features_processing']['cols'] += processing_list
            # df = ds.get_dataframe()
            if ds_item['type'] == 'base' and ds_item['dataset'] == 'train':
                df = ds.get_dataframe()
                train_df = df
                task_dict['base_size'] = task_dict.get('base_size', 0) + df.count()

            elif ds_item['type'] == 'base' and ds_item['dataset'] == 'test':
                df = ds.get_dataframe()
                test_df = df
                task_dict['base_size'] = task_dict.get('base_size', 0) + df.count()

        if train_df is None or test_df is None:
            print("Base Data is None!!!!")
            return

        task_dict['result']['test_count'] = test_df.count()
        task_dict['result']['train_count'] = train_df.count()
        print(f"train_count:{ task_dict['result']['train_count']} test_count:{task_dict['result']['test_count']}")

        for ds_item in datasource_list:
            ds = ds_item['datasouce']
            if ds_item['type'] != 'base':
                # train_df = train_df.alias("train_df")train_valid_count
                # feature = featureDf.alias("feature")
                df = ds.get_dataframe()
                train_valid_count  = train_df.join(df, ds_item['keys']).count()
                test_valid_count = test_df.join(df, ds_item['keys']).count()
                train_valid_percent = round(train_valid_count / task_dict['result']['train_count'], 3)
                test_valid_percent = round(test_valid_count / task_dict['result']['test_count'], 3)
                task_dict['result'][f"{ds_item['name']}_train_valid" ] = train_valid_count
                task_dict['result'][f"{ds_item['name']}_test_valid"] = test_valid_count
                task_dict['result'][f"{ds_item['name']}_train_valid_p" ] =train_valid_percent
                task_dict['result'][f"{ds_item['name']}_test_valid_p"] = test_valid_percent
                task_dict['result'][f"{ds_item['name']}_join_type"] = ds_item['join_type']
                print(f"{ds_item['name']} train_valid_count:{train_valid_count} test_count:{test_valid_count}")
                print(f"{ds_item['name']} train_valid_precent:{train_valid_percent} test_count:{test_valid_percent}")

                train_df = train_df.join(df, ds_item['keys'], ds_item['join_type'])
                test_df = test_df.join(df, ds_item['keys'], ds_item['join_type'])

        features_processing = task_dict['features_processing']
        print(f"features_processing:{features_processing}")
        features_processing['cols'] = list_dict_duplicate_removal(features_processing['cols'])
        train_processed_df, test_processed, vocabulary, feature_dim = processing(train_df, test_df, features_processing)
        task_dict['feature_dim'] = feature_dim
        task_dict['features_vocabulary'] = vocabulary

        for df, subdir in [(train_processed_df, 'train'), (test_processed, 'test')]:
            HdfsOutput.write(df, os.path.join(os.path.join(JOB_ROOT_DIR.HDFS_ROOT, task_name), subdir))

        data_names = ['train', 'test']

        epoch, batch_size, worker_num, input_dim = get_train_params(task_dict)
        for trainer_conf in task_dict['model']:
            hdfsdir = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, task_name)
            localdir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, task_name)
            trainer_params = trainer_conf['trainer_params']
            trainer_params["name"] = task_name
            trainer_params["hdfs_dir"] = hdfsdir
            trainer_params["local_dir"] = localdir
            model_name =trainer_conf['trainer']

            trainer = TrainerFactory.get_trainer(model_name, **trainer_params)
            trainer.train(epoch, batch_size, worker_num, input_dim, data_names[0])
            predictor = PredictorFactory.get_predictor(model_name, **trainer_params)
            pred_results = predictor.predict(worker_num, input_dim, data_names)

            features_vocabulary = get_features_vocabulary(vocabulary)
            features_opts = get_features_opts(vocabulary)
            features_list = get_feature_list(features_vocabulary)

            # print(f"features_vocabulary：{vocabulary}")
            train_auc, test_auc = predictor.evaluate_auc(pred_results)
            print(f'[{task_name}] train auc {train_auc:.3f}, test auc {test_auc:.3f}', task_name, train_auc, test_auc)
            features_weight = trainer.get_features_weight(features_list)
            # print(f"features_weight：{features_weight}")


            feature_weight_filename = os.path.join(localdir, f'{task_name}_{get_human_timestamp()}.weight')
            trainer.save_features_weight(feature_weight_filename)


            ##TODO: 保存权重信息，
            ##TODO: 保存操作索引和操作方式。
            model_dict = {}
            model_dict['task_name'] = task_name
            model_dict['model_name'] = model_name
            model_dict['start_time'] = task_start_time
            model_dict['end_time'] = datetime.now()
            model_dict['args'] = str(task_args)
            model_dict['train_evaluate'] = train_auc
            model_dict['test_evaluate'] = test_auc
            model_dict['features_base'] = features_base_name
            model_dict['features_extend'] = features_extend_name_list
            model_dict['features_weight'] = feature_weight_filename
            model_dict['features_dim'] = input_dim
            model_dict['train_rows'] = task_dict['result']['train_count']
            model_dict['test_rows'] = task_dict['result']['test_count']
            model_dict['result'] = task_dict['result']


            #task_dict_str = json.dumps(task_dict,indent=4)
            #print(task_dict_str)

            self.commit(model_dict)


    def commit(self, model_dict):
        config = {
            'host': '172.31.8.1',
            'database': 'db_max_rtb',
            'user': 'user_maxrtb',
            'password': 'C3YN138V',
        }
        eg = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}'.format(**config))
        tracker = Tracker()
        tracker.job_name = model_dict.get("task_name")
        tracker.job_args = model_dict.get("args")
        tracker.model_name = model_dict.get("model_name")
        tracker.start_time = model_dict.get("start_time")
        tracker.end_time = model_dict.get("end_time")
        tracker.train_evaluate = str(model_dict.get("train_evaluate"))
        tracker.test_evaluate = str(model_dict.get("test_evaluate"))
        tracker.features_base = str(model_dict.get("features_base"))
        tracker.features_extend = str(model_dict.get("features_extend"))
        tracker.features_weight = str(model_dict.get("features_weight"))
        tracker.result = str(model_dict.get("result"))
        tracker.features_dim = model_dict.get('features_dim', 0)
        tracker.train_rows = model_dict.get('train_rows',0)
        tracker.test_rows = model_dict.get('test_rows',0)

        df = tracker.get_df()

        df.to_sql(name='model_training_log', con=eg, if_exists='append', index=False)


