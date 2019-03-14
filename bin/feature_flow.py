import os
from conf.conf import JOB_ROOT_DIR
from pyspark.sql import SparkSession
from libs.feature_datasource.reader import get_features_meta_by_name
from datetime import datetime,timedelta
from libs.feature_datasource.imp.clickhouse_sql import ClickHouseSQLDataSource
from libs.feature_datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.feature_datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.job.job_parser import get_job_local_dir
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from libs.feature.feature_proessing import processing
from libs.feature_dataoutput.hdfs_output import HdfsOutput
from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.predictor.predictor_factory import PredictorFactory

pack_libs(overwrite=True)


g_task_dict ={
    'name':'test_job',
    'task_args':{ 'interval':10,'account': 12, 'vendor': 24},
    'data_split':
        {
            'mode': 'date',
        },
    'features_base':
        {
            'type': 'RTBModelBaseDataSource',
            'name': "rtb",
            'train_args':
                {
                },
            'test_args':
                {
                }
        },
    'features_extend':[
        { "features_name":"av_ctr_day_interval{interval}",'args':{'interval':14}},
    ],

    'features_processing':{
        'label':'is_clk',
        'cols':
        [
            {'processing': 'int', 'col_name': 'Time_Hour'},
            {'processing': 'onehot', 'col_name': 'Time_Hour'},
            {'processing': 'onehot', 'col_name': 'Age'},
        ]
    },
    'model':[
        {'trainer':"tflr",'trainer_params':{},
         }
    ]




}

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

def run_task(task_dict,spark:SparkSession=None):
    task_name = task_dict['name']


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
    if features_base['type'] == 'RTBModelBaseDataSource':
        apply_args = update_dict(task_args,features_base.get('train_args',{}))
        train_ds = RTBModelBaseDataSource(features_base['name'],
                                          task_args['train_start_date'],
                                          task_args['train_end_date'],
                                          spark=task_spark,
                                          is_train=True,
                                          **apply_args)
        ds_dict = {}
        ds_dict['type'] = 'base'
        ds_dict['dataset'] = 'train'
        ds_dict['datasouce'] = train_ds
        datasource_list.append(ds_dict)

        apply_args = update_dict(task_args,features_base.get('test_args',{}))
        test_ds = RTBModelBaseDataSource(features_base['name'],
                                         task_args['test_start_date'],
                                         task_args['test_end_date'],
                                         spark=task_spark,
                                         is_train=False,
                                         **apply_args)
        ds_dict = {}
        ds_dict['type'] = 'base'
        ds_dict['dataset'] = 'test'
        ds_dict['datasouce'] = test_ds
        datasource_list.append(ds_dict)


    features_extend = task_dict.get("features_extend", [])




    for feature in features_extend:
        feature_name = feature.get("features_name","")
        if feature_name != "":
            feature_meta = get_features_meta_by_name( feature_name)
            default_args= feature_meta.get("default_args",{})
            feature_args = feature.get("args",{})
            apply_args = {}
            apply_args.update(**default_args)
            apply_args.update(**task_args)
            apply_args.update(**feature_args)

            features_class = feature_meta.get("feature_class", "")
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
                ds = ClickHouseDailySQLDataSource(feature_name, apply_args['train_start_date'],
                                                  apply_args['test_end_date'], sql_template=feature_meta['feature_context'],
                                                  batch_cond=feature_meta.get('batch_cond',None), spark=task_spark, **apply_args)
            elif features_class == 'ClickHouseSQLDataSource':
                ds = ClickHouseSQLDataSource(feature_name, sql_template=feature_meta['feature_context'], spark=task_spark, **apply_args)
            else:
                ds = None

            if ds is not None:
                ds_dict = {}
                ds_dict['type'] = 'extend'
                ds_dict['datasouce'] = ds
                datasource_list.append(ds_dict)

    print(datasource_list)

    for ds_item in datasource_list:
        ds_item['datasouce'].produce_data()

    train_df =None
    test_df =None

    for ds_item in datasource_list:
        ds = ds_item['datasouce']
        df = ds.get_dataframe()
        if ds_item['type'] == 'base' and ds_item['dataset'] == 'train':
            train_df = df
            task_dict['base_size'] = task_dict.get('base_size',0) + df.count()
        elif ds_item['type'] == 'base' and ds_item['dataset'] == 'test':
            test_df = df
            task_dict['base_size'] = task_dict.get('base_size',0) + df.count()

    if train_df is None or test_df is None:
        print("Base Data is None!!!!")
        return

    features_processing = task_dict['features_processing']
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

        trainer = TrainerFactory.get_trainer(trainer_conf['trainer'], **trainer_params)
        trainer.train(epoch, batch_size, worker_num, input_dim, data_names[0])
        predictor = PredictorFactory.get_predictor(trainer_conf['trainer'], **trainer_params)
        pred_results = predictor.predict(worker_num, input_dim, data_names)

        features_vocabulary = get_features_vocabulary(vocabulary)
        features_opts = get_features_opts(vocabulary)
        features_list = get_feature_list(features_vocabulary)

        print(f"features_vocabulary：{vocabulary}")
        train_auc, test_auc = predictor.evaluate_auc(pred_results)
        print(f'[{task_name}] train auc {train_auc:.3f}, test auc {test_auc:.3f}', task_name, train_auc, test_auc)
        features_weight = trainer.get_features_weight(features_list)
        print(f"features_weight：{features_weight}")

        ##TODO: 保存权重信息，
        ##TODO: 保存操作索引和操作方式。






run_task(g_task_dict)