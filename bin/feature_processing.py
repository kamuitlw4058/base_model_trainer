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


"""
data_split-sdate;edate,rtb_param, f1-join;value,f2-join;value,f3

"""


job_dict ={
    'name':'test_job',
    'data_split':
        {
            'type': 'date',
            'args': {
                'train_start_date': '2019-02-20',
                'train_end_date': '2019-02-25',
                'test_start_date': '2019-02-26',
                'test_end_date': '2019-02-26',
            }
        },
    'features_base':
        {
            'train':
                {
                    'type': 'RTBModelBaseDataSource',
                    'name': "rtb",
                    'args': {'account': 12, 'vendor': 24}
                },
            'test':
                {
                    'type': 'RTBModelBaseDataSource',
                    'name': "rtb",
                    'args': {'account': 12, 'vendor': 24}
                }
        },
    'features_extend':[
        {'type':"ClickHouseDailySQLDataSource","sql":""},
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


job_name = job_dict['name']
spark = spark_session(job_dict['name'], 20)
#datasource_params_list = job_dict['features_extend']
datasource_list =[]

features_base_train = job_dict['features_base']['train']
if features_base_train['type'] == 'RTBModelBaseDataSource':
    kwargs = features_base_train.get('args')
    if kwargs is None:
        kwargs = {}
    train_ds = RTBModelBaseDataSource(features_base_train['name'],
                                job_dict['data_split']['args']['train_start_date'],
                                job_dict['data_split']['args']['train_end_date'],
                                spark=spark,
                                is_train=True,
                                **kwargs)
    ds_dict ={}
    ds_dict['type'] = 'base'
    ds_dict['dataset'] = 'train'
    ds_dict['datasouce'] = train_ds
    datasource_list.append(ds_dict)

    test_ds = RTBModelBaseDataSource(features_base_train['name'],
                                job_dict['data_split']['args']['test_start_date'],
                                job_dict['data_split']['args']['test_end_date'],
                                spark=spark,
                                is_train=False,
                                **kwargs)
    ds_dict ={}
    ds_dict['type'] = 'base'
    ds_dict['dataset'] = 'test'
    ds_dict['datasouce'] = test_ds
    datasource_list.append(ds_dict)



datasource_params_list = job_dict["features_extend"]
for datasource in datasource_params_list:
    if datasource['type'] == 'RTBModelBaseDataSource':
        kwargs =datasource.get('args')
        if kwargs is  None:
            kwargs = {}
        ds = RTBModelBaseDataSource(datasource['name'],datasource['job_dict'],datasource['job_dict'],spark=spark,**kwargs)
    elif datasource['type'] == 'ClickHouseDailySQLDataSource':
        kwargs =datasource.get('args')
        if kwargs is  None:
            kwargs = {}
        ds = ClickHouseDailySQLDataSource(datasource['name'],job_dict['train_start_date'],job_dict['test_end_date'],sql_template=datasource['sql'],batch_cond=datasource.get('batch_cond'),spark=spark,**kwargs)
    elif datasource['type'] == 'ClickHouseSQLDataSource':
        kwargs = datasource.get('args')
        if kwargs is  None:
            kwargs = {}
        ds = ClickHouseSQLDataSource(datasource['name'], sql_template=datasource['sql'], spark=spark, **kwargs)
    else:
        ds = None
    ds_dict = {}
    ds_dict['type'] = 'extend'
    ds_dict['datasouce'] = ds
    datasource_list.append(ds_dict)

df_list =[]
train_df = None
test_df = None


for ds_item in datasource_list:
    ds_item['datasouce'].produce_data()
    df =ds_item['datasouce'].get_dataframe()
    if ds_item['type'] == 'base' and ds_item['dataset'] == 'train':
        train_df = df
    elif ds_item['type'] == 'base' and ds_item['dataset'] == 'test':
        test_df =df


features_processing = job_dict['features_processing']
train_processed_df,test_processed,vocabulary,feature_dim =  processing(train_df,test_df,features_processing)
import os
from conf.conf import JOB_ROOT_DIR

for df, subdir in [(train_processed_df, 'train'), (test_processed, 'test')]:
    HdfsOutput.write(df, os.path.join(os.path.join(JOB_ROOT_DIR.HDFS_ROOT, job_name), subdir))

data_names = ['train', 'test']


def get_train_params(job_dict):
    return 2,64,8,feature_dim


def get_feature_list(vocabulary:list):
    l =[]
    for i in  vocabulary:
        l.append(i["name"])
    return l


def get_features_opts(vocabulary:list) -> list:
    l = []
    for fe in vocabulary:
        d ={}
        d["name"] = fe["name"]
        d["opt"] = fe["opt"]
        l.append(d)
    return l

def get_features_vocabulary(vocabulary:list) -> list:
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


epoch, batch_size, worker_num, input_dim =get_train_params(job_dict)
for trainer_conf in job_dict['model']:
    hdfsdir = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, job_name)
    localdir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, job_name)
    trainer_params = trainer_conf['trainer_params']
    trainer_params["name"] = job_name
    trainer_params["hdfs_dir"]  = hdfsdir
    trainer_params["local_dir"] = localdir

    trainer =TrainerFactory.get_trainer(trainer_conf['trainer'],**trainer_params)
    trainer.train(epoch, batch_size, worker_num, input_dim, data_names[0])
    predictor = PredictorFactory.get_predictor(trainer_conf['trainer'],** trainer_params)
    pred_results = predictor.predict( worker_num,input_dim, data_names)


    features_vocabulary = get_features_vocabulary(vocabulary)
    features_opts = get_features_opts(vocabulary)
    features_list = get_feature_list(features_vocabulary)


    print(f"features_vocabulary：{vocabulary}")
    train_auc, test_auc = predictor.evaluate_auc(pred_results)
    print(f'[{job_name}] train auc {train_auc:.3f}, test auc {test_auc:.3f}', job_name, train_auc, test_auc)
    features_weight = trainer.get_features_weight(features_list)
    print(f"features_weight：{features_weight}")

    ##TODO: 保存权重信息，
    ##TODO: 保存操作索引和操作方式。