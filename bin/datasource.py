from libs.feature_datasource.imp.clickhouse_sql import ClickHouseSQLDataSource
from libs.feature_datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.feature_datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.job.job_parser import get_job_local_dir
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
pack_libs(overwrite=True)


"""
data_split_mode-sdate;edate,rtb_param, f1-join;value,f2-join;value,f3

"""


job_dict ={
    'name':'test_job',
    'data_split_mode':
        {
            'type': 'date',
            'args': {
                'train_start_date': '2019-01-01',
                'train_end_date': '2019-01-05',
                'test_start_date': '2019-01-06',
                'test_end_date': '2019-01-06',
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

    "features_extend":
        [
            {
                'type': 'Daily',
                'class':'ClickHouseDailySQLDataSource',
                'name': "daily_sql",
                'sql':"select Id_Zid,EventDate from rtb_all prewhere EventDate='{target_day}' limit 1",
                'keys':['keys'],
                'join_type':'left',
                'features':{
                    'number':['abc'],
                    'onehot':['abc'],
                    'xxx1':['bbb'],
                },
                'default_keys':'abc',
                'default_features':'',
                'depend':[
                    'f1',
                    'f2',
                ]

            },
            {
                'type': 'Once',
                'class':'ClickHouseSQLDataSource',
                'name': "simple_sql",
                'sql': "select Id_Zid,Age,EventDate from rtb_all prewhere EventDate=today() limit 1",
                'keys': ['keys'],
                'join_type':'left',
                'features': {
                    'number': ['abc'],
                    'onehot': ['abc'],
                    'xxx1': ['bbb']
                },

            }
        ],
    "features_drop":[

    ],
    "model":[
        {
            'name':'model'
        }
    ]
}



spark = spark_session(job_dict['name'], 1)
datasource_params_list = job_dict['features_extend']
datasource_list =[]

features_base = job_dict['features_base']
if features_base['type'] == 'RTBModelBaseDataSource':
    kwargs = features_base.get('args')
    if kwargs is None:
        kwargs = {}
    ds = RTBModelBaseDataSource(features_base['name'],
                                job_dict['train_start_date'],
                                job_dict['train_end_date'],
                                spark=spark,
                                is_train=True,
                                **kwargs)
    ds_dict ={}
    ds_dict['type'] = 'base'
    ds_dict['dataset'] = 'train'
    ds_dict['datasouce'] = ds
    datasource_list.append(ds_dict)

    ds = RTBModelBaseDataSource(features_base['name'],
                                job_dict['test_start_date'],
                                job_dict['test_end_date'],
                                spark=spark,
                                is_train=False,
                                **kwargs)
    ds_dict ={}
    ds_dict['type'] = 'base'
    ds_dict['dataset'] = 'test'
    ds_dict['datasouce'] = ds
    datasource_list.append(ds)




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


for ds in datasource_list:
    ds.produce_data()
    df =ds.get_dataframe()
    df_list.append(df)
    df.show()



