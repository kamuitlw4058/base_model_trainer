import os
from conf.conf import JOB_ROOT_DIR
from pyspark.sql import SparkSession
from libs.feature_datasource.reader import get_features_meta_by_name
from datetime import datetime,timedelta
from libs.feature_datasource.imp.clickhouse_sql import ClickHouseSQLDataSource
from libs.feature_datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.feature_datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.feature_datasource.imp.ad_image import  AdImage
from libs.job.job_parser import get_job_local_dir
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from libs.feature.feature_proessing import processing
from libs.feature_dataoutput.hdfs_output import HdfsOutput
from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.predictor.predictor_factory import PredictorFactory
from libs.feature.define import user_feature,context_feature,user_cap_feature,other_feature
from libs.utilis.dict_utils import list_dict_duplicate_removal
from libs.task.task import Task

pack_libs(overwrite=True)


g_task_dict ={
    'name':'test_job',
    'task_args':{ 'interval':10,'account': 20, 'vendor': 24},
    'data_split':
        {
            'mode': 'date',
            'args':{
            'train_start_date':'2019-03-11',
            'train_end_date':'2019-03-14',
            'test_start_date': '2019-03-15',
            'test_end_date': '2019-03-15'}
        },
    'features_base':
        {
            'type': 'RTBModelBaseDataSource',
            'name': "rtb",
            'global_filter' :['Win_Price > 0', "Device_Os='android'", 'has(Segment.Id, 100012)=0 '],
            'overwrite':False,
            'train_args':
                {
                },
            'test_args':
                {
                }
        },
    'features_extend':[
        { "features_name":"av_ctr_day_interval{interval}",'args':{'interval':30},
          'keys':["Id_Zid","Media_VendorId","Bid_CompanyId","EventDate"],'overwrite':False,
          'processing':[
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_imp'},
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_clk'},
              {'processing': 'onehot', 'col_name': 'a{account}_v{vendor}_last{interval}_ctr'},
            ]},
        {"features_name": "AdImage",
         'keys': ["Bid_AdId"], 'overwrite': False,
         'processing': [
             {'processing': 'vector', 'col_name': 'adimage'},
         ]},
    ],

    'features_processing':{
        'label':'is_clk',
        'cols':
        [
            # {'processing': 'int', 'col_name': 'Time_Hour'},
            # {'processing': 'onehot', 'col_name': 'Time_Hour'},
            # {'processing': 'onehot', 'col_name': 'Age'},
        ]
    },
    'model':[
        {'trainer':"tflr",'trainer_params':{},
         }
    ]

}


model_task = Task(g_task_dict)
model_task.run()