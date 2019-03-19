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

from libs.feature_datasource.imp.ad_image import  AdImage
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from pyspark.sql.dataframe import DataFrame
pack_libs(overwrite=True)
from pyspark.sql.functions import when



spark = spark_session("testimage", 1)

ds = AdImage("ad_image", start_date='2019-03-11',end_date='2019-03-15',account=20,vendor=24, spark=spark)

#df.select( when(df['age']==2, 3).otherwise(4).alias("age") ).collect()
df = ds.produce_data()
df = ds.get_dataframe()

# df.select("index", f.posexplode("valuelist").alias("pos", "value"))\
#     .where(f.col("index").cast("int") == f.col("pos"))\
#     .select("index", "value")\
#     .show()#df:DataFrame = df.select("Bid_AdId", "AdImage_vector").where(len(df["AdImage_vector"]) ==0)
#df =df.filter('adimage_vector != []')
print(df.dtypes)
print(df.columns)
df.show(100)
#df = ds.get_dataframe()
