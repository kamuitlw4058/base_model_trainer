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
pack_libs(overwrite=True)


sql = """SELECT distinct Bid_AdId

FROM zampda.rtb_all prewhere EventDate >= toDate('2019-03-12') -1
AND EventDate <= toDate('2019-03-12') -1
AND TotalErrorCode=0
WHERE Media_VendorId = 24
    AND Bid_CompanyId = 12
    AND notEmpty(Impression.Timestamp)
"""


spark = spark_session("testimage", 1)

ds = AdImage("testimage", sql, spark=spark)

df = ds.produce_data()
df.show(10)
#df = ds.get_dataframe()
