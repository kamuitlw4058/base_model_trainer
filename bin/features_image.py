import os
from conf.conf import JOB_ROOT_DIR
from pyspark.sql import SparkSession
from libs.datasource.reader import get_features_meta_by_name
from datetime import datetime,timedelta
from libs.datasource.imp.clickhouse_sql import ClickHouseSQLDataSource
from libs.datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.job.job_parser import get_job_local_dir
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from libs.processing.feature_proessing import processing
from libs.dataoutput.hdfs_output import HdfsOutput
from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.predictor.predictor_factory import PredictorFactory
from libs.processing.udfs import to_vector,to_array_size,vector_indices,to_string,vector_values
from libs.datasource.imp.ad_image import  AdImage
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from pyspark.sql.dataframe import DataFrame
pack_libs(overwrite=True,job_name='testimage')
from pyspark.sql.functions import when
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StringIndexerModel
from pyspark.ml import Pipeline

spark = spark_session("testimage", 1)

ds = AdImage("ad_image", start_date='2019-03-11',end_date='2019-03-15',account=20,vendor=24, spark=spark)

df = ds.produce_data()
df = ds.get_dataframe()
df = df.withColumn(f"adimage_vec", to_vector('adimage'))
df = df.withColumn(f"Bid_AdId_str", to_string('Bid_AdId'))
df:DataFrame = df.withColumn(f"adimage_size", to_array_size('adimage'))

assembler = VectorAssembler(inputCols=["Bid_AdId_vec","adimage_vec"], outputCol='imp')

string_indexers = StringIndexer(inputCol="Bid_AdId_str", outputCol="Bid_AdId_idx", handleInvalid='keep')


encoders = OneHotEncoder(inputCol="Bid_AdId_idx", outputCol="Bid_AdId_vec", dropLast=True)



pipeline = Pipeline(stages=[string_indexers ,encoders , assembler])

model = pipeline.fit(df)
train_tranfrom =model.transform(df)
train_tranfrom.show(10,truncate=True)


train_tranfrom:DataFrame = train_tranfrom.withColumn('feature_indices', vector_indices('imp'))
train_tranfrom:DataFrame = train_tranfrom.withColumn('feature_values', vector_values('imp'))
train_tranfrom = train_tranfrom.select(["feature_indices","feature_values"])
df = train_tranfrom.toPandas()
row_num = df.shape[0]
print(row_num)
import  numpy as np
x = np.zeros(shape=[row_num, 2116], dtype=np.float32)
for i, row in df.iterrows():
    x[i, row.feature_indices] = row.feature_values

print(x[0,:])
    #y[i, 0] = row.is_clk

#df = train_tranfrom.withColumn('feature_indices', vector_indices('imp'))

# others = [c for c in df.columns if c not in schema]
# res = (df
#        # .select(['is_clk', 'imp'])
#        #.orderBy(functions.rand())
#        .select('feature_indices')
#        )
#
# res.show(10,truncate=False)
#from pyspark.sql import functions as F
#print(df.agg(F.max(df.adimage_size)).collect())
#print(df.agg(F.max(df.adimage_size).alias("adimage_size_max")).collect()[0]['adimage_size_max'])
# df.select("index", f.posexplode("valuelist").alias("pos", "value"))\
#     .where(f.col("index").cast("int") == f.col("pos"))\
#     .select("index", "value")\
#     .show()#df:DataFrame = df.select("Bid_AdId", "AdImage_vector").where(len(df["AdImage_vector"]) ==0)
#df =df.filter('adimage_vector != []')
#print(df.dtypes)
#print(df.columns)
