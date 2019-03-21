import os
from conf.conf import JOB_ROOT_DIR
from pyspark.sql import SparkSession
from libs.feature_datasource.reader import get_features_meta_by_name
from datetime import datetime,timedelta
from libs.feature_datasource.imp.csv_datasource import CsvDataSource
from libs.feature_datasource.imp.rtb_model_base import RTBModelBaseDataSource
from libs.feature_datasource.imp.clickhouse_daily_sql import  ClickHouseDailySQLDataSource
from libs.job.job_parser import get_job_local_dir
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from libs.feature.feature_proessing import processing
from libs.feature_dataoutput.hdfs_output import HdfsOutput
from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.predictor.predictor_factory import PredictorFactory
from libs.feature.udfs import to_vector,to_array_size,vector_indices,to_string
from libs.feature_datasource.imp.ad_image import  AdImage
from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StringIndexerModel
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType,ArrayType

from libs.env.spark import spark_session
from  libs.pack import  pack_libs
from pyspark.sql.dataframe import DataFrame
from libs.feature.udf.wrapper_udf import split_to_list_udf,list_dict_index_udf,list_dict_has_key_udf,list_avg_udf
job_name = 'test_ds_csv'

pack_libs(overwrite=True,job_name=job_name)

spark = spark_session(job_name, 1)

ds = CsvDataSource(job_name,"hdfs:///user/model/extend_data/user.txt", spark=spark, header=False, delimiter='\t',
                   schema_names=['zid', 'imp', 'clk', 'imp_adid', 'clk_adid'])

user_df:DataFrame = ds.get_dataframe()
user_df.show(10)

ds =  CsvDataSource(job_name,"hdfs:///user/model/extend_data/word_vec.txt", spark=spark, header=False, delimiter='\t',
                    schema_names=['adid', 'adid_vec'])
word_vec_df:DataFrame = ds.get_dataframe()
word_vec_df.show(10)


udf_builder = split_to_list_udf(" ",DoubleType())
apply_udf =udf_builder.get_udf()

word_vec_df = word_vec_df.withColumn("split_col",apply_udf("adid_vec"))
#word_vec_df.show(10)
word_vec_list = word_vec_df.toPandas().to_dict('records')
print(word_vec_list[0])

word_vec_dict = {}

for row in word_vec_list:
    word_vec_dict[row['adid']] = row['split_col']

apply_udf = list_dict_has_key_udf(word_vec_dict).get_udf()
df = user_df.withColumn("adid_has_keylist",apply_udf('imp_adid'))

apply_udf = list_dict_index_udf(word_vec_dict,[0.0 for i in range(32)],output_type=ArrayType(DoubleType())).get_udf()
df = df.withColumn("adid_vec_list",apply_udf('imp_adid'))


apply_udf = list_avg_udf().get_udf()
df = df.withColumn("adid_vec_avg",apply_udf('adid_vec_list'))
df.show(10,truncate=False)



#print(word_vec_dict.get("678469"))
#print(word_vec_dict)

