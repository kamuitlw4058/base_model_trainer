from libs.feature.processing.processing_base import ProcessingBase
from pyspark.sql.types import ArrayType,DoubleType
from libs.feature.udfs import to_vector,to_array_size

import logging
logger = logging.getLogger(__name__)
from pyspark.sql import functions as F

# df = df.withColumn(f"adimage_size", to_array_size('adimage'))
# gdf = df.groupBy(df.adimage_size)
#
# print(gdf.agg(F.max(df.adimage_size)).collect()[0])

class VectorProcessing(ProcessingBase):

    #TODO:这边是否要兼容不用的dtype
    @staticmethod
    def convert_vector(df,cols):
        vector_size_list = []
        for col in cols:
            df = df.withColumn(f"{col}_vec", to_vector(col))
            df = df.withColumn(f"{col}_size", to_array_size(col))
            vector_size =df.agg(F.max(df[f"{col}_size"]).alias(f"{col}_size_max")).collect()[0][f"{col}_size_max"]
            vector_size_list.append({'col':f"{col}_vec",'size':vector_size})
        return df,[f"{col}_vec" for col in cols],vector_size_list

    @staticmethod
    def get_type():
        return "vector"


    @staticmethod
    def get_name():
        return "vector"

    @staticmethod
    def get_processor(cols):
        return VectorProcessing.convert_vector

    @staticmethod
    def get_vocabulary(stage,col):
      return col,[col + '_vec']