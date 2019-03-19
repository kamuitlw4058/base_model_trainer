from libs.feature.processing.processing_base import ProcessingBase

import logging
logger = logging.getLogger(__name__)



class VectorProcessing(ProcessingBase):

    @staticmethod
    def convert_vector(df,cols):
        for col in cols:
            df = df.withColumn(f"{col}_vec", df[col])
        return df,[f"{col}_vec" for col in cols]

    @staticmethod
    def get_type():
        return "dataframe"


    @staticmethod
    def get_name():
        return "vector"

    @staticmethod
    def get_processor(cols):
        return VectorProcessing.convert_vector

    @staticmethod
    def get_vocabulary(stage,col):
      return col,[col + '_vec']