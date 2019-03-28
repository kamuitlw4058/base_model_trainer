from libs.processing.imp.processing_base import ProcessingBase

import logging
logger = logging.getLogger(__name__)



class DoubleProcessing(ProcessingBase):

    @staticmethod
    def convert_number(df,cols):
        for col in cols:
            df = df.withColumn(f"{col}_double", df[col].cast("double"))
        return df,[f"{col}_double" for col in cols]

    @staticmethod
    def get_type():
        return "dataframe"


    @staticmethod
    def get_name():
        return "double"

    @staticmethod
    def get_processor(cols):
        return DoubleProcessing.convert_number

    @staticmethod
    def get_vocabulary(stage,col):
      return col,[col + '_double']



class IntProcessing(ProcessingBase):

    @staticmethod
    def convert_number(df,cols):
        for col in cols:
            df = df.withColumn(f"{col}_int", df[col].cast("int"))
        return df,[f"{col}_int" for col in cols]


    @staticmethod
    def get_type():
        return "dataframe"


    @staticmethod
    def get_name():
        return "int"

    @staticmethod
    def get_processor(cols):
        return IntProcessing.convert_number

    @staticmethod
    def get_vocabulary(stage,col):
      return col,[col + '_int']

