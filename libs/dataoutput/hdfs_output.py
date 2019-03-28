from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType
from libs.dataoutput.dataoutput import DataOutput


class HdfsOutput(DataOutput):

    @staticmethod
    def write(df,path):
        df.write.parquet(path=path, mode='overwrite')
        #print(df.columns)
        return