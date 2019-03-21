import logging
logger = logging.getLogger(__name__)
from libs.feature_datasource.datasource import  DataSource
from pyspark.sql.dataframe import DataFrame
from conf import hadoop as hadoop_conf
import libs.env.hadoop
from pyspark.sql import SparkSession
from libs.utilis.spark_utils import read_csv
from libs.env.hdfs import hdfs
import logging
logger = logging.getLogger(__name__)




class CsvDataSource(DataSource):


    def __init__(self, name, fileurl, spark, header=True, delimiter=',', parallel=False, schema_names=None):
        self.model = None
        self._spark:SparkSession = spark
        self._header= header
        self._delimiter = delimiter
        self._fileurl = fileurl
        self._schema_names =schema_names
        super().__init__(name)


    def get_dataframe(self):
        df =read_csv(self._fileurl, spark=self._spark, has_header=self._header, delimiter=self._delimiter, schema_names=self._schema_names)
        #ret_df:DataFrame = self._spark.read.csv(self._fileurl,header=self._header,"delimiter","\t")
        return df

    @staticmethod
    def get_type():
        return 'csv'


    def produce_data(self,overwrite=False,df_handler=None,write_df=True)->DataFrame:
        return None
