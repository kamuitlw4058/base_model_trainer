from libs.feature_datasource.datasource import DataSource
from conf.clickhouse import hosts
from libs.env.spark import spark_session,SparkClickhouseReader
import random
import conf.hadoop as hadoop_conf
from libs.utilis.dict_utils import  get_simple_str
from libs.env.hdfs import hdfs
from libs.utilis.sql_utils import jdbc_sql
from libs.feature_datasource.imp.clickhouse_helper import clickhouse_produce_data,clickhouse_check_filepath
from conf.conf import ZAMPLUS_ZAMPDA_DATABASE,ZAMPLUS_ZAMPDA_LOCAL_DATABASE,ZAMPLUS_RTB_ALL_JDBC_URL,ZAMPLUS_RTB_LOCAL_JDBC_URL,CLICKHOUSE_DAILY_SQL_DATE_COL
import logging
logger = logging.getLogger(__name__)




class ClickHouseSQLDataSource(DataSource):
    def __init__(self, name, sql_template,spark,parallel=False,**kwargs):
        self._spark = spark
        self._sql_template = sql_template
        self._parallel = parallel
        super().__init__(name,args=kwargs)


    @staticmethod
    def get_type():
        return 'ck_sql'

    def set_sql(self,sql):
        self._sql_template = sql

    def set_parallel(self,parallel):
        self._parallel = parallel


    def get_output_filepath(self,**kwargs):
        output_file = f"{self.get_type()}{get_simple_str(**kwargs)}"
        output_path = f"{hadoop_conf.HDFS_FEATURE_ROOT}/{str(self._name).format(**kwargs)}/{output_file}"
        return output_path


    def get_dataframe(self):
        hdfs_files_list=[]
        kwargs = self._args
        output_path = self.get_output_filepath(**kwargs)
        if clickhouse_check_filepath(self._name,output_path):
            hdfs_files_list.append(output_path)

        ret_df = None
        if len(hdfs_files_list) > 0:
            ret_df = self._spark.read.parquet(*hdfs_files_list)

        return ret_df


    def produce_data(self,overwrite=False,df_handler=None,write_df=True):
        #spark = spark_session(self._name, self._spark_executor_num, self._local_dir)
        output_path = self.get_output_filepath(**self._args)
        reader = SparkClickhouseReader(self._spark, ZAMPLUS_RTB_LOCAL_JDBC_URL)
        sql = jdbc_sql(self._sql_template.format(**self._args))
        name = str(self._name).format(**self._args)
        return  clickhouse_produce_data(name,reader,sql,output_path,overwrite=overwrite,parallel=self._parallel,df_handler=df_handler,write_df=write_df)

