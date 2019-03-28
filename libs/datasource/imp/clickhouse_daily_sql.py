from libs.datasource.datasource import DataSource
from conf.clickhouse import hosts
from conf.conf import FEATURES_NOT_FORMAT_LIST
from libs.env.spark import spark_session,SparkClickhouseReader
import random
import conf.hadoop as hadoop_conf
from libs.utilis.dict_utils import  get_simple_str,get_simple_str_by_template
from libs.utilis.sql_utils import jdbc_sql
from libs.env.hdfs import hdfs
from conf.conf import ZAMPLUS_ZAMPDA_DATABASE,ZAMPLUS_ZAMPDA_LOCAL_DATABASE,ZAMPLUS_RTB_ALL_JDBC_URL,ZAMPLUS_RTB_LOCAL_JDBC_URL,CLICKHOUSE_DAILY_SQL_DATE_COL

from datetime import datetime,timedelta
from libs.datasource.imp.clickhouse_helper import  clickhouse_produce_data,clickhouse_check_filepath
import logging
logger = logging.getLogger(__name__)

class ClickHouseDailySQLDataSource(DataSource):
    def __init__(self, name, start_date, end_date,sql_template,spark,parallel=1,batch_cond=None,**kwargs):
        self._parallel =parallel
        self._sql_template = sql_template
        self._start_date =start_date
        self._end_date= end_date
        self._spark = spark
        self._batch_cond= batch_cond
        super().__init__(name,args=kwargs)


    @staticmethod
    def get_type():
        return 'ck_daily_sql'


    @staticmethod
    def date_range(beginDate, endDate):
        dates = []
        if isinstance( beginDate,datetime):
            dt = beginDate
        else:
            dt = datetime.strptime(beginDate, "%Y-%m-%d")

        if isinstance(endDate,datetime):
            end_dt = endDate
        else:
            end_dt = datetime.strptime(endDate, "%Y-%m-%d")

        while dt <= end_dt:
            dates.append(dt)
            dt = dt + timedelta(1)
        return dates



    def get_sql_list(self,dates,sql_template,**kwargs):
        rSql = []
        for d in dates:
            #kwargs[CLICKHOUSE_DAILY_SQL_DATE_COL] =  d.strftime("%Y-%m-%d")
            kwargs[CLICKHOUSE_DAILY_SQL_DATE_COL] = d
            #print(kwargs)
            #print(sql_template)
            sql = sql_template.format(**kwargs)
            #print(sql)
            rSql.append((sql,d))
        return  rSql


    def get_day_sql_list(self, start_date, end_date,sql_template, **kwargs):
        dates = self.date_range(start_date,end_date)
        return self.get_sql_list(dates,sql_template,**kwargs)


    def _get_daily_sql_list(self,start_date, end_date,sql_template=None,batch_cond=None,**kwargs):
        if sql_template is None:
            sql_template = ""

        sqlList = []
        if batch_cond:
            for cond in batch_cond:
                kwargs.update(cond)
                sl = self.get_day_sql_list(start_date, end_date, sql_template, **kwargs)
                for sql, day in sl:
                    sqlList.append((sql, day, cond))
        else:
            sl = self.get_day_sql_list(start_date, end_date, sql_template, **kwargs)
            for sql, day in sl:
                sqlList.append((sql, day, {}))
        return sqlList

    def produce_data(self,overwrite=False):

        start_date, end_date, sql_template,parallel,batch_cond,kwargs = self._start_date,self._end_date,self._sql_template,self._parallel,self._batch_cond,self._args
        sqlList = self._get_daily_sql_list(start_date,end_date,sql_template,**kwargs)

        reader = SparkClickhouseReader(self._spark, ZAMPLUS_RTB_ALL_JDBC_URL)
        for s,d,cond in sqlList:
            kwargs[CLICKHOUSE_DAILY_SQL_DATE_COL] =  d.strftime("%Y-%m-%d")
            kwargs.update(cond)
            output_file = f"{self.get_type()}{get_simple_str_by_template([sql_template],not_format=FEATURES_NOT_FORMAT_LIST, **kwargs)}"
            #output_file = f"{self.get_type()}_{str(self._name).format(**kwargs)}{get_simple_str(**kwargs)}"
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + str(self._name).format(**kwargs) + '/' + output_file
            clickhouse_produce_data(self._name,reader,s,output_path,overwrite=overwrite)


    def get_dataframe(self):
        start_date, end_date, sql_template, parallel, batch_cond, kwargs = self._start_date, self._end_date, self._sql_template, self._parallel, self._batch_cond, self._args
        sqlList = self._get_daily_sql_list(start_date,end_date)
        hdfs_files_list=[]
        for s,d,cond in sqlList:
            kwargs[CLICKHOUSE_DAILY_SQL_DATE_COL] = d.strftime("%Y-%m-%d")
            kwargs.update(cond)

            output_file = f"{self.get_type()}{get_simple_str_by_template([sql_template],not_format=FEATURES_NOT_FORMAT_LIST, **kwargs)}"
            #output_file = f"{self.get_type()}_{str(self._name).format(**kwargs)}{get_simple_str(**kwargs)}"
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + str(self._name).format(**kwargs) + '/' + output_file

            if clickhouse_check_filepath(self._name,output_path):
                hdfs_files_list.append(output_path)

        ret_df = None
        if len(hdfs_files_list) > 0:
            #print(hdfs_files_list)
            ret_df = self._spark.read.parquet(*hdfs_files_list)

        return ret_df

