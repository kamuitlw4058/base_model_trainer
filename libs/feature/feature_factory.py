import sys
sys.path.append('.')
import json
import logging.config
from conf.conf import JOB_ROOT_DIR
logging.config.dictConfig(json.load(open('conf/logging.json')))

logger = logging.getLogger(__name__)

from  libs.feature.feature_sql import FeatureSql


from datetime import datetime,timedelta
import conf.hadoop as hadoop_conf

from libs.env.spark import spark_session,provide_spark_session
from libs.pack import pack_libs
import random
from  conf import clickhouse
from libs.env import hadoop
from libs.env.hdfs import hdfs
from libs.feature.clickhouse_sparksql_map import replace_map
from libs.feature import udfs

class FeatureReader:
    def __init__(self,feature,url,executor_num):
        self._feature = feature
        self._url = url
        self._executor_num = executor_num
        self._feature_df = None

    @staticmethod
    def jdbc_sql(sql):
        #sql = sql.lstrip("(")
        #sql = sql.rstrip(")")
        sql = f"({sql})"
        return sql




    @provide_spark_session
    def readDays(self,start_date,end_date,prop,session=None,**kwargs):
        sqlList = self._feature.get_day_sql_list(start_date, end_date,**kwargs)
        retDf = None
        for s,d in sqlList:

            kwargs[self._feature._data_date_col] = d
            output_file = self._feature.get_output_name(d,**kwargs)
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + self._feature._name + '/' + output_file
            df = None
            if hdfs.exists(output_path):
                logger.info("feature {name} file {path} is exist! we will use file.".format(name=self._feature._name, path=output_path))
                df = session.read.parquet(output_path)
            else:
                logger.info(
                    "feature {name} file {path} is not exist! we get data from clickhouse.".format(name=self._feature._name, path=output_path))
                s = self.jdbc_sql(s)
                df = session.read.jdbc(self._url, s, properties=prop)
                df.write.parquet(path=output_path,mode='overwrite')
            if not retDf:
                retDf = df
            else:
                retDf =  retDf.union(df)
        return retDf



    def readDaysWithSql(self, start_date, end_date, sql_template, output_template, prop, batch_cond=None, use_jdbc=True, session=None, suffix="", **kwargs):
        sqlList =[]
        hdfs_files_list = []
        if batch_cond:
            for cond in batch_cond:
                kwargs.update(cond)
                sl = self._feature.get_day_sql_list(start_date, end_date,sql_template,**kwargs)
                for sql,day in sl:
                    sqlList.append((sql,day,cond))
        else:
            sl = self._feature.get_day_sql_list(start_date, end_date,sql_template,**kwargs)
            for sql, day in sl:
                sqlList.append((sql, day, {}))

        ret_df = None
        for s,d,cond in sqlList:
            kwargs[self._feature._data_date_col] = d
            kwargs.update(cond)
            output_file = output_template.format(**kwargs) + suffix
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + self._feature._name + '/' + output_file
            if hdfs.exists(output_path):
                logger.info("feature {name} file {path} is exist! we will use file.".format(name=self._feature._name, path=output_path))
                hdfs_files_list.append(output_path)
                df = None
            else:
                logger.info(
                    "feature {name} file {path} is not exist! we get data from clickhouse.".format(name=self._feature._name, path=output_path))
                if use_jdbc:
                    s = self.jdbc_sql(s)
                    df = session.read.jdbc(self._url, s, properties=prop)
                else:
                    df = session.sql(sql)
                df.write.parquet(path=output_path,mode='overwrite')
            if not ret_df:
                ret_df = df
            else:
                if df:
                    ret_df = ret_df.union(df)

        if not ret_df and len(hdfs_files_list) > 0 :
            ret_df = session.read.parquet(*hdfs_files_list)
        else:
            df =None
            if len(hdfs_files_list) >0:
                df = session.read.parquet(*hdfs_files_list)

            if df:
                ret_df.union(df)
        return ret_df



    @provide_spark_session
    def readHours(self,start_date,end_date,prop,session=None,**kwargs):
        sqlList = self._feature.get_hour_sql_list(start_date, end_date,pre_sql=True, **kwargs)
        retDf = None
        for s,d in sqlList:
            kwargs[self._feature._data_date_col] = d
            output_file = self._feature.get_output_name(d,**kwargs)
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + self._feature._name + '/' + output_file
            df = None
            if hdfs.exists(output_path):
                logger.info("feature {name} file {path} is exist! we will use file.".format(name=self._feature._name, path=output_path))
                df = session.read.parquet(output_path)
            else:
                logger.info(
                    "feature {name} file {path} is not exist! we get data from temp table.".format(name=self._feature._name, path=output_path))

                s = self.jdbc_sql(s)
                df = session.read.jdbc(self._url, s, properties=prop)
                #logger.info(f"count:{df.count()}")
                #logger.info(f"count:{df.show(1)}")
                df.write.parquet(path=output_path,mode='overwrite')
            if not retDf:
                retDf = df
            else:
                retDf =  retDf.union(df)
        return retDf

    @provide_spark_session
    def read(self,start_date,end_date,prop,session=None,**kwargs):
        if self._feature._start_date_offset:
            pre_sql_start_date = start_date + timedelta(self._feature._start_date_offset)
        else:
            pre_sql_start_date = start_date

        if self._feature._pre_sql and self._feature._temp_table and self._feature._data_time_on_hour:
            logger.info("get feature from hours list...")
            featureDf = self.readHours(start_date, end_date, prop, session=session, **kwargs)
            if self._feature._temp_table_format:
                temp_table_name = self._feature._temp_table_format.format(**kwargs)
                kwargs[self._feature._temp_table] = temp_table_name
            else:
                temp_table_name = self._feature._temp_table

            featureDf.createOrReplaceTempView(temp_table_name)

            sql = self._feature._sql.format(**kwargs)
            featureDf = session.sql(sql)
        elif self._feature._pre_sql:
            logger.info("get feature from pre sql list...")
            logger.info(f"params:{kwargs}")
            logger.info(f"sql start date:{start_date}  sql end date:{end_date}")

            for item in self._feature._pre_sql:
                if item.get('start_date_offset'):
                    pre_sql_start_date = start_date + timedelta(item.get('start_date_offset'))
                else:
                    pre_sql_start_date = start_date
                logger.info(f"pre-sql start date:{pre_sql_start_date}  pre-sql end date:{end_date}")
                featureDf = self.readDaysWithSql(pre_sql_start_date, end_date, item.get('sql'),
                                                 item.get('output_template'), prop, batch_cond=item.get('batch_cond'),suffix='_pre', session=session,
                                                 **kwargs)

                if item.get('table_name_template'):
                    temp_table_name = item.get('table_name_template').format(**kwargs)
                    kwargs[item.get('table_name')] = temp_table_name
                else:
                    temp_table_name = item.get('table_name')

                featureDf.createOrReplaceTempView(temp_table_name)

            logger.info("start main sql...")


            if self._feature._once_sql:
                sql = self._feature._sql.format(**kwargs)
                featureDf = session.read.jdbc(self._url, sql, properties=prop)
            else:
                featureDf = self.readDaysWithSql(start_date, end_date,
                                                 self._feature._sql,
                                                 self._feature._output_name,
                                                 prop,
                                                 use_jdbc=False,
                                                 batch_cond=self._feature._batch_cond, session=session,
                                                 **kwargs)
        elif self._feature._csv:
            logger.info("get feature csv file...")
            logger.info("csv url: " + str(self._feature._csv))
            if self._feature._csv_sep:
                sep= self._feature._csv_sep
            else:
                sep =','

            featureDf = session.read.csv(self._feature._csv, header=True, inferSchema=True, sep=sep)
            logger.info("csv feature data count:" + str(featureDf.count()))
            featureDf.show(10)
            self._feature_df = featureDf

        else:
            logger.info("get feature from days list...")
            logger.info(f"params:{kwargs}")
            if self._feature._once_sql:
                logger.info("use once sql...")
                sql = self._feature._sql.format(**kwargs)
                sql = FeatureReader.jdbc_sql(sql)
                featureDf = session.read.jdbc(self._url, sql, properties=prop)
            else:
                featureDf = self.readDaysWithSql(start_date, end_date, self._feature._sql, self._feature._output_name, prop, batch_cond=self._feature._batch_cond,session=session,
                                                 **kwargs)
        self._feature_df = featureDf
        return featureDf

    def get_feature_df(self):
        return  self._feature_df

    def get_feature(self):
        return self._feature

    def get_number_features(self):
        return  self._feature._number_features

    def get_feature_keys(self):
        return self._feature.get_keys()

    @staticmethod
    def unionRaw(rawDf,featureDf,keys,number_features=None):
        raw = rawDf.alias("raw")
        feature = featureDf.alias("feature")
        joinedDf = raw.join(feature,keys,"left")
        if number_features:
            for f in number_features:
                logger.info("int not zero....")
                joinedDf = joinedDf.withColumn(f, udfs.int_default_zero(f))
        return joinedDf






