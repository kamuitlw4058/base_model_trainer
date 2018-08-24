import sys
sys.path.append('.')
import json
import logging.config
from conf.conf import JOB_ROOT_DIR
logging.config.dictConfig(json.load(open('conf/logging.json')))

logger = logging.getLogger(__name__)

from  libs.feature.feature_sql import FeatureSql



import conf.hadoop as hadoop_conf

from libs.env.spark import spark_session,provide_spark_session
from libs.pack import pack_libs
import random
from  conf import clickhouse
from libs.env import hadoop
from libs.env.hdfs import hdfs

class FeatureReader:
    def __init__(self,feature,url):
        self._feature = feature
        self._url = url


    @provide_spark_session
    def read(self,sql,prop,session=None):
        raw = session.read.jdbc(self._url, sql, properties=prop)
        return raw

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
                df = session.read.jdbc(self._url, s, properties=prop)
                df.write.parquet(path=output_path,mode='overwrite')
            if not retDf:
                retDf = df
            else:
                retDf =  retDf.union(df)
        return retDf

    def readDaysBatchCond(self,start_date,end_date,prop,session=None,**kwargs):
        sqlList =[]

        for cond in self._feature._batch_cond:
            kwargs.update(cond)
            sl = self._feature.get_day_sql_list(start_date, end_date,pre_sql=True,**kwargs)
            for sql,day in sl:
                sqlList.append((sql,day,cond))

        retDf = None
        for s,d,cond in sqlList:
            kwargs[self._feature._data_date_col] = d
            kwargs.update(cond)
            output_file = self._feature.get_output_name(d,**kwargs)
            output_path = hadoop_conf.HDFS_FEATURE_ROOT + '/' + self._feature._name + '/' + output_file
            df = None
            if hdfs.exists(output_path):
                logger.info("feature {name} file {path} is exist! we will use file.".format(name=self._feature._name, path=output_path))
                df = session.read.parquet(output_path)
            else:
                logger.info(
                    "feature {name} file {path} is not exist! we get data from clickhouse.".format(name=self._feature._name, path=output_path))
                df = session.read.jdbc(self._url, s, properties=prop)
                df.write.parquet(path=output_path,mode='overwrite')
            if not retDf:
                retDf = df
            else:
                retDf =  retDf.union(df)
        return retDf

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
                    "feature {name} file {path} is not exist! we get data from clickhouse.".format(name=self._feature._name, path=output_path))
                #logger.info(f"sql:{s}")
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
    def unionRaw(self,rawDf,start_date,end_date,prop,session=None,**kwargs):
        from pyspark.sql.functions import col
        if self._feature._pre_sql and  self._feature._temp_table and self._feature._data_time_on_hour:
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
        elif self._feature._pre_sql and self._feature._temp_table and self._feature._batch_cond:
            logger.info("get feature from batch cond list...")
            featureDf = self.readDaysBatchCond(start_date,end_date,prop,session=session, **kwargs)
            if self._feature._temp_table_format:
                temp_table_name = self._feature._temp_table_format.format(**kwargs)
                kwargs[self._feature._temp_table] = temp_table_name
            else:
                temp_table_name = self._feature._temp_table

            featureDf.createOrReplaceTempView(temp_table_name)

            sql = self._feature._sql.format(**kwargs)
            featureDf = session.sql(sql)
        else:
            logger.info("get feature from days list...")
            featureDf = self.readDays(start_date,end_date,prop,session=session,**kwargs)

        raw = rawDf.alias("raw")
        feature = featureDf.alias("feature")
        #on = [col('raw.' + k) == col("feature." + k) for k in self._feature._keys]
        joinedDf = raw.join(feature,self._feature._keys,"left")
        #for k in self._feature._keys:
        #    joinedDf = joinedDf.drop("feature." + k)
        return joinedDf





# database = 'model'
#
# URL = f'jdbc:clickhouse://{random.choice(clickhouse.hosts)}/{database}'
#
# #URL = 'test'
# if __name__ == '__main__':
#     logger.info("start main")
#
#     pack_libs()
#     logger.info("end main")
#     sql_tmp =  ctr_feature.get_ctr_feature()
#
#     session = spark_session("testFeature",3,None)
#
#
#     #feature = feature_sql(["Id_Zid,Media_VendorId,EventDate"],sql_tmp,"[{account},{vendor}]","target_day")
#     feature = FeatureSql("compaign_last30_ctr",["Id_Zid","Media_VendorId","Bid_CompanyId","EventDate"], ["a{account}_{vendor}_last14_imp","a{account}_{vendor}_last14_clk"],sql_tmp,
#                           "target_day","a{account}_v{vendor}_t{target_day:%Y%m%d}")
#
#
#     factory = FeatureReader(feature,URL)
#     args = {'account':12, 'vendor':24}
#
#     raw = factory.read(ctr_feature.get_raw_sql(),clickhouse.ONE_HOST_CONF,session=session)
#     raw.show()
#
#     unioned =  factory.unionRaw(raw,datetime.now() -timedelta(days=10),datetime.now()-timedelta(days=9) ,clickhouse.ONE_HOST_CONF,session=session,**args)
#     unioned.show()
#
#
#     #raw =  factory.read(session=session)
#     #print(raw)
