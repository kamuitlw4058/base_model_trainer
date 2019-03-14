import logging
logger = logging.getLogger(__name__)


from zamplus_common.env.spark.client import spark_session
from zamplus_common.conf.clickhouse import CH_ONE_HOST_CONF, CH_CONF, URL, DEFAULT_DATABASE,CH_HOSTS_PORT
from datetime import datetime
import random
import zamplus_common.env.hadoop.hdfs


start_time = datetime.now().strftime('%Y%m%d%H%M%S')

spark = spark_session("DataMining-" +start_time)


def _get_jdbc_url(host=URL,database=None):
    if database is None:
        database =DEFAULT_DATABASE

    if host is None:
        url = URL.format(random.choice(CH_HOSTS_PORT))
    else:
        url = URL.format(host)
    return url+ database


def clickhouse_jdbc(sql, host=None, database=None, properties=CH_ONE_HOST_CONF):
    jdbc_url = _get_jdbc_url(host,database)
    #print(sql)
    #print(properties)
    return spark.read.jdbc(jdbc_url, f'({sql})', properties=properties)


def sql_op(sql_str):
    return spark.sql(sql_str)

def clickhouse_jdbc_parallel(sql, host=None,database=None):
    if database is not None:
        clickhouse_conf =CH_CONF
        clickhouse_conf['clickhouseDb'] = database
    else:
        clickhouse_conf = CH_CONF
    return clickhouse_jdbc(sql, host, database, properties=clickhouse_conf)


def parquet(path):
    return spark.read.parquet(path)

def csv(path):
    return spark.read.csv(path)












