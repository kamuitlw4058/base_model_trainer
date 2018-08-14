import logging
logger = logging.getLogger(__name__)

import random
import pandas as pd
from sqlalchemy import create_engine
from conf.clickhouse import hosts
from libs.dataio.sql import SQL
from libs.feature.define import get_raw_columns
from libs.env.spark import SparkClickhouseReader
from pyspark.sql import functions
import numpy as np
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT


_win_filter = ['TotalErrorCode = 0', 'Win_Timestamp > 0']

_clk_filters = ['notEmpty(Click.Timestamp) = 1'] + _win_filter

_imp_filters = ['notEmpty(Click.Timestamp) = 0'] + _win_filter




class RTBReader:
    def __init__(self,url,table):
        self._url = url
        #f'clickhouse://{random.choice(hosts)}/zampda'
        self._engine = create_engine(url.format(hosts=random.choice(hosts)))
        self._table = table

    @staticmethod
    def get_clk_and_imp_num_sql(table,filters):
        cols = [
            'sum(notEmpty(Click.Timestamp)) as clk',
            'sum(notEmpty(Impression.Timestamp)) as imp'
        ]
        sql = SQL()
        q = sql.table(table).select(cols).where(filters).to_string()
        return q

    def get_clk_and_imp_num(self,filters):
        q = RTBReader.get_clk_and_imp_num_sql(self._table, filters)
        num = pd.read_sql(q, self._engine)
        if num.empty:
            clk_num, imp_num = 0, 0
        else:
            clk_num, imp_num = num.clk.sum(), num.imp.sum()

        logger.info('[%s] clk=%d, imp=%d', self._job_id, clk_num, imp_num)
        return clk_num, imp_num

    def get_feature_raw(self,spark, pos_ratio,neg_ratio,filters,executor_num):

        sql = self._build_feature_sql(pos_ratio,neg_ratio,filters)

        if isinstance(spark,SparkClickhouseReader):
            spark_clickhouse = spark
        else:
            spark_clickhouse =SparkClickhouseReader(spark,self._url.format(hosts=random.choice(hosts)))

        raw = spark_clickhouse.read_sql_parallel(sql,executor_num)

        raw.cache()

        total = self.count_feature_raw(raw)

        if total.clk < CLK_LIMIT:
            raise RuntimeError('too fewer clk({clk} < {CLK_LIMIT})'.format(clk=total.clk,CLK_LIMIT=CLK_LIMIT))

        return raw,total.clk,total.imp

    def _build_feature_sql(self, pos_ratio,neg_ratio,filters):
        cols = get_raw_columns()
        posSql = SQL()
        posSql.table(self._table).select(cols).sample(pos_ratio).where(filters + _clk_filters)
        negSql = SQL()
        negSql.table(self._table).select(cols).sample(neg_ratio).where(filters + _imp_filters)
        posSql.union([negSql])

        sql = posSql.to_string()
        return sql

    def count_feature_raw(self, raw):
        return (raw.agg(
            functions.sum('is_clk').alias('clk'),
            functions.sum('is_win').alias('imp'),
            functions.count('is_win').alias('cnt'))
            .toPandas()
            .iloc[0])





