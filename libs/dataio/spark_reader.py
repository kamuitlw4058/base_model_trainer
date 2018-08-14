import logging
logger = logging.getLogger(__name__)

import os
import random
import numpy as np
import pandas as pd
import sqlalchemy as sa
from pyspark.sql import functions
from conf import clickhouse
from conf.spark import SPARK_CONFIG
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT
from libs.dataio.sql import SQL
from libs.feature.define import get_raw_columns


def filter_args(args, pattern):
    tmp_filters = []
    for i in range(len(args)):
        if args[i].find(pattern) != 0:
            tmp_filters.append(args[i])
    return tmp_filters


_win_filter = ['TotalErrorCode = 0', 'Win_Timestamp > 0']

_clk_filters = ['notEmpty(Click.Timestamp) = 1'] + _win_filter

_imp_filters = ['notEmpty(Click.Timestamp) = 0'] + _win_filter


class SparkDataReader:
    def __init__(self, job_id, spark, tracker):
        self._job_id = job_id
        self._spark = spark
        self._tracker = tracker
        self._eg = sa.create_engine(f'clickhouse://{random.choice(clickhouse.hosts)}/zampda')

    def read(self, runtime):
        self._tracker.status = 'read data'
        logger.info('[%s] start reading data, runtime: %s', self._job_id, runtime)
        raw = self.retrieve_all(runtime)
        return raw

    def retrieve_all(self, runtime):
        logger.info('[%s] select all data', self._job_id)

        sql = self.build_sql(runtime)

        raw = self._spark.read.jdbc(clickhouse.URL, f'({sql})', properties=clickhouse.CONF)\
            .repartition(runtime.executor_num)

        # cache data before counting
        raw.cache()

        total = self.count(raw)
        no_clk = total.imp - total.clk
        base = np.min([total.clk, no_clk])
        logger.info('[%s] sample info: total==%d, is_clk(%d) : no_clk(%d) = %.1f : %.1f',
                    self._job_id, total.cnt, total.clk, no_clk,
                    np.around(total.clk / base, decimals=1),
                    np.around(no_clk / base, decimals=1))

        runtime.clk_sample_num = int(total.clk)
        runtime.imp_sample_num = int(total.imp)
        runtime.sample_num = int(total.imp)

        self._tracker.clk_sample_num = total.clk
        self._tracker.imp_sample_num = total.imp
        self._tracker.sample_num = total.imp

        if total.clk < CLK_LIMIT:
            raise RuntimeError(f'[{self._job_id}] too fewer clk({total.clk} < {CLK_LIMIT})')

        return raw

    def build_sql(self, runtime):
        cols = get_raw_columns()
        posSql = SQL()
        posSql.table('rtb_local2').select(cols).sample(runtime.pos_ratio).where(runtime.filters + _clk_filters)
        negSql = SQL()
        negSql.table('rtb_local2').select(cols).sample(runtime.neg_ratio).where(runtime.filters + _imp_filters)
        posSql.union([negSql])

        sql = posSql.to_string()
        logger.info('[%s]\n%s', self._job_id, sql)
        runtime.data_sql = sql
        return sql

    def count(self, raw):
        return (raw.agg(
            functions.sum('is_clk').alias('clk'),
            functions.sum('is_win').alias('imp'),
            functions.count('is_win').alias('cnt'))
            .toPandas()
            .iloc[0])
