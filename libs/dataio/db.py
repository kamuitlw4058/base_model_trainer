import logging
logger = logging.getLogger(__name__)

import random
import pandas as pd
from sqlalchemy import create_engine
from conf.clickhouse import hosts
from libs.dataio.sql import SQL


class DataInfo:
    def __init__(self, job_id=''):
        self._engine = create_engine(f'clickhouse://{random.choice(hosts)}/zampda')
        self._detect_all_sql = open('./libs/dataio/detect_all_jobs.sql').read()
        self._job_id = job_id

    def ls(self, day_begin, day_end):
        q = self._detect_all_sql.format(day_begin=day_begin, day_end=day_end)
        logger.info(q)

        count = pd.read_sql(q, self._engine)
        return count

    def get_clk_and_imp_num(self, filters):
        cols = [
            'sum(notEmpty(Click.Timestamp)) as clk',
            'sum(notEmpty(Impression.Timestamp)) as imp'
        ]
        sql = SQL()
        q = sql.table('rtb_all').select(cols).where(filters).to_string()
        logger.info('[%s] get click and imp number sql\n%s', self._job_id, q)

        num = pd.read_sql(q, self._engine)
        if num.empty:
            clk_num, imp_num = 0, 0
        else:
            clk_num, imp_num = num.clk.sum(), num.imp.sum()

        logger.info('[%s] clk=%d, imp=%d', self._job_id, clk_num, imp_num)
        return clk_num, imp_num
