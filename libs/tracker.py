#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import pandas as pd
from sqlalchemy import create_engine
import mysql.connector


class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)


def _connect():
    config = {
        'host': '172.31.8.1',
        'database': 'db_max_rtb',
        'user': 'user_maxrtb',
        'password': 'C3YN138V',
        'buffered': True
    }
    conn = mysql.connector.connect(**config)
    conn.set_converter_class(NumpyMySQLConverter)
    return conn


class Tracker:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.set(k, v)

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def set(self, key, value):
        self.__dict__[key] = value

    def commit(self):
        df = pd.Series(self.__dict__).to_frame().transpose()
        eg = create_engine('mysql+mysqlconnector://', creator=_connect)
        df.to_sql(name='model_opt_log', con=eg, if_exists='append', index=False)
        logger.info('[%s] success commit to db', self.__dict__['job_id'])
