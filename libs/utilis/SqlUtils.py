#!/usr/bin/env python3
# -*- coding: utf-8 -*-



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


def connect(configure=None):
    if not configure:
        config = {
            'host': '172.31.8.1',
            'database': 'db_max_rtb',
            'user': 'user_maxrtb',
            'password': 'C3YN138V',
            'buffered': True
        }
    else:
        config = configure
    conn = mysql.connector.connect(**config)
    conn.set_converter_class(NumpyMySQLConverter)
    return conn



