#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import logging
logger = logging.getLogger(__name__)
import pandas as pd
from zamplus_common.datasource.reader import get_mysql_reader



class RowSaver:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.set(k, v)

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def set(self, key, value):
        self.__dict__[key] = value


    def get_df(self):
        return pd.Series(self.__dict__).to_frame().transpose()

    def commit(self,table_name):

        df = self.get_df()
        mysql_reader = get_mysql_reader()
        mysql_reader.to_sql(df,table_name)

