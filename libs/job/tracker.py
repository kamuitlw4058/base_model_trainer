#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import pandas as pd


class Tracker:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.set(k, v)

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def set(self, key, value):
        self.__dict__[key] = value


    def get_df(self):
        return pd.Series(self.__dict__).to_frame().transpose()

