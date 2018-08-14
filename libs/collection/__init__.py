#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import numpy as np


class AttributeDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self.__setitem__(k, v)
        for k, v in kwargs.items():
            self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, AttributeDict):
            value = AttributeDict(value)
        elif isinstance(value, np.int64) or isinstance(value, np.uint64):
            value = int(value)
        elif isinstance(value, np.float32) or isinstance(value, np.float64):
            value = float(value)

        super().__setitem__(key, value)

    def __getattr__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError as e:
            raise AttributeError(key)

    __setattr__ = __setitem__
