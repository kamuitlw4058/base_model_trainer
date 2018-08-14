#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


def dict_merge(left, right):
    """
    merge right dict to left dict
    :param left: dict
    :param right: dict
    :return: new dict
    """
    keys = {i for i in left.keys()} | {i for i in right.keys()}
    res = {}
    for k in keys:
        if k in left:
            v = left[k]
            if isinstance(v, dict) and k in right:
                res[k] = dict_merge(v, right[k])
            else:
                res[k] = right[k] if k in right else v
        else:
            res[k] = right[k]

    return res
