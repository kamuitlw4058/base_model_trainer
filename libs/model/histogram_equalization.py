#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import numpy as np
import pandas as pd
import logging
logger = logging.getLogger(__name__)


class HistogramEqualization:
    def __init__(self, bin_size=100000):
        self._bin_size = bin_size
        self._bins = np.zeros(self._bin_size + 1, dtype=np.int32)

    def fit(self, data):
        trunc_data = (data * self._bin_size).astype(np.int32) # truncated value
        counts = np.bincount(trunc_data)
        self._bins[:counts.shape[0]] += counts

    def _mapping(self):
        idx = np.flatnonzero(self._bins) # original truncated value
        non_zero_bkt = self._bins[idx]
        cdf = np.cumsum(non_zero_bkt) / np.sum(non_zero_bkt)

        bkt = pd.DataFrame({'x': idx/self._bin_size, 'y': cdf})

        # min, max sentry value
        bkt.loc[bkt.shape[0]] = [bkt.x.min()-1, bkt.y.min()]
        bkt.loc[bkt.shape[0]] = [bkt.x.max()+1, bkt.y.max()]

        return bkt.sort_values('x').reset_index(drop=True)

    def transform(self, data):
        bkt = self._mapping()
        u_idx = np.searchsorted(bkt['x'], data)

        h = bkt.iloc[u_idx].reset_index(drop=True)
        l = bkt.iloc[u_idx - 1].reset_index(drop=True)

        d = h - l
        y = d.y / d.x * (data - l.x) + l.y
        return y

    def save(self, filename):
        bkt = self._mapping()
        bkt.to_csv(filename, sep='\t', header=False, index=False, float_format='%.6f')
        logger.info('success write Histogram Equalization file %s', filename)
