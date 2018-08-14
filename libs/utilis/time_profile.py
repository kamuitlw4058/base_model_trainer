#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import time


class TimeMonitor:
    def __init__(self):
        self._start = time.time()

    def reset(self):
        self._start = time.time()

    def elapsed_seconds(self):
        now = time.time()
        return now - self._start
