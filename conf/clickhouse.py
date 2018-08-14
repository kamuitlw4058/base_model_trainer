#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import random


hosts = [
    '172.31.6.10:8123',
    '172.31.6.11:8123',
    '172.31.6.12:8123',
    '172.31.6.13:8123',
    '172.31.6.14:8123',
    '172.31.6.15:8123',
    '172.31.6.16:8123',
    '172.31.6.17:8123',
    '172.31.6.18:8123',
    '172.31.6.19:8123',
    '172.31.6.20:8123',
    '172.31.6.21:8123',
    '172.31.6.22:8123',
    '172.31.6.23:8123',
    '172.31.6.24:8123',
    '172.31.6.25:8123',
    '172.31.6.26:8123',
    '172.31.6.27:8123',
    '172.31.6.28:8123',
    '172.31.6.29:8123',
]

database = 'zampda_local'

URL = f'jdbc:clickhouse://{random.choice(hosts)}/{database}'

CONF = dict(
    driver='ru.yandex.clickhouse.ClickHouseDriver',
    clickhouseIp=','.join(hosts),
    clickhouseDb=database,
    clickhouse='true',
    clickhouseSocketTimeout=f'{600 * 1000}'
)
ONE_HOST_CONF = dict(
    driver='ru.yandex.clickhouse.ClickHouseDriver',
    clickhouseIp=','.join(hosts),
    clickhouseDb=database,
    clickhouseSocketTimeout=f'{600 * 1000}'
)