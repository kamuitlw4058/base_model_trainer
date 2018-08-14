#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

from datetime import date, datetime


def get_account_filter(v):
    return f'Bid_CompanyId={v}'


def get_vendor_filter(v):
    return f'Media_VendorId={v}'


def get_audience_filter(audience, account):
    if 'rmkt' == audience:
        return f'has(Segment.Id, 100000 + {account})'
    return f'has(Segment.Id, 100000 + {account}) = 0'


def get_os_filter(v):
    return f"Device_Os='{v}'"


def get_day_begin_filter(v):
    return f"EventDate>='{v}'"


def get_day_end_filter(v):
    return f"EventDate<='{v}'"


OS_ID_MAP = {'android': 1, 'ios': 2}

AUDIENCE_ID_MAP = {'ht': 1, 'rmkt': 2}


def gen_job_id(meta):
    if 'id' in meta:
        return meta.id

    if all([i in meta for i in ('account', 'vendor', 'os', 'audience')]):
        return f'{meta.account}_{meta.vendor}_{OS_ID_MAP[meta.os]}_{AUDIENCE_ID_MAP[meta.audience]}'

    if all([i in meta for i in ('vendor', 'os')]):
        return f'{meta.vendor}_{OS_ID_MAP[meta.os]}'

    raise RuntimeError(f'missing values to generate job id, meta = {meta}')


_day_format = {
    str: lambda x: x,
    datetime: lambda x: f'{x:%Y-%m-%d}',
    date: lambda x: f'{x:%Y-%m-%d}'
}


def update_day_range(data, day_begin, day_end):
    interval = {
        'day_begin': f'{day_begin:%Y-%m-%d}',
        'day_end': f'{day_end:%Y-%m-%d}'
    }
    # parse day interval
    for t in ['day_begin', 'day_end']:
        if t in data:
            day = data[t]
            data[t] = _day_format[type(day)](day)
        else:
            data[t] = interval[t]
