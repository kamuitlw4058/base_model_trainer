import sqlalchemy as sa
from conf.clickhouse import hosts
import pandas as pd
import time
import random
from datetime import datetime
import logging
logger = logging.getLogger(__name__)


eg = sa.create_engine(f'clickhouse://{hosts[random.randint(0, len(hosts)-1)]}')
cos_filter = {'1': 'android',
              '2': 'ios'}


def filter(preday, today, hour):
    FILTER = {True: f" prewhere (EventDate = '{today}' or (EventDate = '{preday}' and Time_Hour>={hour})) ",
              False: f" prewhere (EventDate = '{today}' and Time_Hour>={hour}) "}
    return FILTER[today>preday]


def datetime_toString(dt):
    return dt.strftime("%Y-%m-%d")


def detect_jobs(limit_pv = 1000000):
    hour = time.localtime(time.time()).tm_hour
    my_sql = f'''
        SELECT
            key,
            pv
        FROM
        (
            SELECT
                PreBidScore.Key as key,
                count() AS pv
            FROM zampda.rtb_all
            ARRAY JOIN PreBidScore
            PREWHERE (EventDate = today() or (EventDate = today()-1 and Time_Hour >= {hour}))
            WHERE length(PreBidScore.Key) > 0
            GROUP BY PreBidScore.Key
            ORDER BY PreBidScore.Key ASC
        )
        WHERE pv > {limit_pv}
        ORDER BY pv DESC
    '''
    res = pd.read_sql(my_sql, eg)
    return res


def build_parameter(row):
    now = datetime.today()
    key = row.key
    ids = key.split('_')
    if len(ids) == 4:
        vendor, cos = ids[1], cos_filter[ids[2]]
    else:
        vendor, cos = ids[0], cos_filter[ids[1]]


    preday = ''
    today = datetime_toString(now)
    hour = time.localtime(time.time()).tm_hour
    sample_ratio = 0.92

    my_sql = f'''
    select
        EventDate,
        Time_Hour,
        count(1) as pv
    from
        zampda.rtb_all
    array join PreBidScore
    PREWHERE (EventDate = today() or (EventDate = today()-1 and Time_Hour>={hour}))
    WHERE PreBidScore.Key = '{key}' and Media_VendorId = {vendor} and Device_Os = '{cos}'
    GROUP BY EventDate,Time_Hour
    ORDER BY EventDate DESC,Time_Hour DESC
    '''
    tmp = pd.read_sql(my_sql, eg)
    tmp['cum_pv'] = tmp.pv.cumsum()
    tmp['flag'] = tmp['cum_pv'] > 1000_0000

    for row in tmp.itertuples():
        preday = f'{row.EventDate}'
        hour = row.Time_Hour
        if row.flag:
            sample_ratio = round(950_0000 / row.cum_pv, 3)
            break

    parameter = {'prewhere': filter(preday, today, hour),
                 'where': f" where PreBidScore.Key = '{key}' and Media_VendorId = {vendor} and Device_Os = '{cos}'",
                 'sample': f' sample {sample_ratio}'}

    res = f"select PreBidScore.Score as score from zampda.rtb_all {parameter['sample']} array join PreBidScore {parameter['prewhere']} {parameter['where']}"

    return res


def build_tasks():
    logger.info('start detect jobs ......')
    jobs = detect_jobs()
    logger.info('jobs sum %d', len(jobs))
    jobs['sql'] = jobs.apply(build_parameter, axis=1)
    logger.info('complete build tasks sql')
    return jobs[['key','sql']]
