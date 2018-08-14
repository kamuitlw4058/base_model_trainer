import sys
sys.path.append('.')

import pandas as pd
import numpy as np
import sqlalchemy as sa
import random
from concurrent.futures import ThreadPoolExecutor

from conf.clickhouse import hosts
from conf.conf import FTP_DIR
from app.he_realtime.jobs import build_tasks
from libs.dataio.deploy import put
from libs.model.histogram_equalization import HistogramEqualization

# prepare logging
import os
import subprocess
if not os.path.exists('./log'):
    os.mkdir('./log')

if not os.path.exists('./app/he_realtime/data'):
    os.mkdir('./app/he_realtime/data')

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s , %(levelname)s , %(message)s',
                    filemode='a',
                    filename='./log/hert_app.log')
logger = logging.getLogger(__file__)


def clickhouse_tofile(key, sql):
    host = hosts[random.randint(0, len(hosts)-1)]
    host = host.split(':')[0]
    cmd = f'''
    clickhouse-client -h {host} --query="{sql}" > tmp_score_{key}.csv
    '''
    ret = subprocess.call(cmd, cwd='./app/he_realtime/data', shell=True)
    if ret != 0:
        raise RuntimeError('fail to call "{}", return code {}'.format(cmd, ret))
    return f'./app/he_realtime/data/tmp_score_{key}.csv'


def run(args):
    key, sql = args[0], args[1]
    try:
        file_name = f'./app/he_realtime/data/{key}.hert'
        he = HistogramEqualization()
        logger.info('start hert %s', key)
        logger.info('[%s] sql --> %s', key, sql)
        tmp = clickhouse_tofile(key, sql)
        score = np.genfromtxt(tmp)
        os.remove(tmp)
        logger.info('[%s] --> data.shape : %s', key, len(score))
        he.fit(score)
        logger.info('[%s] --> finish fit', key)
        he.save(file_name)
        logger.info('send hert %s file', key)
        put(file_name, FTP_DIR)
    except Exception as e:
        logger.exception(e)


def training_tasks():
    jobs = build_tasks()

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(run, [(job.key, job.sql) for job in jobs.itertuples()])

    logger.info('finish all tasks')


if __name__ == '__main__':
    logger.info('begin to train hert models......')
    training_tasks()
