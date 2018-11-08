#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

from datetime import datetime
from multiprocessing import Process



def run_job(args):
    try:
        logger.info("start job!")
        task_func, job = args
        logger.debug(f'jobmanager{job.get_job_manager()}')
        procdess_name = job.job_name
        if not procdess_name:
            procdess_name = "job_" + "{ts:%y%m%d_%H%M%S}".format(ts =datetime.now())
        p = Process(target=task_func, name=procdess_name,args=(job,))
        p.start()
        p.join()
    except Exception as e:
        logger.exception(e)