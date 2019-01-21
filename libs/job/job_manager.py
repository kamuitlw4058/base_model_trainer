import logging
logger = logging.getLogger(__name__)

import os
import pandas as pd
from datetime import date, datetime
from sklearn.utils import shuffle
from libs.job.base_job import BASE_JOB
from conf.conf import IMP_LIMIT, CLK_LIMIT, WINDOW
import conf.jobs as custom_jobs
from conf.conf import JOB_ROOT_DIR
from libs.collection.utils import dict_merge
from . import Job
from libs.collection import AttributeDict
from . import tools


def _append_runtime_filters(job):
    filters = ['TotalErrorCode = 0', 'Win_Price > 0']
    meta, data = job.meta, job.data

    recipe = AttributeDict()
    for i in ['account', 'vendor', 'os', 'audience']:
        if data.use_meta:
            if i in meta:
                recipe[i] = meta[i]
        else:
            if i in data:
                recipe[i] = data[i]
            elif i in meta:
                recipe[i] = meta[i]
    for i in ['day_begin', 'day_end']:
        recipe[i] = data[i]

    # basic filters
    for k in ['account', 'vendor', 'os', 'day_begin', 'day_end']:
        if k in recipe:
            v = getattr(tools, f'get_{k}_filter')(recipe[k])
            filters.append(v)

    if ('audience' in recipe) and ('account' in recipe):
        filters.append(tools.get_audience_filter(recipe.audience, recipe.account))

    if recipe.os == 'android' and recipe.vendor != 5 and ('account' in recipe and recipe.account not in (9, 12, 20, 629)):
        filters.append('notEmpty(AppCategory.Key)')

    if recipe.vendor == 21:
        filters += ['Slot_Type = 6']

    if recipe.vendor == 24:
        filters += ['(Slot_Type in (6, 17) or arrayExists(x-> has(SlotViewType, x), [147,148,149,150,351]))']

    if recipe.vendor == 5 and recipe.account != 20:
        filters.append('Slot_Type = 6')

    filters += data.filters

    runtime = AttributeDict()
    runtime.filters = [i for i in filters if i]
    runtime.recipe = recipe
    if 'account' in recipe:
        runtime.account = recipe.account
    runtime.vendor = recipe.vendor
    runtime.pos_proportion = data.pos_proportion
    runtime.neg_proportion = data.neg_proportion
    runtime.local_dir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, job.id)
    runtime.hdfs_dir = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, job.id)
    job.runtime = runtime

    return job


def _get_count(day_begin, day_end):
    # get count
    info = DataInfo()
    count = info.ls(day_begin, day_end)

    rmkt = count[count.audience == 1].copy()
    rmkt.audience = 'rmkt'

    ht = count.groupby(['account', 'vendor', 'os'], as_index=False).sum()
    ht.audience = 'ht'

    return pd.concat([rmkt, ht], axis=0, ignore_index=True)


def detect_jobs_from_data():
    logger.info('detect jobs, imp_limit=%d, clk_limit=%d', IMP_LIMIT, CLK_LIMIT)

    day_end = date.today()
    day_begin = day_end - WINDOW

    count = _get_count(day_begin, day_end)
    count = shuffle(count)

    all_jobs = []
    for row in count.itertuples(index=False):
        if row.vendor in (1, 2, 9):
            logger.info('ignore 1, 2, 9 vendors')
            continue

        if row.imp < IMP_LIMIT or row.clk < CLK_LIMIT:
            logger.warning('too fewer imp or clk for %s', row)
            continue

        job = Job(dict_merge(BASE_JOB, {}))
        meta = job.meta

        meta.account = row.account
        meta.vendor = row.vendor
        meta.os = row.os
        meta.audience = row.audience

        job.id = tools.gen_job_id(meta)
        tools.update_day_range(job.data, day_begin, day_end)

        all_jobs.append(job)
        # logger.info('gen job: %s', row)

    logger.info('gen job numbers %d', len(all_jobs))

    return all_jobs


def _parse_custom_jobs():
    res = []
    for j in custom_jobs.jobs:
        job = Job(dict_merge(BASE_JOB, j))
        job.id = tools.gen_job_id(job.meta)

        today = date.today()
        tools.update_day_range(job.data, today-WINDOW, today)

        res.append(job)
    return res


def gen_jobs(mode):
    manual_jobs = _parse_custom_jobs()

    if mode == 'manual':
        jobs = manual_jobs
    elif mode == 'merge':
        jobs = detect_jobs_from_data()
        job_map = {j.id: j for j in jobs}
        for j in manual_jobs:
            if j.id not in job_map:
                # new manual job append to jobs
                jobs.append(j)
                continue

            # merging
            job = job_map[j.id]
            for k, v in j.items():
                if type(job[k]) == dict:
                    job[k].update(v)
                else:
                    job[k] = v
    else:
        raise RuntimeError(f'unknown option {mode}')

    return [_append_runtime_filters(j) for j in jobs]
