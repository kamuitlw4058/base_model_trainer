#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


from libs.model.linear_model import LogisticRegression
from libs.job import Job
import pyarrow.parquet as pq
import numpy as np


job_dir = '/data/online_job/distributed_model/cache/12_75_1_1'


if __name__ == '__main__':
    job = Job()
    job.from_file(f'{job_dir}/job.json')

    lr = LogisticRegression(input_dim=job.runtime.input_dim)
    lr.from_checkpoint(f'{job_dir}/lr')

    t = pq.read_table(f'{job_dir}/test/part-00000-1cbc87ba-4732-42cc-89fc-4d9657d584e1-c000.snappy.parquet')

    df = t.to_pandas()

    pred = []
    for i, r in df.iterrows():
        x = np.zeros([1, job.runtime.input_dim])
        x[0, r.feature_indices] = 1
        y = lr.predict(x)
        pred.append(y[0, 0])

    df['pred'] = pred

    df.to_json(f'{job_dir}/pred_sample.json', orient='records', force_ascii=False, lines=True)