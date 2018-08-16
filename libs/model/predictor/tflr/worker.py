#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import os
import sys
sys.path.append('.')

import argparse
from libs.model.linear_model import LogisticRegression
from libs.dataio.tf_data_reader import DataGenerator
from libs.env.hdfs import hdfs


def main_fun_name():
    return __file__


def _predict(lr, data_path, out_path):
    print(f'output path = {out_path}')
    data_g = DataGenerator(data_path, lr.input_dim(), 256)
    with open(out_path, 'w') as f:
        for data_x, data_y in data_g.sync_next_batch():
            pred = lr.predict(data_x)
            for label, p in zip(data_y, pred):
                f.write(f'{int(label[0])}\t{p[0]:.6f}\n')


def main(flags):
    flags.task_index = int(os.environ["TF_INDEX"])
    flags.job_name = os.environ["TF_ROLE"]

    if flags.job_name == 'ps':
        return

    local_ckpt_dir = os.path.join('.', flags.model)
    hdfs.download_checkpoint(os.path.join(flags.hdfs_dir, flags.model), local_ckpt_dir)

    lr = LogisticRegression(input_dim=flags.input_dim)
    lr.from_checkpoint(local_ckpt_dir)

    for data_name in flags.data.split(','):
        local_root = f'./{flags.job_id}/{data_name}'
        if not os.path.exists(local_root):
            os.makedirs(local_root)

        basename = f'part-{flags.task_index}.csv'
        local_filename = os.path.join(local_root, basename)
        _predict(lr, data_name, local_filename)

        print_dir(local_root)

        hdfs_dst = os.path.join(flags.hdfs_dir, f'{data_name}_pred', basename)
        hdfs.put(local_filename, hdfs_dst)
        print(f'success upload {local_filename} to {hdfs_dst}')


def parse_options():

    parser = argparse.ArgumentParser()

    parser.add_argument("--job_name", type=str, default="", help="One of 'ps', 'worker'")
    parser.add_argument("--task_index", type=int, default=0, help="Index of task within the job")
    parser.add_argument('--job_id', type=str, help='job id')
    parser.add_argument("--hdfs_dir", type=str, help="hdfs root dir for this job")
    parser.add_argument("--data", type=str, help="The relative path for data")
    parser.add_argument("--model", type=str, help="The relative save path for model")
    parser.add_argument('--input_dim', type=int, help='feature vector size')

    return parser.parse_known_args()


if __name__ == "__main__":
    import logging
    logging.basicConfig(stream=sys.stderr)
    logger = logging.getLogger(__name__)

    logger.info('start tensorflow worker ...')

    from libs.dataio.fstool import print_dir
    print_dir()

    flags, _ = parse_options()
    main(flags)
