#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import sys
sys.path.append('.')

import os
import json
import argparse
import tensorflow as tf


def main_fun_name():
    return __file__


def main(flags):
    from libs.model.deepfm import DeepFM
    from libs.model.tf.tf_deepfm_data_reader import DataGenerator

    logger.info('flags: %s', flags)

    # cluster specification
    flags.task_index = int(os.environ['TF_INDEX'])
    flags.job_name = os.environ['TF_ROLE']
    cluster_def = json.loads(os.environ['TF_CLUSTER_DEF'])
    cluster = tf.train.ClusterSpec(cluster_def)
    print('ClusterSpec:', cluster_def)
    print('current task id:', flags.task_index, ' role:', flags.job_name)

    server = tf.train.Server(cluster, job_name=flags.job_name, task_index=flags.task_index)

    if flags.job_name == 'ps':
        server.join()
        return

    device_name = f'/job:worker/task:{flags.task_index}'
    with tf.device(tf.train.replica_device_setter(worker_device=device_name, cluster=cluster)):
        # build model
        with tf.name_scope('input'):
            ids = tf.placeholder(dtype=tf.int32, shape=[None, flags.input_dim], name='ids')
            vals = tf.placeholder(tf.float32, shape=[None, flags.input_dim], name='x')
            lables = tf.placeholder(tf.float32, shape=[None], name='y')

        model = DeepFM(flags.input_dim, l2=flags.l2)
        train_op = model.build_model(ids,vals, lables, flags.learning_rate)
        saver = tf.train.Saver()

        data_g = DataGenerator(flags.data, flags.input_dim, flags.batch_size, flags.training_epochs)
        next_batch = data_g.sync_next_batch()
        # hooks = [tf.train.StopAtStepHook(last_step=10000000)]
        chief_hook = [tf.train.CheckpointSaverHook(checkpoint_dir=flags.model, save_steps=1000, saver=saver)]

        def step_fn(step_context):
            train_ids,train_x, train_y = next(next_batch)
            step_context.run_with_hooks(train_op, feed_dict={ids:train_ids, vals: train_x, lables: train_y})

        with tf.train.MonitoredTrainingSession(master=server.target,
                                               is_chief=(flags.task_index == 0),
                                               chief_only_hooks=chief_hook,
                                               checkpoint_dir=flags.log_dir,
                                               # hooks=hooks,
                                               save_checkpoint_secs=None,
                                               max_wait_secs=60
                                               ) as mon_sess:
            while not mon_sess.should_stop():
                mon_sess.run_step_fn(step_fn)


def parse_options():
    parser = argparse.ArgumentParser()

    parser.add_argument('--job_name', type=str, default='', help='One of "ps", "worker"')
    parser.add_argument('--task_index', type=int, default=0, help='Index of task within the job')

    parser.add_argument('--job_id', type=str, help='job id for this training')
    parser.add_argument('--hdfs_dir', type=str, help='hdfs root dir for this job')
    parser.add_argument('--data', type=str, help='The path for train file')
    parser.add_argument('--model', type=str, help='The save path for model')
    parser.add_argument('--log_dir', type=str, help='The log path for model')
    parser.add_argument('--training_epochs', type=int, default=5, help='the epoch of the train')
    parser.add_argument('--input_dim', type=int, help='imp vector size')
    parser.add_argument('--learning_rate', type=float, help='learning rate of the train')
    parser.add_argument('--batch_size', type=int, help='The size of one batch')
    parser.add_argument('--l2', type=float, help='l2 regularization factor')

    return parser.parse_known_args()


if __name__ == '__main__':
    import logging
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                        format='%(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s')
    logger = logging.getLogger('worker')

    logger.info('start tensorflow worker, tensorflow version = %s', tf.__version__)
    tf.logging.set_verbosity(logging.DEBUG)

    from libs.utilis.FsUtils import print_dir
    print_dir()

    flags, _ = parse_options()
    main(flags)
