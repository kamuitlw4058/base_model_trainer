#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'

import logging
logger = logging.getLogger(__name__)

import tensorflow as tf


class LogisticRegression:
    def __init__(self, input_dim, l2=1.0, sess=None):
        self._sess = sess
        self._input_dim = input_dim
        self._l2 = float(l2)
        self._input_name = 'input/x:0'
        self._output_name = 'lr/Sigmoid:0'

    def from_checkpoint(self, ckpt_dir):
        tf.reset_default_graph()
        ckpt = tf.train.get_checkpoint_state(ckpt_dir)

        saver = tf.train.import_meta_graph(f'{ckpt.model_checkpoint_path}.meta', clear_devices=True)
        logger.info('local ckpt dir: %s', ckpt.model_checkpoint_path)

        self._sess = tf.Session()
        saver.restore(self._sess, tf.train.latest_checkpoint(ckpt_dir))

        return self

    def build_model(self, x, y_, learning_rate):
        regularizer = tf.contrib.layers.l2_regularizer(self._l2)

        lr = tf.layers.Dense(units=1, activation=tf.nn.sigmoid, kernel_regularizer=regularizer, name='lr')
        y = lr(x)

        loss = tf.losses.log_loss(y_, y) + tf.reduce_sum(lr.losses)
        tf.summary.scalar('loss', loss)

        _, auc_op = tf.metrics.auc(predictions=y, labels=y_)
        tf.summary.scalar('auc', auc_op)

        global_step = tf.train.get_or_create_global_step()
        train_op = tf.train.AdamOptimizer(learning_rate).minimize(loss, global_step=global_step)

        return train_op

    def predict(self, input_x):
        x = self._sess.graph.get_tensor_by_name(self._input_name)
        y = self._sess.graph.get_tensor_by_name(self._output_name)
        return self._sess.run(y, feed_dict={x: input_x})

    def save(self, filename):
        input_graph_def = self._sess.graph.as_graph_def()
        output_graph_def = tf.graph_util.convert_variables_to_constants(
            sess=self._sess,
            input_graph_def=input_graph_def,
            output_node_names=self._output_name[:-2].split(',')
        )
        with tf.gfile.GFile(filename, 'wb') as f:
            f.write(output_graph_def.SerializeToString())
        logger.info('%d ops in the final graph.', len(output_graph_def.node))

    def get_weight(self):
        return self._sess.run('lr/kernel:0')

    def get_bias(self):
        return self._sess.run('lr/bias:0')

    def input_dim(self):
        return self._input_dim
