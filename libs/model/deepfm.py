#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import logging
logger = logging.getLogger(__name__)

import tensorflow as tf


class DeepFM:
    def __init__(self, input_dim, l2=0.001, sess=None,embedding_size=16,hidden_layer=[128,64],dropout=[0.5,0.5]):
        self._sess = sess
        self._input_dim = input_dim
        self._l2 = float(l2)
        self._embedding_size = embedding_size
        self._input_ids = 'input/ids:0'
        self._input_name = 'input/x:0'
        self._output_name = 'DeepFM-out/Sigmoid:0'
        self._is_train_name = "is_on_train"
        self._layers = hidden_layer
        self._dropout = dropout

    def from_checkpoint(self, ckpt_dir):
        tf.reset_default_graph()
        ckpt = tf.train.get_checkpoint_state(ckpt_dir)

        saver = tf.train.import_meta_graph(f'{ckpt.model_checkpoint_path}.meta', clear_devices=True)
        logger.info('local ckpt dir: %s', ckpt.model_checkpoint_path)

        self._sess = tf.Session()
        saver.restore(self._sess, tf.train.latest_checkpoint(ckpt_dir))

        return self

    def build_model(self, ids, vals,lables, learning_rate):

        FM_B = tf.get_variable(name='fm_bias', shape=[1], initializer=tf.constant_initializer(0.0))
        FM_W = tf.get_variable(name='fm_w', shape=[self._input_dim], initializer=tf.glorot_normal_initializer())
        FM_V = tf.get_variable(name='fm_v', shape=[self._input_dim, self._embedding_size],
                               initializer=tf.glorot_normal_initializer())

        is_on_train = tf.placeholder(dtype=tf.bool, name='is_on_train')

        with tf.variable_scope("First-order"):
            feat_wgts = tf.nn.embedding_lookup(FM_W, ids)  # None * F * 1
            y_w = tf.reduce_sum(tf.multiply(feat_wgts, vals), 1)

        with tf.variable_scope("Second-order"):
            embeddings = tf.nn.embedding_lookup(FM_V, ids)  # None * F * K
            input_feat_vals_reshaped = tf.reshape(vals, shape=[-1, self._input_dim, 1])
            embeddings = tf.multiply(embeddings, input_feat_vals_reshaped)  # vij*xi
            sum_square = tf.square(tf.reduce_sum(embeddings, 1))
            square_sum = tf.reduce_sum(tf.square(embeddings), 1)
            y_v = 0.5 * tf.reduce_sum(tf.subtract(sum_square, square_sum), 1)  # None * 1

        with tf.variable_scope("Deep-part"):

            normalizer_fn = None
            normalizer_params = None

            deep_inputs = tf.reshape(embeddings, shape=[-1, self._input_dim * self._embedding_size])  # None * (F*K)
            for i in range(len(self._layers)):
                # if FLAGS.batch_norm:
                #    deep_inputs = batch_norm_layer(deep_inputs, train_phase=train_phase, scope_bn='bn_%d' %i)
                # normalizer_params.update({'scope': 'bn_%d' %i})
                deep_inputs = tf.contrib.layers.fully_connected(inputs=deep_inputs, num_outputs=self._layers[i],
                                                                # normalizer_fn=normalizer_fn, normalizer_params=normalizer_params, \
                                                                weights_regularizer=tf.contrib.layers.l2_regularizer(
                                                                    self._l2), scope='mlp%d' % i)
                # if FLAGS.batch_norm:
                #     deep_inputs = batch_norm_layer(deep_inputs, train_phase=train_phase,
                #                                    scope_bn='bn_%d' % i)  # 放在RELU之后 https://github.com/ducha-aiki/caffenet-benchmark/blob/master/batchnorm.md#bn----before-or-after-relu
                if is_on_train is not None:
                    # if mode == tf.estimator.ModeKeys.TRAIN:
                    deep_inputs = tf.nn.dropout(deep_inputs, keep_prob=self._dropout[
                        i])  # Apply Dropout after all BN layers and set dropout=0.8(drop_ratio=0.2)
                    # deep_inputs = tf.layers.dropout(inputs=deep_inputs, rate=dropout[i], training=mode == tf.estimator.ModeKeys.TRAIN)

            y_deep = tf.contrib.layers.fully_connected(inputs=deep_inputs, num_outputs=1, activation_fn=tf.identity,
                                                       weights_regularizer=tf.contrib.layers.l2_regularizer(self._l2),
                                                       scope='deep_out')
            y_d = tf.reshape(y_deep, shape=[-1])
            # sig_wgts = tf.get_variable(name='sigmoid_weights', shape=[layers[-1]], initializer=tf.glorot_normal_initializer())
            # sig_bias = tf.get_variable(name='sigmoid_bias', shape=[1], initializer=tf.constant_initializer(0.0))
            # deep_out = tf.nn.xw_plus_b(deep_inputs,sig_wgts,sig_bias,name='deep_out')

        with tf.variable_scope("DeepFM-out"):
            # y_bias = FM_B * tf.ones_like(labels, dtype=tf.float32)  # None * 1  warning;这里不能用label，否则调用predict/export函数会出错，train/evaluate正常；初步判断estimator做了优化，用不到label时不传
            # y_bias = FM_B * tf.ones_like(y_d, dtype=tf.float32)      # None * 1
            #y_bias = FM_B
            y_bias = FM_B * tf.ones_like(y_d, dtype=tf.float32)  # None * 1
            y = y_bias + y_w + y_v + y_d
            #y = y_bias + y_w + y_v
            # y = y_bias + y_w
            pred = tf.sigmoid(y)

        # loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=y, labels=labels))

        loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=y, labels=lables)) + \
               self._l2 * tf.nn.l2_loss(FM_W) + \
               self._l2 * tf.nn.l2_loss(FM_V)

        accury = tf.reduce_mean(tf.cast(tf.equal(tf.cast(tf.greater(pred, 0.5), tf.float32), lables), tf.float32))

        # ------bulid optimizer------
        # if FLAGS.optimizer == 'Adam':
        #     optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8)
        # elif FLAGS.optimizer == 'Adagrad':
        #     optimizer = tf.train.AdagradOptimizer(learning_rate=learning_rate, initial_accumulator_value=1e-8)
        # elif FLAGS.optimizer == 'Momentum':
        #     optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.95)
        # elif FLAGS.optimizer == 'ftrl':
        #     optimizer = tf.train.FtrlOptimizer(learning_rate)


        global_step = tf.train.get_or_create_global_step()
        optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8)

        train_op = optimizer.minimize(loss, global_step=global_step)

        return train_op

    def predict(self, ids,input_x):
        tensor_ids = self._sess.graph.get_tensor_by_name(self._input_ids)
        tensor_x = self._sess.graph.get_tensor_by_name(self._input_name)
        tensor_y = self._sess.graph.get_tensor_by_name(self._output_name)
        tensor_is_train = self._sess.graph.get_tensor_by_name(self._is_train_name)

        return self._sess.run(tensor_y, feed_dict={tensor_ids:ids, tensor_x: input_x,tensor_is_train:False})

    def save(self, filename):
        pass



    def input_dim(self):
        return self._input_dim
