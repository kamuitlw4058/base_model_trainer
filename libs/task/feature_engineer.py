#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'mark'
__email__ = 'mark@zamplus.com'


import logging
logger = logging.getLogger(__name__)

import os
import json
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from libs.feature.define import get_bidding_feature, context_feature, user_feature, user_cap_feature
from libs.feature.cleaner import clean_data
from libs.feature import udfs
from libs.feature.multi_category_encoder import MultiCategoryEncoder, MultiCategoryEncoderModel


def to_hdfs(df, hdfs_path, raw_cols=None):

    @functions.udf(ArrayType(IntegerType()))
    def vector_indices(v):
        return [int(i) for i in v.indices]

    df = df.withColumn('feature_indices', vector_indices('feature'))

    schema = ['is_clk', 'feature_indices']
    # others = [c for c in df.columns if c not in schema]
    res = (df
           # .select(['is_clk', 'feature'])
           .orderBy(functions.rand())
           .select(schema + (raw_cols if raw_cols else []))
           )

    res.write.parquet(path=hdfs_path, mode='overwrite')
    return res.columns


class FeatureEngineer:
    def __init__(self, job_id, spark, tracker=None):
        self._job_id = job_id
        self._spark = spark
        self._tracker = tracker
        self._model = None
        self._vocabulary = None
        self._category_feature = []
        self._multi_value_category_feature = []

    def transform(self, runtime, raw):
        self._tracker.status = 'fe'

        if 'account' in runtime and 'vendor' in runtime:
            bidding_feature = get_bidding_feature(runtime.account, runtime.vendor)
            ext_feature = user_cap_feature + bidding_feature
        else:
            ext_feature = user_cap_feature
        raw = self.expend_fields(raw, ext_feature)

        raw = clean_data(self._job_id, raw, self._spark)
        logger.info('[%s] columns %s', self._job_id, raw.schema.names)

        all_feature = user_feature + context_feature + ext_feature
        self._category_feature = [i for i in all_feature if raw.agg(functions.countDistinct(raw[i]).alias('cnt')).collect()[0].cnt > 1]
        logger.info('[%s] drop unique feature %s', self._job_id, set(all_feature) - set(self._category_feature))

        train, test = raw.randomSplit([0.9, 0.1])
        logger.info('[%s] data statics: total = %d, train = %d, test = %d', self._job_id, raw.count(), train.count(), test.count())

        # one hot encoding
        string_indexers = [StringIndexer(inputCol=c, outputCol="{}_idx".format(c), handleInvalid='keep')
                           for c in self._category_feature]
        encoders = [OneHotEncoder(inputCol="{}_idx".format(c), outputCol="{}_vec".format(c), dropLast=True)
                    for c in self._category_feature]

        # one encoding app category
        self._multi_value_category_feature = [
            'AppCategory',
            'segment'
        ]
        multi_enc = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_vec') for i in self._multi_value_category_feature]

        vec_cols = ['{}_vec'.format(c) for c in self.get_featue_names()]
        assembler = VectorAssembler(inputCols=vec_cols, outputCol='feature')
        pipeline = Pipeline(stages=string_indexers + encoders + multi_enc + [assembler])

        self._model = pipeline.fit(train)
        runtime.feature_vocabulary = self._extract_vocabulary()
        runtime.input_dim = self.feature_dim()
        runtime.feature_names = self.get_featue_names()

        train_res = self._model.transform(train)
        test_res = self._model.transform(test)

        v = train_res.select('feature').take(1)[0].feature
        logger.info('[%s] feature vector size = %d', self._job_id, v.size)
        if v.size != self.feature_dim():
            raise RuntimeError(f'[{self._job_id}] feature vector size not match,'
                               f' real({v.size}) != calc({self.feature_dim()})')

        # write data to hdfs
        for df, subdir in [(train_res, 'train'), (test_res, 'test')]:
            logger.info('[%s] number of %s data partitions %d', self._job_id, subdir, df.rdd.getNumPartitions())
            runtime.data_schema = to_hdfs(df, os.path.join(runtime.hdfs_dir, subdir)),
            #self._category_feature + self._multi_value_category_feature)

        logging.info('[%s] finish prepare data', self._job_id)

        return train_res, test_res

    def _extract_vocabulary(self):

        def _build_dict(name, values):
            return {
                'name': name,
                'value': ['' if v == 'nan' else v for v in values]
            }

        vocabulary = []
        for idx, name in enumerate(self._category_feature):
            m = self._model.stages[idx]
            vocabulary.append(_build_dict(name, m.labels))

        for m in self._model.stages:
            if isinstance(m, MultiCategoryEncoderModel):
                vocabulary.append(_build_dict(m.getInputCol(), m.getVocabulary()))

        self._vocabulary = vocabulary
        return vocabulary

    def feature_dim(self):
        dim = 0
        for feature in self._vocabulary:
            dim += len(feature['value'])
        return dim

    def save_feature_index_map(self, filename):
        f = None
        try:
            f = open(filename, mode='w', encoding='utf-8')
            idx = 0
            for fe in self._vocabulary:
                name = fe['name']
                for v in fe['value']:
                    f.write(f'{name}_{v}\t{idx}\n')
                    idx += 1
            logger.info('[%s] finish write %s', self._job_id, filename)
        except Exception as e:
            logger.error('[%s] fail to write %s', self._job_id, e)
            raise e
        finally:
            if f:
                f.close()
                f = None

    def get_feature_opts(self):
        res = [{'name': name, 'opt': 'one-hot'} for name in self._category_feature]
        res += [{'name': name, 'opt': 'multi-one-hot'} for name in self._multi_value_category_feature]
        return res

    def save_feature_opts(self, filename):
        f = None
        try:
            f = open(filename, mode='w', encoding='utf-8')
            for i in self.get_feature_opts():
                f.write(f'{json.dumps(i)}\n')
            logger.info('[%s] success, write feature file: %s', self._job_id, filename)
        except Exception as e:
            logging.error('[%s] fail write %s', self._job_id, e)
            raise e
        finally:
            if f:
                f.close()
                f = None

    def get_featue_names(self):
        return self._category_feature + self._multi_value_category_feature

    @staticmethod
    def expend_fields(raw, ext_feature):
        # append fields
        from functools import reduce
        ext_dict = 'ext_dict'
        opts = [
            ('weekday', udfs.weekday('ts')),
            ('is_weekend', udfs.is_weekend('ts')),
            (ext_dict, udfs.to_ext_dict('ext_key', 'ext_value')),
        ]

        raw = reduce(lambda d, args: d.withColumn(*args), opts, raw)

        # extract cap feature & bidding feature from ext_dict
        raw = reduce(lambda df, c: df.withColumn(c, df[ext_dict].getItem(c)), ext_feature, raw)

        return raw.drop(ext_dict, 'ext_key', 'ext_value')
