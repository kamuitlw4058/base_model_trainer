import logging
logger = logging.getLogger(__name__)

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import functions
from libs.feature.multi_category_encoder import MultiCategoryEncoder, MultiCategoryEncoderModel
from libs.feature.feature_encoder import FeatureEncoder

class RuntineFeatureEncoder(FeatureEncoder):
    def __init__(self,job_id):
        self._job_id  = job_id
        self._model = None
        self._vocabulary = None
        self._category_feature = []
        self._multi_value_category_feature = []
        self._dim = -1

    def encoder(self,raw, features,multi_value_category_feature,train_ratio=0.9,test_ratio=0.1):

        logger.info(' columns %s',  raw.schema.names)

        self._category_feature = [i for i in features if raw.agg(functions.countDistinct(raw[i]).alias('cnt')).collect()[0].cnt > 1]
        logger.info('drop unique feature %s', set(features) - set(self._category_feature))

        # 这边对先拆分，原始数据，再进行特征转换，是为了避免训练时，用到了测试集的一些OneHot的特征信息。
        # 原因是，我们的算法对
        train, test = raw.randomSplit([train_ratio, test_ratio])
        logger.info('[%s] data statics: total = %d, train = %d, test = %d', self._job_id, raw.count(), train.count(), test.count())

        # one hot encoding
        string_indexers = [StringIndexer(inputCol=c, outputCol="{}_idx".format(c), handleInvalid='keep')
                           for c in self._category_feature]
        encoders = [OneHotEncoder(inputCol="{}_idx".format(c), outputCol="{}_vec".format(c), dropLast=True)
                    for c in self._category_feature]

        self._multi_value_category_feature = multi_value_category_feature

        multi_enc = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_vec') for i in self._multi_value_category_feature]

        vec_cols = ['{}_vec'.format(c) for c in self.get_feature_names()]
        assembler = VectorAssembler(inputCols=vec_cols, outputCol='feature')
        pipeline = Pipeline(stages=string_indexers + encoders + multi_enc + [assembler])

        self._model = pipeline.fit(train)

        self._extract_vocabulary()

        train_res = self._model.transform(train)

        # 这边使用训练集的转换模型去转换测试集。
        test_res = self._model.transform(test)

        v = train_res.select('feature').take(1)[0].feature
        logger.info('feature vector size = %d',  v.size)
        if v.size != self.feature_dim():
            raise RuntimeError(f'feature vector size not match,'
                               f' real({v.size}) != calc({self.feature_dim()})')

        return train_res, test_res

    def get_feature_names(self):
        return self._category_feature + self._multi_value_category_feature

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
        if self._dim == -1:
            self._dim = 0
            for feature in self._vocabulary:
                self._dim += len(feature['value'])
        return self._dim