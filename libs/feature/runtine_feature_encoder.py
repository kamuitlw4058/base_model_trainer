import logging
logger = logging.getLogger(__name__)

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import functions
from libs.feature.multi_category_encoder import MultiCategoryEncoder, MultiCategoryEncoderModel
from libs.feature.feature_encoder import FeatureEncoder
import  json
from libs.feature import udfs

class RuntineFeatureEncoder(FeatureEncoder):
    def __init__(self,job_id):
        self._job_id  = job_id
        self._model = None
        self._vocabulary = None
        self._category_feature = []
        self._multi_value_category_feature = []
        self._dim = -1

    def encoder(self,raw, test,features,multi_value_category_feature,number_features=None):

        logger.info(' schema %s', raw.schema)
        logger.info(' columns %s',  raw.schema.names)
        logger.info('features: %s', features)
        logger.info('multi_value_category_feature: %s',multi_value_category_feature)
        logger.info(f'input number feature:{number_features}')
        logger.info(f'input features :{features}')
        self._category_feature = [i for i in features if raw.agg(functions.countDistinct(raw[i]).alias('cnt')).collect()[0].cnt > 1]
        logger.info('drop unique feature %s', set(features) - set(self._category_feature))
        if number_features:
            self._number_features = [number_feature for number_feature in number_features if number_feature in  self._category_feature]
        else:
            self._number_features =[]
        logger.info(f'number feature:{self._number_features}')

        self._category_feature = list((set(self._category_feature) - set(self._number_features)))
        logger.info(f'one_hot feature:{self._category_feature}')

        # 这边对先拆分，原始数据，再进行特征转换，是为了避免训练时，用到了测试集的一些OneHot的特征信息。
        # 原因是，我们的算法对
        train,test = raw,test
        train_count,test_count =  train.count(), test.count()
        logger.info('[%s] data statics: total = %d, train = %d, test = %d', self._job_id, train_count + test_count, train_count,test_count)

        # one hot encoding
        string_indexers = [StringIndexer(inputCol=c, outputCol="{}_idx".format(c), handleInvalid='keep')
                           for c in self._category_feature]
        encoders = [OneHotEncoder(inputCol="{}_idx".format(c), outputCol="{}_vec".format(c), dropLast=True)
                    for c in self._category_feature]

        if multi_value_category_feature:
            self._multi_value_category_feature = multi_value_category_feature
            multi_enc = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_vec') for i in self._multi_value_category_feature]
        else:
            multi_enc =None

        vec_cols = ['{}_vec'.format(c) for c in self.get_feature_names()]

        if  len(self._number_features) > 0:
            vec_cols += self._number_features

        assembler = VectorAssembler(inputCols=vec_cols, outputCol='feature')

        if multi_value_category_feature:
            stages = string_indexers + encoders + multi_enc + [assembler]
        else:
            stages =string_indexers + encoders  + [assembler]

        pipeline = Pipeline(stages=stages)

        self._model = pipeline.fit(train)

        self._extract_vocabulary()

        train_res = self._model.transform(train)

        # 这边使用训练集的转换模型去转换测试集。
        test_res = self._model.transform(test)
        print(test_res)

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

        for m in self._number_features:
            vocabulary.append(_build_dict(m,[m + '_number']))
        #print(f"one-hot vocabulary col size:{vocabulary}")
        #vocabulary += len(self._number_features)
        #print(f"total vocabulary col size:{vocabulary}")
        self._vocabulary = vocabulary
        return vocabulary

    def feature_dim(self):
        if self._dim == -1:
            self._dim = 0
            for feature in self._vocabulary:
                self._dim += len(feature['value'])
        return self._dim

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
    def get_feature_list(self):
        l = []
        idx = 0
        for fe in self._vocabulary:
            name = fe['name']
            for v in fe['value']:
                l.append(f'{name}_{v}')
                idx += 1
        return l


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
