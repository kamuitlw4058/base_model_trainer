#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "mark"
__email__ = "mark@zamplus.com"


from pyspark import keyword_only
from pyspark.sql.functions import *
from pyspark.ml import Estimator, Transformer
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasOutputCol


class HasThreshold(Params):
    threshold = Param(Params._dummy(), "threshold", 'pruning threshold', typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super().__init__()

    def setThreshold(self, value=0.001):
        return self._set(threshold=value)

    def getThreshold(self):
        return self.getOrDefault(self.threshold)


class HasVocabulary(Params):
    vocabulary = Param(Params._dummy(), "vocabulary", 'vocabulary for transfor', typeConverter=TypeConverters.toListString)

    def __init__(self):
        super().__init__()

    def setVocabulary(self, value):
        return self._set(vocabulary=value)

    def getVocabulary(self):
        return self.getOrDefault(self.vocabulary)


class HasSepChar(Params):
    sepChar = Param(Params._dummy(), "sepChar", 'string seperator', typeConverter=TypeConverters.toString)

    def __init__(self):
        super().__init__()

    def setSepChar(self, value):
        return self._set(sepChar=value)

    def getSepChar(self):
        return self.getOrDefault(self.sepChar)


class MultiCategoryEncoderModel(Transformer, HasInputCol, HasOutputCol, HasSepChar, HasVocabulary):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, sepChar=None, vocabulary=None):
        super().__init__()
        self.setParams(**self._input_kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, sepChar=None, vocabulary=None):
        return self._set(**self._input_kwargs)

    def _transform(self, dataset):
        vocab = {n: i for i, n in enumerate(self.getVocabulary())}
        size = len(vocab)
        sepChar = self.getSepChar()

        @udf(returnType=VectorUDT())
        def to_vec(s):
            idx = [vocab[n] for n in s.split(sepChar) if n in vocab]
            idx = list(set(idx))
            idx.sort()
            val = [1] * len(idx)
            return Vectors.sparse(size, idx, val)

        return dataset.withColumn(self.getOutputCol(), to_vec(self.getInputCol()))


class MultiCategoryEncoder(Estimator, HasInputCol, HasOutputCol, HasSepChar, HasThreshold):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, sepChar=',', threshold=0.001):
        super().__init__()
        self._setDefault(sepChar=sepChar, threshold=threshold)
        self.setParams(**self._input_kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, sepChar=None, threshold=None):
        return self._set(**self._input_kwargs)

    def _fit(self, dataset):
        inputCol = self.getInputCol()
        outputCol = self.getOutputCol()
        col = f'{inputCol}_seped'
        sepChar = self.getSepChar()

        res = dataset.select(explode(split(inputCol, sepChar)).alias(col)).groupBy(col).count().toPandas()
        # remove '' value
        res = res[res[col] != ''].copy()
        # probability
        res['ratio'] = res['count'] / res['count'].sum()

        # pruning low variance value
        threshold = self.getThreshold()

        def binary_variance(p):
            return p * (1 - p)

        res = res[res['ratio'].apply(lambda r: binary_variance(r) >= threshold)]
        vocab = res[col].tolist()

        return (MultiCategoryEncoderModel()
            .setInputCol(inputCol)
            .setOutputCol(outputCol)
            .setVocabulary(vocab)
            .setSepChar(sepChar)
            )
