from libs.processing.imp.processing_base import ProcessingBase

import logging
logger = logging.getLogger(__name__)

from pyspark.ml.feature import OneHotEncoder, StringIndexer


class OneHotProcessing(ProcessingBase):

    @staticmethod
    def get_type():
        return "pipline"

    @staticmethod
    def get_name():
        return "onehot"

    @staticmethod
    def get_processor(cols):
        string_indexers = [StringIndexer(inputCol=c, outputCol="{}_idx".format(c), handleInvalid='keep')
                           for c in cols]

        encoders = [OneHotEncoder(inputCol="{}_idx".format(c), outputCol="{}_vec".format(c), dropLast=True)
                    for c in cols]

        return string_indexers + encoders,zip(string_indexers,cols), ['{}_vec'.format(c) for c in cols]

    @staticmethod
    def get_vocabulary(stage,col):
      return col,stage.labels
