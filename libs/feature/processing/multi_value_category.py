from libs.feature.processing.processing_base import ProcessingBase

import logging
logger = logging.getLogger(__name__)


from libs.feature.multi_category_encoder import MultiCategoryEncoder, MultiCategoryEncoderModel


class MultiValueCategoryProcessing(ProcessingBase):

    @staticmethod
    def get_type():
        return "pipline"


    @staticmethod
    def get_name():
        return "multi_value"

    @staticmethod
    def get_processor(cols):
        multi_enc = [MultiCategoryEncoder(inputCol=i, outputCol=f'{i}_vec') for i in cols]
        #return string_indexers + encoders, zip(string_indexers, cols), ['{}_vec'.format(c) for c in cols]
        return multi_enc,zip(multi_enc,cols), ['{}_vec'.format(c) for c in cols]


    @staticmethod
    def get_vocabulary(stage,col):
      return stage.getInputCol(), stage.getVocabulary()