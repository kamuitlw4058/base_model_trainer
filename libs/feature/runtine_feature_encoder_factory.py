from libs.feature.feature_encoder_factory import FeatureEncoderFactory
from libs.feature.runtine_feature_encoder import RuntineFeatureEncoder


class RuntineFeatureEncoderFactory(FeatureEncoderFactory):

    def get_feature_encoder(self, job):
        return RuntineFeatureEncoder()
