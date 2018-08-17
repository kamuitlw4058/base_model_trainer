from libs.feature.feature_encoder_factory import FeatureEncoderFactory
from libs.feature.runtine_feature_encoder import RuntineFeatureEncoder


class RuntineFeatureEncoderFactory(FeatureEncoderFactory):
    def __init__(self,job):
        self._job_id = job.job_name
        self._encoder =  RuntineFeatureEncoder(self._job_id)

    def get_feature_encoder(self):
        return self._encoder
