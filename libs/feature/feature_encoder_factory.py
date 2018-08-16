from abc import ABC,abstractmethod


class FeatureEncoderFactory(ABC):

    @abstractmethod
    def get_feature_encoder(self):
        """read from soure"""
        return



