from abc import ABC,abstractmethod


class FeatureEncoderFactory(ABC):

    @abstractmethod
    def getFeatureEncoder(self,job):
        """read from soure"""
        return



