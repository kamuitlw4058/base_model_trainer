from abc import ABC,abstractmethod

class FeatureEncoder(ABC):

    @abstractmethod
    def encoder(self,raw, features,multi_value_feature):
        return
