from abc import ABC,abstractmethod

class FeatureEncoder(ABC):

    @abstractmethod
    def encoder(self,raw, features,multi_value_feature):
        return

    @abstractmethod
    def feature_dim(self):
        return

    @abstractmethod
    def get_feature_names(self):
        return
