from abc import ABC,abstractmethod

class FeatureEncoder(ABC):

    @abstractmethod
    def encoder(self,raw, test,features,multi_value_feature,number_features=None):
        return

    @abstractmethod
    def feature_dim(self):
        return

    @abstractmethod
    def get_feature_names(self):
        return

    @abstractmethod
    def get_feature_list(self):
        return