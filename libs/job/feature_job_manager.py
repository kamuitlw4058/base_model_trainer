from abc import ABC,abstractmethod


class JobManager(ABC):

    @abstractmethod
    def get_datasource(self):
        return

    @abstractmethod
    def get_feature_encoder(self):
        return


    @abstractmethod
    def get_dataoutput(self):
        return


