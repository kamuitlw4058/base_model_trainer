from abc import ABC,abstractmethod


class DataSourceFactory(ABC):

    @abstractmethod
    def get_datasource(self,job):
        """read from soure"""
        return


