from abc import ABC,abstractmethod


class DataSoureFactory(ABC):

    @abstractmethod
    def get_datasource(self,job):
        """read from soure"""
        return



