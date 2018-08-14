from abc import ABC,abstractmethod


class DataSoureFactory(ABC):

    @abstractmethod
    def getDataSource(self,job):
        """read from soure"""
        return



