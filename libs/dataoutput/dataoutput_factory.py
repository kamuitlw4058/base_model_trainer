from abc import ABC,abstractmethod


class DataOutputFactory(ABC):

    @abstractmethod
    def getDataOutput(self,job):
        return
