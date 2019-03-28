from abc import ABC,abstractmethod

class DataOutput(ABC):

    @staticmethod
    @abstractmethod
    def write(df,path):
        return




