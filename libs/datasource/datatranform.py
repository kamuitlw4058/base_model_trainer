from abc import ABC,abstractmethod

class DataTranform(ABC):

    def __init__(self, name,args=None):
        self._name = name
        self._args = args

    def get_name(self):
        return self._name


    def get_args(self):
        return self._args

    @abstractmethod
    def tranform(self,dataframe):
        return

    @staticmethod
    @abstractmethod
    def get_type():
        return
