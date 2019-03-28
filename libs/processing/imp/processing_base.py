from abc import ABC,abstractmethod


class ProcessingBase(ABC):


    @staticmethod
    @abstractmethod
    def get_type(cols):
        return

    @staticmethod
    @abstractmethod
    def get_processor(cols):
        return


    @staticmethod
    @abstractmethod
    def get_name():
        return

    @staticmethod
    @abstractmethod
    def get_vocabulary(stage,col):
        return