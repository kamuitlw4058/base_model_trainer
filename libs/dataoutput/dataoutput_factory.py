from abc import ABC,abstractmethod


class DataOutputFactory(ABC):

    @abstractmethod
    def get_dataoutput(self):
        return
