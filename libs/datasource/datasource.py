from abc import ABC,abstractmethod

class DataSource(ABC):

    @abstractmethod
    def get_feature_datas(self):
        return

    @abstractmethod
    def get_data_size(self):
        return

    @abstractmethod
    def get_executor_num(self):
        return

    @abstractmethod
    def close(self):
        return





