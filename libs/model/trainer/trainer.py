from abc import ABC,abstractmethod

class Trainer(ABC):


    @staticmethod
    @abstractmethod
    def get_model_name():
        return

    @abstractmethod
    def train(self, epoch, batch_size, worker_num, input_dim, data_name):
        return

    @abstractmethod
    def print_features_weight(self, features_list):
        return

    @abstractmethod
    def save_features_weight(self, filename):
        return



