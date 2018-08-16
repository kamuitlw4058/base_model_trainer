from abc import ABC,abstractmethod

class Predictor(ABC):


    @staticmethod
    @abstractmethod
    def get_model_name():
        return

    @abstractmethod
    def predict(self, worker_num, input_dim, data_names):
        return

    @abstractmethod
    def evaluate_auc(self, data_names):
        return
