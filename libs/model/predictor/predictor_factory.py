from abc import ABC,abstractmethod

class PredictorFactory(ABC):

    @abstractmethod
    def get_predictor(self):
        return



