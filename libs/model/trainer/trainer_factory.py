from abc import ABC,abstractmethod

class TrainerFactory(ABC):

    @abstractmethod
    def get_trainer(self):
        return



