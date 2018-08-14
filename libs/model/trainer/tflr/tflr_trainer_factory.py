from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.trainer.tflr.tflr_trainer import TFLRTrainer


class TFLRTrainerFactory(TrainerFactory):

    def get_trainer(self):
        return TFLRTrainer()
