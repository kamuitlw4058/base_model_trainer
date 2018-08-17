from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.trainer.tflr.tflr_trainer import TFLRTrainer


class TFLRTrainerFactory(TrainerFactory):

    def __init__(self,job):
        self._job = job
        self._trainer = TFLRTrainer(self._job.job_name,
                                    self._job.hdfs_dir,
                                    self._job.local_dir,
                                    self._job.learning_rate,
                                    self._job.l2)

    def get_trainer(self):
        return self._trainer
