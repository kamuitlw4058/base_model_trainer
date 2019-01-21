from libs.model.trainer.trainer_factory import TrainerFactory
from libs.model.trainer.tf_deepfm.tf_deepfm_trainer import TF_DeepFM_Trainer


class TF_DeepFM_TrainerFactory(TrainerFactory):

    def __init__(self,job):
        self._job = job
        self._trainer = TF_DeepFM_Trainer(self._job.job_name,
                                          self._job.hdfs_dir,
                                          self._job.local_dir,
                                          self._job.learning_rate,
                                          self._job.l2)

    def get_trainer(self):
        return self._trainer
