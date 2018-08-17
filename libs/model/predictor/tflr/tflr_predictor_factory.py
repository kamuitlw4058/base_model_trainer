from libs.model.predictor.tflr.tflr_predictor import TFLRPredictor
from libs.model.predictor.predictor_factory import PredictorFactory


class TFLRPredictorFactory(PredictorFactory):

    def __init__(self,job):
        self._job = job
        self._predictor = TFLRPredictor(self._job.job_name,
                                    self._job.hdfs_dir,
                                    self._job.local_dir)

    def get_predictor(self):
        return self._predictor
