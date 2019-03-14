
from libs.model.predictor.tflr.tflr_predictor import TFLRPredictor

class PredictorFactory():


    @staticmethod
    def get_predictor(trainer_name,**kwargs):

        trainer= None

        if trainer_name == "tflr":
            trainer =  TFLRPredictor(kwargs['name'],
                        kwargs['hdfs_dir'],
                        kwargs['local_dir'])
        return trainer







