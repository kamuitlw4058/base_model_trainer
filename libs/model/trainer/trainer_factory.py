from libs.model.trainer.tflr.tflr_trainer import TFLRTrainer

class TrainerFactory():
    @staticmethod
    def get_trainer(trainer_name,**kwargs):

        trainer= None

        if trainer_name == "tflr":
            trainer =  TFLRTrainer(kwargs['name'],
                        kwargs['hdfs_dir'],
                        kwargs['local_dir'])
        return trainer



