import logging
logger = logging.getLogger(__name__)


from libs.datasource.rtb_datasource_factory import RTBDataSourceFactory
from libs.feature.runtine_feature_encoder_factory import RuntineFeatureEncoderFactory
from libs.dataoutput.rtb_dataouput_factory import RTBDataOutputFactory
from libs.job.feature_job_manager import JobManager
from libs.model.trainer.tflr.tflr_trainer_factory import TFLRTrainerFactory
from libs.model.predictor.tflr.tflr_predictor_factory import TFLRPredictorFactory


class FeatureJobManger(JobManager):

    @staticmethod
    def get_batch_size( clk_num):
        batch_size = 0
        if clk_num < 10000:
            batch_size = 32
        elif clk_num < 10 * 10000:
            batch_size = 64
        elif clk_num < 20 * 10000:
            batch_size = 64
        elif clk_num < 40 * 10000:
            batch_size = 128
        else:
            batch_size = 128

        return batch_size

    @staticmethod
    def get_epoch_num( clk_num):
        epoch = 0
        if clk_num < 10000:
            epoch = 10
        elif clk_num < 10 * 10000:
            epoch = 3
        elif clk_num < 20 * 10000:
            epoch = 2
        elif clk_num < 40 * 10000:
            epoch = 2
        else:
            epoch = 2

        return epoch

    @staticmethod
    def get_worker_num(data_size):
        if data_size < 30_0000:
            return 1
        elif data_size < 100_0000:
            return 2
        elif data_size < 500_0000:
            return 4
        elif data_size < 1000_0000:
            return 8
        elif data_size < 4000_0000:
            return 16
        else:
            return 64

    def __init__(self,job):
        self._job = job
        self._dataoutput = RTBDataOutputFactory()
        self._datasource = RTBDataSourceFactory(job)
        self._feature_encoder = RuntineFeatureEncoderFactory(job)
        self._trainer = TFLRTrainerFactory(job)
        self._predictor = TFLRPredictorFactory(job)


    def get_datasource(self):
        return self._datasource.get_datasource()

    def get_feature_encoder(self):
        return self._feature_encoder.get_feature_encoder()

    def get_dataoutput(self):
        return self._dataoutput.get_dataoutput()

    def get_trainer(self):
        return self._trainer.get_trainer()

    def get_predictor(self):
        return self._predictor.get_predictor()

    def get_trainer_params(self):
        clk_sum,imp_sum = self.get_datasource().get_clk_imp()
        epoch =self.get_epoch_num(clk_sum)
        batch_size = self.get_batch_size(clk_sum)
        executor_num = self.get_datasource().get_executor_num()
        worker_num = min(self.get_worker_num(clk_sum),executor_num)
        input_dim = self.get_feature_encoder().feature_dim()
        #epoch,batch_size,worker_num,input_dim, data_name):
        return epoch,batch_size,worker_num,input_dim










