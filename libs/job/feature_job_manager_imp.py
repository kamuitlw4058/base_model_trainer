import logging
logger = logging.getLogger(__name__)


from libs.datasource.rtb_datasource_factory import RTBDataSourceFactory
from libs.feature.runtine_feature_encoder_factory import RuntineFeatureEncoderFactory
from libs.dataoutput.rtb_dataouput_factory import RTBDataOutputFactory
from libs.job.feature_job_manager import JobManager

class FeatureJobManger(JobManager):
    def get_datasource_factory(self):
        return RTBDataSourceFactory()

    def get_feature_encoder_factory(self):
        return RuntineFeatureEncoderFactory()

    def get_dataoutput_factory(self):
        return RTBDataOutputFactory()









