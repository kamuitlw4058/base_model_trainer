import logging
logger = logging.getLogger(__name__)

import pandas as pd
from sqlalchemy import create_engine
from libs.job.feature_job_manager_imp import FeatureJobManger
from libs.collection import  AttributeDict

class FeatureJob(AttributeDict):


    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.set(k, v)
        self._job_manager = None


    def __setattr__(self, key, value):
        self.__dict__[key] = value


    def set(self, key, value):
        self.__dict__[key] = value

    def set_job_manager(self,job_manager):
        self._job_manager = job_manager

    def get_job_manager(self):
        return self._job_manager


    def commit(self):
        df = pd.Series(self.__dict__).to_frame().transpose()
        config = {
            'host': '172.31.8.1',
            'database': 'db_max_rtb',
            'user': 'user_maxrtb',
            'password': 'C3YN138V',
        }
        eg = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}'.format(**config))
        df.to_sql(name='model_opt_log', con=eg, if_exists='append', index=False)
        logger.info('[%s] success commit to db', self.__dict__['job_id'])
