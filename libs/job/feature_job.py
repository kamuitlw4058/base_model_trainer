import logging
logger = logging.getLogger(__name__)

import pandas as pd
from sqlalchemy import create_engine
from libs.job.feature_job_manager_imp import FeatureJobManger
from libs.collection import  AttributeDict
from libs.job.tracker import Tracker

class FeatureJob(AttributeDict):


    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.set(k, v)
        self._job_manager = None


    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def set_status(self,value):
        self.set("status",value)
        #self.commit()

    def get(self,key):
        ret = None
        try:
            ret = self.__dict__[key]
        except:
            pass
        return  ret



    def set(self, key, value):
        self.__dict__[key] = value

    def set_job_manager(self,job_manager):
        self._job_manager = job_manager

    def get_job_manager(self):
        return self._job_manager


    def commit(self):
        config = {
            'host': '172.31.8.1',
            'database': 'db_max_rtb',
            'user': 'user_maxrtb',
            'password': 'C3YN138V',
        }
        eg = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}'.format(**config))

        tracker = Tracker()
        tracker.job_name = self.get("job_name")
        tracker.start_time = self.get("start_time")
        tracker.end_time = self.get("end_time")
        tracker.start_date = self.get("start_date")
        tracker.end_date = self.get("end_date")
        tracker.account = self.get("account")
        tracker.vendor = self.get("vendor")
        tracker.filter= self.get("filters")
        tracker.train_auc = self.get("train_auc")
        tracker.test_auc = self.get("test_auc")
        tracker.new_features = self.get("new_features")
        tracker.test_start_date = self.get("test_start_date")
        tracker.test_end_date = self.get("test_end_date")
        tracker.tag = self.get("tag")
        tracker.os = self.get("os")
        tracker.audience = self.get("audience")
        tracker.status = self.get("status")

        df = tracker.get_df()

        df.to_sql(name='train_opt_log', con=eg, if_exists='append', index=False)
        logger.info('[%s] success commit to db', self.job_name)
