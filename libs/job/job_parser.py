import logging
logger = logging.getLogger(__name__)

from datetime import datetime
from libs.job.feature_job import  FeatureJob
from libs.job.feature_job_manager_imp import FeatureJobManger
import os
from conf.conf import JOB_ROOT_DIR

def parser(options):
    job = FeatureJob()
    job.account = options.account
    job.vendor = options.vendor
    if not options.job_name:
        job_name = options.feature_name + "_{ts:%y%m%d_%H%M%S}".format(ts=datetime.now())
    else:
        job_name = options.job_name
    job.job_name = job_name

    job.local_dir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, job.job_name)
    job.hdfs_dir = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, job.job_name)

    job.start_date = options.start_date
    job.end_date = options.end_date
    job.test_start_date = options.test_start_date
    job.test_end_date = options.test_end_date
    job.new_features = options.new_features

    #TODO: 这边后续如何传入参数，数据格式，配置文件格式都需要确认
    with open(options.filters) as filter_file:
        filter_str = filter_file.read()
        logging.debug("filter_str:" + filter_str)
        if filter_str:
            job.filters = eval(filter_str)
        else:
            raise RuntimeError("get filter list failed! ")

    if not options.pos_proportion:
        job.pos_proportion = 1
    else:
        job.pos_proportion = options.pos_proportion

    if not options.neg_proportion:
        job.neg_proportion = 2
    else:
        job.neg_proportion = options.neg_proportion

    job.learning_rate = options.learning_rate
    job.l2 = options.l2




    return [job]



