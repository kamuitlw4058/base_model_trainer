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



