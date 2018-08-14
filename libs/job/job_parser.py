from datetime import datetime
from libs.job.feature_job import  FeatureJob
from libs.job.feature_job_manager_imp import FeatureJobManger
import os
from conf.conf import JOB_ROOT_DIR

def parser(options):
    job = FeatureJob
    job.account = options.account
    job.vendor = options.vendor
    if not options.job_name:
        job_name = options.feature_name + "_{ts:%y%m%d_%H%M%S}".format(ts=datetime.now())
    else:
        job_name = options.job_name
    job.job_name = job_name

    job.local_dir = os.path.join(JOB_ROOT_DIR.LOCAL_ROOT, job.id)
    job.hdfs_dir = os.path.join(JOB_ROOT_DIR.HDFS_ROOT, job.id)
    job.filters = options.filters

    if not options.pos_proportion:
        job.pos_proportion = 1

    if not options.neg_proportion:
        job.neg_proportion = 2

    job_manager = FeatureJobManger();
    job.set_job_mangaer(job_manager)

    return [job]