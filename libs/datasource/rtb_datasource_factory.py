from libs.datasource.datasource_factory import DataSoureFactory
from libs.datasource.rtb_datasource import RTBDataSource
from conf.conf import RTB_ALL_TABLE_NAME, RTB_LOCAL_TABLE_NAME,CLICKHOUSE_URL_TEMPLATE

class RTBDataSourceFactory(DataSoureFactory):

    def _get_url_template(self):
        return CLICKHOUSE_URL_TEMPLATE

    def _get_rtb_all_table_name(self):
        return RTB_ALL_TABLE_NAME

    def _get_rtb_local_table_name(self):
        return RTB_LOCAL_TABLE_NAME


    def __init__(self,job):
        self._job = job
        #    def __init__(self,job_id,local_dir,filters,pos_proportion,neg_proportion,url_template,table,account,vendor):
        self._datasource = RTBDataSource(job.job_name,
                                         job.local_dir,
                                         job.filters,
                                         job.pos_proportion,
                                         job.neg_proportion,
                                         self._get_url_template(),
                                         self._get_rtb_all_table_name(),
                                         self._get_rtb_local_table_name(),
                                         job.account,
                                         job.vendor,
                                         job.start_date,
                                         job.end_date,
                                         job.test_start_date,
                                         job.test_end_date,
                                         job.new_features)



    def get_datasource(self):
        return self._datasource
