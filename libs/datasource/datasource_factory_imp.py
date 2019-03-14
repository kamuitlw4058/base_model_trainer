from libs.datasource.datasource_factory import DataSourceFactory
from libs.datasource.rtb_datasource import RTBDataSource
from conf.conf import RTB_ALL_TABLE_NAME, RTB_LOCAL_TABLE_NAME,CLICKHOUSE_URL_TEMPLATE
from libs.datasource.file_datasource import FileDataSource
from libs.env.spark import spark_session

class RTBDataSourceFactory(DataSourceFactory):

    def _get_url_template(self):
        return CLICKHOUSE_URL_TEMPLATE

    def _get_rtb_all_table_name(self):
        return RTB_ALL_TABLE_NAME

    def _get_rtb_local_table_name(self):
        return RTB_LOCAL_TABLE_NAME


    def __init__(self,job):
        self._job = job
        if job.datasource == 'rtb':
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
                                         job.new_features,
                                        job.new_features_args)
        elif job.datasource == 'file':

            self._datasource = FileDataSource(
                job.job_name,
                job.local_dir,
                job.filepath,
                job.filetype,
                session= None
            )


    def get_datasource(self):
        return self._datasource
