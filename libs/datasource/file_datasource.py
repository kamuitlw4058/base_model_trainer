import logging
logger = logging.getLogger(__name__)

from libs.datasource.datasource import DataSource
from libs.env.spark import spark_session

class FileDataSource(DataSource):

    def __init__(self,job_id,local_dir,path,filetype='csv',sep='\t',session=None):
        self._job_id = job_id
        self._local_dir = local_dir
        self._path = path
        self._filetype = filetype
        self._csv_sep = sep
        self._session = session
        self._datasource_df =None
        print(path,filetype,sep,session)


    def get_feature_datas(self):
        print("Get get_feature_datas csv file...")
        spark = spark_session(self._job_id,6,self._local_dir)
        self._session = spark
        if self._filetype == 'csv':
            logger.info("Get base data from csv file...")
            logger.info("csv url: " + str(self._path))
            datasource_df = self._session.read.csv(self._path, header=True, inferSchema=True, sep=self._csv_sep)
            logger.info("csv data length:" + str(datasource_df.count()))
            logger.info('csv data schema:' + str(datasource_df.schema.names))
            #logger.info("csv col:" + str(featureDf.))
            datasource_df.show(10)
            self._datasource_df = datasource_df
        elif self._filetype == 'parquet':
            logger.info("Get base data from parquet file...")
            logger.info("parquet url: " + str(self._path))

            datasource_df = self._session.read.parquet(self._path, header=True, inferSchema=True, sep=self._csv_sep)
            logger.info("parquet data length:" + str(datasource_df.count()))
            #logger.info("csv col:" + str(featureDf.))
            datasource_df.show(10)
            self._datasource_df = datasource_df

        train, test = self._datasource_df.randomSplit([0.9, 0.1])
        return train,test,self._datasource_df.schema.names,None,None

    def get_data_size(self):
        return

    def get_executor_num(self):
        return

    def close(self):
        return
