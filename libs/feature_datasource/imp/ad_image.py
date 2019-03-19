import logging
logger = logging.getLogger(__name__)
from libs.feature_datasource.imp.clickhouse_sql import  ClickHouseSQLDataSource
from pyspark.sql.dataframe import DataFrame
from libs.common.database.reader import get_mysql_reader
from libs.feature.model_udfs import image_vector as image_vector_udf
from conf import hadoop as hadoop_conf
from libs.utilis.dict_utils import get_simple_str

adid_sql = """SELECT distinct toShortId(Bid_AdId) as Bid_AdId
FROM zampda.rtb_all prewhere EventDate >= toDate('{start_date}') 
AND EventDate <= toDate('{end_date}') 
AND TotalErrorCode=0
WHERE Media_VendorId = {vendor}
    AND Bid_CompanyId = {account}
    AND notEmpty(Impression.Timestamp)
"""


class AdImage(ClickHouseSQLDataSource):


    def __init__(self, name, start_date,end_date,spark,parallel=False,**kwargs):
        super().__init__(name, adid_sql.format(start_date=start_date,end_date=end_date,**kwargs), spark, parallel=parallel)
        self.model = None
        self._args = kwargs
        self._args['start_date'] = start_date
        self._args['end_date'] = end_date


    def get_dataframe(self):
        output_path = self.get_image_vector_output_filepath(**self._args)
        print(f"read dataframe from:{output_path}")
        ret_df = self._spark.read.parquet(output_path)
        #ret_df.show(10)
        return ret_df

    @staticmethod
    def get_type():
        return 'AdImage'



    @staticmethod
    def download_image(adid):
        pass

    def get_output_filepath(self,**kwargs):
        output_file = f"{self.get_type()}{get_simple_str(**kwargs)}"
        output_path = f"{hadoop_conf.HDFS_FEATURE_ROOT}/{str(self._name).format(**kwargs)}/{output_file}"
        return output_path

    def get_image_vector_output_filepath(self,**kwargs):
        output_file = f"{self.get_type()}_image_vector{get_simple_str(**kwargs)}"
        output_path = f"{hadoop_conf.HDFS_FEATURE_ROOT}/{str(self._name).format(**kwargs)}/{output_file}"
        return output_path

    def produce_data(self,overwrite=False,df_handler=None,write_df=True)->DataFrame:
        df = super().produce_data(overwrite=True,write_df =False)


        df =df.withColumn("adimage", image_vector_udf("Bid_AdId"))

        output_path = self.get_image_vector_output_filepath(**self._args)

        df.write.parquet(path=output_path, mode='overwrite')
        return df
