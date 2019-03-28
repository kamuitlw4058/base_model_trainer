import logging
logger = logging.getLogger(__name__)
from pyspark.sql.dataframe import DataFrame
from conf import hadoop as hadoop_conf
from libs.utilis.dict_utils import get_simple_str
import logging
logger = logging.getLogger(__name__)
from libs.datasource.datasource import DataSource
from libs.utilis.spark_utils import read_csv
from libs.processing.udf.wrapper_udf import split_to_list_udf,list_dict_index_udf,list_dict_has_key_udf,list_avg_udf
from pyspark.sql.types import *
from libs.env.hdfs import hdfs

class Spark(DataSource):

    def __init__(self, name,spark,handler=None,parallel=False,**kwargs):
        super().__init__(name)
        self.model = None
        self._args = kwargs
        self._spark = spark
        self._handler = handler


    def get_dataframe(self):
        output_path = self.get_output_filepath(**self._args)
        print(f"read dataframe from:{output_path}")
        ret_df:DataFrame = self._spark.read.parquet(output_path)
        ret_df.show(10)
        return ret_df

    @staticmethod
    def get_type():
        return 'Spark'



    def get_output_filepath(self,**kwargs):
        output_file = f"{self.get_type()}{get_simple_str(**kwargs)}"
        output_path = f"{hadoop_conf.HDFS_FEATURE_ROOT}/{str(self._name).format(**kwargs)}/{output_file}"
        return output_path

    def _produce_data(self):
        if self._handler is not None:
            df =self._handler(self._spark)
        else:
            raise RuntimeError("This is a abstractmethod please register")
        return df

    def produce_data(self,overwrite=False,df_handler=None,write_df=True)->DataFrame:
        df = None
        output_path = self.get_output_filepath(**self._args)
        if not hdfs.exists(output_path):
            logger.info("file [{path}] is not exist! we process data from clickhouse."
                        .format(path=output_path))
            df = self._produce_data()
            df.write.parquet(path=output_path, mode='overwrite')

        else:
            if overwrite:
                logger.info("file [{path}] is  exist! we overwrite process data from clickhouse."
                            .format(path=output_path))
                df = self._produce_data()
                df.write.parquet(path=output_path, mode='overwrite')

        return df
