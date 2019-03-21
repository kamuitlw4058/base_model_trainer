import logging
logger = logging.getLogger(__name__)
from pyspark.sql.dataframe import DataFrame
from conf import hadoop as hadoop_conf
from libs.utilis.dict_utils import get_simple_str
import logging
logger = logging.getLogger(__name__)
from libs.feature_datasource.datasource import DataSource
from libs.utilis.spark_utils import read_csv
from libs.feature.udf.wrapper_udf import split_to_list_udf,list_dict_index_udf,list_dict_has_key_udf,list_avg_udf
from pyspark.sql.types import *

class AdidVecDataSource(DataSource):


    def __init__(self, name, start_date,end_date,spark,parallel=False,**kwargs):
        super().__init__(name)
        self.model = None
        self._args = kwargs
        self._spark = spark
        self._args['start_date'] = start_date
        self._args['end_date'] = end_date


    def get_dataframe(self):
        output_path = self.get_image_vector_output_filepath(**self._args)
        print(f"read dataframe from:{output_path}")
        ret_df:DataFrame = self._spark.read.parquet(output_path)
        #print(ret_df.columns)
        #rint(ret_df.dtypes)
        ret_df.show(10)
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





    def produce_data(self,overwrite=False,df_handler=None,write_df=True)->DataFrame:

        user_df  = read_csv( "hdfs:///user/model/extend_data/user.txt", spark=self._spark, has_header=False,
                           delimiter='\t',
                           schema_names=['zid', 'imp', 'clk', 'imp_adid', 'clk_adid'])



        word_vec_df = read_csv( "hdfs:///user/model/extend_data/word_vec.txt", spark=self._spark, has_header=False,
                           delimiter='\t',
                           schema_names=['adid', 'adid_vec'])

        apply_udf = split_to_list_udf(" ", DoubleType()).get_udf()
        word_vec_df = word_vec_df.withColumn("adid_arr", apply_udf("adid_vec"))
        word_vec_list = word_vec_df.toPandas().to_dict('records')

        word_vec_dict = {}

        for row in word_vec_list:
            word_vec_dict[row['adid']] = row['adid_arr']

        apply_udf = list_dict_index_udf(word_vec_dict, [0.0 for i in range(32)],
                                        output_type=ArrayType(DoubleType())).get_udf()
        user_df = user_df.withColumn("adid_vec_list", apply_udf('imp_adid'))
        apply_udf = list_avg_udf().get_udf()
        user_df = user_df.withColumn("adid_vec_avg", apply_udf('adid_vec_list'))
        df =user_df.select(["zid", "adid_vec_avg"])
        df.show(10)

        return df
