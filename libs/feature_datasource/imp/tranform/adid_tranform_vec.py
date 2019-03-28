import logging
logger = logging.getLogger(__name__)
from pyspark.sql.dataframe import DataFrame
from conf import hadoop as hadoop_conf
from libs.utilis.dict_utils import get_simple_str
import logging
logger = logging.getLogger(__name__)
from libs.feature_datasource.datatranform import DataTranform
from libs.utilis.spark_utils import read_csv
from libs.feature.udf.wrapper_udf import split_to_list_udf,list_dict_index_udf,list_dict_has_key_udf,list_avg_udf,list_sum_udf,value_dict_index_udf
from pyspark.sql.types import *
from libs.env.hdfs import hdfs

class AdidVecDataTranform(DataTranform):


    def __init__(self, name,spark=None,parallel=False,**kwargs):
        super().__init__(name)
        self.model = None
        self._args = kwargs
        self._spark = spark


    def tranform(self,df):

        word_vec_file = "hdfs:///user/model/extend_data/adid_emb_res.csv"
        word_vec_df = read_csv(word_vec_file, spark=self._spark, has_header=False,
                               delimiter='\t',
                               schema_names=['adid', 'adid_vec'])
        print(f"read word_vecv from:{word_vec_file}")
        apply_udf = split_to_list_udf(" ", DoubleType()).get_udf()
        word_vec_df = word_vec_df.withColumn("adid_arr", apply_udf("adid_vec"))
        word_vec_list = word_vec_df.toPandas().to_dict('records')

        word_vec_dict = {}

        for row in word_vec_list:
            word_vec_dict[row['adid']] = row['adid_arr']



        apply_udf = value_dict_index_udf(word_vec_dict, [0.0 for i in range(16)],
                                        output_type=ArrayType(DoubleType())).get_udf()
        df = df.withColumn("Bid_AdId_emb", apply_udf('Bid_AdId'))
        #
        #
        # user_df = user_df.withColumn("adid_clk_vec_list", apply_udf('clk_adid'))
        # apply_udf = list_avg_udf(16).get_udf()
        # user_df = user_df.withColumn("adid_vec_avg", apply_udf('adid_vec_list'))
        # user_df = user_df.withColumn("adid_clk_vec_avg", apply_udf('adid_clk_vec_list'))
        # apply_udf = list_sum_udf(16).get_udf()
        # user_df = user_df.withColumn("adid_vec_sum", apply_udf('adid_vec_list'))
        # user_df = user_df.withColumn("adid_clk_vec_sum", apply_udf('adid_clk_vec_list'))
        # df = user_df.select(["Id_Zid", "adid_vec_avg", "adid_vec_sum", "adid_clk_vec_avg", "adid_clk_vec_sum"])
        # df.write.parquet(path=output_path, mode='overwrite')


        return df

    @staticmethod
    def get_type():
        return 'Bid_AdId_Seq_Vec_Tran'


