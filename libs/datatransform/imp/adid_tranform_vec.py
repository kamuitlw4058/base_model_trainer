import logging
logger = logging.getLogger(__name__)
import logging
logger = logging.getLogger(__name__)
from libs.datatransform.datatranform import DataTranform
from libs.utilis.spark_utils import read_csv
from libs.processing.udf.wrapper_udf import split_to_list_udf, value_dict_index_udf
from pyspark.sql.types import *


class AdidVecTranform(DataTranform):


    def __init__(self, name,spark=None,parallel=False,**kwargs):
        super().__init__(name)
        self.model = None
        self._args = kwargs
        self._spark = spark


    def tranform(self,df):
        word_vec_file = "hdfs:///user/model/extend_data/ad_vec_1.txt"
        #word_vec_file = "hdfs:///user/model/extend_data/adid_emb_res.csv"
        word_vec_df = read_csv(word_vec_file, spark=self._spark, has_header=False,
                               delimiter='\t',
                               schema_names=['adid', 'adid_vec'])
        print(f"read word_vecv from:{word_vec_file}")
        apply_udf = split_to_list_udf(" ", DoubleType()).get_udf()
        word_vec_df = word_vec_df.withColumn("adid_arr", apply_udf("adid_vec"))
        word_vec_list = word_vec_df.select(["adid","adid_arr"]).toPandas().to_dict('records')

        word_vec_dict = {}

        for row in word_vec_list:
            word_vec_dict[row['adid']] = row['adid_arr']



        apply_udf = value_dict_index_udf(word_vec_dict, [0.0 for i in range(8)],
                                        output_type=ArrayType(DoubleType())).get_udf()
        df = df.withColumn("Bid_AdId_emb", apply_udf('Bid_AdId'))
        df.select(["Bid_AdId","Bid_AdId_emb"]).show(10)


        return df

    @staticmethod
    def get_type():
        return 'AdidVecTranform'


