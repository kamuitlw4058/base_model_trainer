from libs.dataoutput.dataoutput import DataOutput
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType

class RTBDataOutput(DataOutput):

    def write_hdfs(self,df,path,raw_cols=None):
        @functions.udf(ArrayType(IntegerType()))
        def vector_indices(v):
            return [int(i) for i in v.indices]

        df = df.withColumn('feature_indices', vector_indices('feature'))

        schema = ['is_clk', 'feature_indices']
        # others = [c for c in df.columns if c not in schema]
        res = (df
               # .select(['is_clk', 'feature'])
               .orderBy(functions.rand())
               .select(schema + (raw_cols if raw_cols else []))
               )

        res.write.parquet(path=path, mode='overwrite')
        return res.columns
