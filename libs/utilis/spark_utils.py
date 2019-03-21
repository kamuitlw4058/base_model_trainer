
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType



def read_csv(fileurl, spark:SparkSession, has_header=True, delimiter=',', schema_names=None):
    fields = [StructField(field_name, StringType(), True) for field_name in schema_names]
    schema = StructType(fields)

    reader = spark.read.format("com.databricks.spark.csv")
    reader = reader.option("header", has_header)
    reader = reader.option("delimiter", delimiter)
    if schema_names is not None:
        reader = reader.schema(schema)
    df = reader.load(fileurl)

    return df

