from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors, VectorUDT

@udf(MapType(StringType(), StringType()))
def to_ext_dict(ext_key, ext_val):
    return {k: v for k, v in zip(ext_key.split('#'), ext_val.split('#'))}


@udf(MapType(StringType(), StringType()))
def to_ctr_dict(ext_key, ext_val):
    return {k: v for k, v in zip(eval(ext_key), eval(ext_val)) if k[-3:] in ['clk', 'imp', 'ctr']}


# @udf(ArrayType(ArrayType(DoubleType)))
# def to_index_list(key_list):
#     return {k: v for k, v in zip(eval(ext_key), eval(ext_val)) if k[-3:] in ['clk', 'imp', 'ctr']}


@udf()
def to_string(v):
    return str(v)


@udf()
def weekday(dt):
    return f'{dt:%A}'.lower()

@udf(returnType=DoubleType())
def int_default_zero(values):
    if values:
        return values
    return 0.0


@udf()
def is_weekend(dt):
    return 'yes' if dt.weekday() >= 5 else 'no'


@udf(ArrayType(IntegerType()))
def vector_indices(v):
    return [int(i) for i in v.indices]

@udf(ArrayType(DoubleType()))
def vector_values(v):
    return [float(i) for i in v.values]


@udf(VectorUDT())
def to_vector(dt):
    return Vectors.dense(dt)

@udf(IntegerType())
def to_array_size(dt):
    if dt is not None:
        return len(dt)
    return 0

