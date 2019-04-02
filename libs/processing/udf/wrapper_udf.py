from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors, VectorUDT



class vector_dense_udf:
    def __init__(self,size,default_value=0.0,output_type=VectorUDT(),**kwargs):
        self.size=size
        self.default_value = default_value
        self.output_type = output_type


    def get_udf(self):
        @udf(self.output_type)
        def warpped_udf(dt):
            if dt is None:
                return Vectors.dense([self.default_value for i in range(self.size)])
            else:
                return Vectors.dense(dt)

        return warpped_udf



class value_dict_index_udf:
    def __init__(self,dict,default_value,output_type=StringType(),**kwargs):
        self.dict = dict
        self.kwargs = kwargs
        self.default_value = default_value
        self.output_type = output_type


    def get_values_by_index(self,value):
        return self.dict.get(value,self.default_value)


    def get_udf(self):
        @udf(self.output_type)
        def warpped_udf(col):
            return self.get_values_by_index(str(col))

        return warpped_udf

class list_dict_index_udf:
    def __init__(self,dict,default_value,output_type=StringType(),**kwargs):
        self.dict = dict
        self.kwargs = kwargs
        self.default_value = default_value
        self.output_type = output_type


    def get_values_by_index(self,index):
        return self.dict.get(index,self.default_value)


    def get_udf(self):
        @udf(ArrayType(self.output_type))
        def warpped_udf(col):
            return [ self.get_values_by_index(str(item))  for item in   eval(col)]

        return warpped_udf

class list_dict_has_key_udf:
    def __init__(self,dict):
        self.dict = dict


    def get_values_by_index(self,index):
        r =  self.dict.get(index)
        if r is None:
            return 0
        return 1


    def get_udf(self):
        @udf(ArrayType(IntegerType()))
        def warpped_udf(col):
            return [ self.get_values_by_index(str(item))  for item in eval(col)]

        return warpped_udf



class list_avg_udf:
    def __init__(self,size):
        self._size = size

    def get_udf(self):
        import numpy as np
        @udf(ArrayType(DoubleType()))
        def warpped_udf(col):
            a = None
            l = len(col)
            for item in col:
                if a is None:
                    a = np.array(item)
                else:
                    a += np.array(item)
            if a is None:
                a = np.array([0.0 for i in range(self._size)])
                return a.tolist()

            return (a/l).tolist()

        return warpped_udf

class list_sum_udf:
    def __init__(self,size):
        self._size = size

    def get_udf(self):
        import numpy as np
        @udf(ArrayType(DoubleType()))
        def warpped_udf(col):
            a = None
            for item in col:
                if a is None:
                    a = np.array(item)
                else:
                    a += np.array(item)
            if a is None:
                a = np.array([0.0 for i in range(self._size)])

            return a.tolist()

        return warpped_udf


class split_to_list_udf:
    def __init__(self,split,col_type):
        self._split = split
        self._type = col_type

    def get_udf(self):
        @udf(ArrayType(self._type))
        def warpped_udf(col):
            return [float(i.strip()) for i in str(col).strip().split(self._split)]
        return warpped_udf