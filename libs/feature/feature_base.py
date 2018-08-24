
class FeatureBase:
    def __init__(self, name,keys, values,data_date_col,output_name,args =None):
        self._name = name
        self._keys = keys
        self._values = values
        self._data_date_col = data_date_col
        self._output_name = output_name
        self._args = args

    def get_name(self):
        return self._name

    def get_keys(self):
        return self._keys

    def get_values(self):
        return self._values

    def get_data_date_col(self):
        return self._data_date_col

    def get_args(self):
        return self._args

    def get_output_name(self,**kwargs):
        return self._output_name

