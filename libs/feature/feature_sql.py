from  libs.feature.feature_base import FeatureBase
import json

import datetime


class FeatureSql(FeatureBase):

    @staticmethod
    def date_range(beginDate, endDate):
        dates = []
        if isinstance( beginDate,datetime.datetime):
            dt = beginDate
        else:
            dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")

        if isinstance(endDate,datetime.datetime):
            end_dt = endDate
        else:
            end_dt = datetime.datetime.strptime(endDate, "%Y-%m-%d")

        while dt <= end_dt:
            dates.append(dt)
            dt = dt + datetime.timedelta(1)
        return dates

    def __init__(self, name,keys,values, sql, data_date,output):
        super().__init__(name,keys,values,data_date,output)
        #super(FeatureBase, self).__init__(name,keys,values,data_date,output)
        self._sql = sql

    def get_sql(self,**kwargs):
         return self._sql.format(**kwargs)

    def get_sql_list(self, start_date, end_date, **kwargs):
        rSql = []
        dates = self.date_range(start_date,end_date)
        for d in dates:
            kwargs[self._data_date_col] = d
            sql = self._sql.format(**kwargs)
            rSql.append((sql,d))
        return  rSql

    def get_keys(self):
        return self._keys

    def get_name(self):
        return self._name

    def get_output_name(self,data_date,**kwargs):
        kwargs[self._data_date_col] = data_date
        return self._output_name.format(**kwargs)

    def get_values(self,**kwargs):
        return [v.format(**kwargs) for v in  self._values]

    def to_file(self,filename):
        f = open(filename, 'w', encoding='utf-8')
        d = {}

        #def __init__(self, name, keys, values, sql, data_date, output):
        d['name'] = self.get_name()
        d['keys'] = self.get_keys()
        d['values'] = self._values
        d['sql'] = self._sql
        d['data_date_col'] = self._data_date_col
        d['output_name'] = self._output_name
        json.dump(d, f, ensure_ascii=False)

    @staticmethod
    def from_file(filename):
        f = open(filename, 'r', encoding='utf-8')
        feature_sql_json = json.load(f)
        return FeatureSql(feature_sql_json['name'],
                      feature_sql_json['keys'],
                      feature_sql_json['values'],
                      feature_sql_json['sql'],
                      feature_sql_json['data_date_col'],
                      feature_sql_json['output_name'])

    def get_data_date_col(self):
        return self._data_date_col



