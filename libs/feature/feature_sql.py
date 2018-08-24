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

    @staticmethod
    def hour_range(beginDate, endDate):
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
            dt = dt + datetime.timedelta(hours=1)
        return dates

    def __init__(self, name,keys,values,sql, data_date,output,data_time_on_hour= False, pre_sql=None,temp_table_format=None,
                 temp_table=None,batch_cond =None,start_date_offset=None,feature_args=None,once_sql = None):
        if feature_args:
            feature_name = name.format(**feature_args)
            #feature_keys = [k.format(**feature_args) for k in keys]
            #feature_values = [v.format(**feature_args) for v in values]
            feature_keys = keys
            feature_values =values
            args = feature_args
        else:
            feature_name = name
            feature_keys = keys
            feature_values =values
            args = None

        super().__init__(feature_name,feature_keys,feature_values,data_date,output,args = args)
        self._data_time_on_hour = data_time_on_hour
        self._sql = sql
        self._pre_sql= pre_sql
        self._temp_table = temp_table
        self._temp_table_format = temp_table_format
        self._batch_cond = batch_cond
        self._start_date_offset = start_date_offset
        self._feature_args = feature_args
        self._once_sql = once_sql




    def get_sql(self,**kwargs):
         return self._sql.format(**kwargs)


    def get_sql_list(self,dates,pre_sql=False,**kwargs):
        rSql = []
        for d in dates:
            kwargs[self._data_date_col] = d
            if pre_sql :
                sql_format = self._pre_sql
            else:
                sql_format = self._sql
            sql = sql_format.format(**kwargs)
            rSql.append((sql,d))
        return  rSql

    def get_day_sql_list(self, start_date, end_date,pre_sql=False, **kwargs):
        dates = self.date_range(start_date,end_date)
        return self.get_sql_list(dates,pre_sql=pre_sql,**kwargs)

    def get_hour_sql_list(self, start_date, end_date,pre_sql=False, **kwargs):
        dates = self.hour_range(start_date,end_date)
        return self.get_sql_list(dates, pre_sql=pre_sql, **kwargs)

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
        d['pre_sql'] = self._pre_sql
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
                      feature_sql_json['output_name'],
                    pre_sql = feature_sql_json.get('pre_sql'),
                    temp_table_format = feature_sql_json.get('temp_table_format'),
                    temp_table=feature_sql_json.get('temp_table'),
                    data_time_on_hour = feature_sql_json.get('data_time_on_hour'),
                    batch_cond=feature_sql_json.get('batch_cond'),
                    start_date_offset=feature_sql_json.get('start_date_offset'),
                    feature_args=feature_sql_json.get('feature_args'),
                    once_sql=feature_sql_json.get('once_sql'),
        )

    def get_data_date_col(self):
        return self._data_date_col



