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

    def __init__(self, name,keys,values,sql=None, data_date=None,output = None,data_time_on_hour= False, pre_sql=None,temp_table_format=None,
                 temp_table=None,batch_cond =None,start_date_offset=None,feature_args=None,once_sql = None,csv=None,csv_sep=None,number_features=None):
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
        self._csv= csv
        self._csv_sep= csv_sep
        self._number_features = number_features




    def get_sql(self,**kwargs):
         return self._sql.format(**kwargs)



    def get_sql_list(self,dates,sql_template,**kwargs):
        rSql = []
        for d in dates:
            kwargs[self._data_date_col] = d
            sql = sql_template.format(**kwargs)
            rSql.append((sql,d))
        return  rSql


    def get_day_sql_list(self, start_date, end_date,sql_template, **kwargs):
        dates = self.date_range(start_date,end_date)
        return self.get_sql_list(dates,sql_template,**kwargs)

    def get_hour_sql_list(self, start_date, end_date,sql_template, **kwargs):
        dates = self.hour_range(start_date,end_date)
        return self.get_sql_list(dates, sql_template, **kwargs)

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
                    sql =   feature_sql_json.get('sql'),
                    data_date = feature_sql_json.get('data_date_col'),
                    output =  feature_sql_json.get('output_name'),
                    pre_sql = feature_sql_json.get('pre_sql'),
                    temp_table_format = feature_sql_json.get('temp_table_format'),
                    temp_table=feature_sql_json.get('temp_table'),
                    data_time_on_hour = feature_sql_json.get('data_time_on_hour'),
                    batch_cond=feature_sql_json.get('batch_cond'),
                    start_date_offset=feature_sql_json.get('start_date_offset'),
                    feature_args=feature_sql_json.get('feature_args'),
                    once_sql=feature_sql_json.get('once_sql'),
                    csv = feature_sql_json.get('csv'),
                    csv_sep = feature_sql_json.get('csv_sep'),
                    number_features=feature_sql_json.get('number_features'),
        )

    def get_data_date_col(self):
        return self._data_date_col



