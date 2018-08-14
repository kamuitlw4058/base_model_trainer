from  libs.feature.feature_base import feature_base

import datetime


class feature_sql(feature_base):

    @staticmethod
    def date_range(beginDate, endDate):
        dates = []
        dt  =None
        if isinstance( beginDate,datetime.datetime):
            dt = beginDate
        else:
            dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")

        endDt = None

        if isinstance( endDate,datetime.datetime):
            endDt = endDate
        else:
            endDt = datetime.datetime.strptime(endDate, "%Y-%m-%d")



        date = dt.strftime("%Y-%m-%d")
        while dt <= endDt:
            dates.append(dt)
            #dates.append(date)
            dt = dt + datetime.timedelta(1)
            date = dt.strftime("%Y-%m-%d")
        return dates


    def __init__(self, name,keys, sql, data_date,output):
        super(feature_sql,self).__init__(name,keys,data_date,output)
        self._sql = sql

    def get_sql(self,**kwargs):
         return self._sql.format(**kwargs)

    def get_sql_list(self, start_date, end_date, **kwargs):
        rSql = []
        dates = feature_sql.date_range(start_date,end_date)
        for d in dates:
            kwargs[self._data_date] = d
            sql = self._sql.format(**kwargs)
            rSql.append((sql,d))
        return  rSql


    def get_output_file_name(self,data_date,**kwargs):
        kwargs[self._data_date] = data_date
        return self._output.format(**kwargs)