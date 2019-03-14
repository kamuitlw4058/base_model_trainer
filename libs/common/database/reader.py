from zamplus_common.datasource.clickhouse.engine import create_engine
from zamplus_common.datasource.mysql.engine import create_engine as mysql_create_engine
from zamplus_common.datasource.mysql.engine import get_default_database as mysql_default_database
from zamplus_common.utils.DatetimeUtils import get_utc_sec, get_datetime_by_utc_sec, get_hour_sec
from datetime import datetime
import  pandas as pd
from zamplus_common.datasource.clickhouse.cmd import cmd_sql

def get_mysql_reader(database=None):
    engine = mysql_create_engine()
    if not database:
        database = mysql_default_database()

    reader = Reader(engine,database)

    return reader


class Reader():
    def __init__(self,engine=None,database=None):
        self._database = database
        self._engine = engine

    def set_database(self,database):
        self._database = database

    def read_sql(self, sql_str, database=None, use_spark=False, parallel=1):
        if not database:
            if not self._database:
                raise Exception("没有定义数据库")
            database = self._database
        else:
            self._database = database

        if not self._engine:
            self._engine = create_engine(database)

        if use_spark:
            from zamplus_common.datasource.clickhouse.spark import clickhouse_jdbc, clickhouse_jdbc_parallel
            from zamplus_common.datasource.clickhouse.spark import sql_op as spark_sql_op

            if parallel == 0:
                return spark_sql_op(sql_str)
            elif parallel == 1:
                return clickhouse_jdbc(sql_str,database=database)
            else:
                return clickhouse_jdbc_parallel(sql_str,database=database)
        else:
            return pd.read_sql(sql_str,self._engine)


    def exec_sql(self, sql,args):
        db_conn = self._engine.connect()

        try:
            with db_conn.begin() as db_trans:
                db_conn.execute(sql,*args)

                db_trans.commit()
        except Exception as e:
            print('exception!!!')
            print(e)




    def to_sql(self,df,table_name):

        insert_sql_temp = """
        INSERT INTO 
        {table_name}
        ({column_names}) 
        VALUES  ({row_values})
        ON DUPLICATE KEY UPDATE 
            {update_cols}
        """

        format_dict = {}
        values_dict = {}
        values_list = []
        format_dict['table_name'] = table_name
        format_dict['column_names'] = ",".join(df.columns)
        # package_size=VALUES(package_size),
        for row in df.values:
            row_str = []
            for col,v in zip(df.columns,row):
                if isinstance(v, float):
                    row_str.append(f"%f")
                elif isinstance(v, int):
                    row_str.append(f"%d")
                else:
                    row_str.append(f"%s")

                values_dict[col] = v
                values_list.append(str(v))
            update_cols = []
            for c in df.columns:
                update_cols.append(f"{c}=VALUES({c})\n")

            format_dict['row_values'] = ",".join(row_str)
            format_dict['update_cols'] = ",".join(update_cols)
            insert_sql = insert_sql_temp.format(**format_dict)

            r = self.exec_sql(insert_sql,values_list)
       # for dataframe.coloumns:
       # dataframe.to_sql(name=table_name, con=self._engine, if_exists='append', index=False)

    def parquet(self,path):
        from zamplus_common.datasource.clickhouse.spark import parquet as spark_parquet
        return spark_parquet(path)

    def csv(self,path):
        from zamplus_common.datasource.clickhouse.spark import csv as spark_csv
        return spark_csv(path)



reader = Reader()


def sql_dict_op(sql_str,sql_reader=reader,name=None,use_spark=False,database=None,verbose=False,dict_type='records'):
    result = sql_op(sql_str,name=name,sql_reader=sql_reader,use_spark=use_spark,database=database,verbose=verbose)
    return result.to_dict(dict_type)

def sql_op(sql_str,sql_reader = reader,name=None,use_spark=False,database=None,verbose=False,parallel=1):
    if database is not None:
        reader.set_database(database)

    if verbose:
        print("<name>:" + str(name))
        #print("<sql>:" + sql_str)
    result = sql_reader.read_sql(sql_str, use_spark=use_spark,parallel=parallel)
    if verbose:
        print("finish sql")
        #print("<dataframe>:\n" + str(result))
    return result

def sql_cmd_op(sql_str,name=None,database=None,verbose=False,delete_output=True):
    if database is not None:
        reader.set_database(database)

    if verbose:
        print("<name>:" + str(name))
        #print("<sql>:" + sql_str)
    result =cmd_sql(sql_str,name=name,read_output=True,delete_output=delete_output)
    if verbose:
        print("finish sql")
        #print("<dataframe>:\n" + str(result))
    return result


def _get_time_params(window, data_time):
    if not window or not data_time:
        print(" Wrong parmas of funcation")

    end_time_sec = get_utc_sec(data_time)
    start_time_sec = end_time_sec - window
    start_date = get_datetime_by_utc_sec(start_time_sec)
    end_date = get_datetime_by_utc_sec(end_time_sec)

    return  start_date,end_date,start_time_sec,end_time_sec


def sql_window_op( sql, window, sql_reader=reader,data_time=datetime.now(),name=None,use_spark=False,database=None,hours=True,args=None,verbose=False):
    if hours:
        window = window * 60 * 60
    start_date, end_date, start_time_sec, end_time_sec = _get_time_params(window, data_time)

    use_args = {}
    use_args['start_date'] = start_date
    use_args['end_date'] = end_date
    use_args['start_ts'] = start_time_sec
    use_args['end_ts'] = end_time_sec
    if args is not None:
       use_args.update(args)
    print(use_args)

    sql_str =sql.format(**use_args)

    #print(sql_str)
    return sql_op(sql_str,sql_reader=sql_reader, name=name,use_spark=use_spark,database=database,verbose=verbose)
