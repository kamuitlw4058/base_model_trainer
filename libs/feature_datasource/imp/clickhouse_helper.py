from conf.conf import ZAMPLUS_ZAMPDA_DATABASE,ZAMPLUS_ZAMPDA_LOCAL_DATABASE,ZAMPLUS_RTB_ALL_JDBC_URL,ZAMPLUS_RTB_LOCAL_JDBC_URL,CLICKHOUSE_DAILY_SQL_DATE_COL
from libs.env.hdfs import hdfs
from libs.utilis.sql_utils import jdbc_sql
from libs.feature.cleaner import clean_nan_data
import logging
logger = logging.getLogger(__name__)


def get_and_write_data( reader, sql, output_path, parallel=False,clean_data=True,df_handler=None,write_df =True):
    if parallel:
        df = reader.read_sql_parallel(sql, url=ZAMPLUS_RTB_LOCAL_JDBC_URL)
    else:
        df = reader.read_sql(sql, ZAMPLUS_RTB_ALL_JDBC_URL)

    if df_handler is not None:
        df = df_handler(reader._spark, df)

    if clean_data:
        df = clean_nan_data(df)
    if write_df:
        df.write.parquet(path=output_path, mode='overwrite')
    return df



def clickhouse_produce_data(name,reader, sql, output_path, overwrite=False,parallel=False,clean_data=True,df_handler=None,write_df=True):
    df = None
    if not hdfs.exists(output_path):
        logger.info("[feature] {name} [file] {path} is not exist! we get data from clickhouse."
                    .format(name=name,path=output_path))
        apply_sql = jdbc_sql(sql)
        df =get_and_write_data(reader, apply_sql, output_path,parallel=parallel,clean_data=clean_data,df_handler=df_handler,write_df=write_df)
    else:
        if overwrite:
            logger.info(
                "[feature] {name} [file] {path} is exist! we get data from clickhouse and overwrite"
                    .format(name=name, path=output_path))
            apply_sql = jdbc_sql(sql)
            df =get_and_write_data(reader, apply_sql, output_path,parallel=parallel,clean_data=clean_data,df_handler=df_handler,write_df=write_df)

    return df



def clickhouse_check_filepath(name,output_path):
    if hdfs.exists(output_path):
        logger.info("[feature]{name} [file]{path} is exist! we will read the file!"
                    .format(name=name, path=output_path))
        return True
    else:
        logger.error("[feature]{name} [file]{path} is not exist!!!!"
                     .format(name=name, path=output_path))
        return False