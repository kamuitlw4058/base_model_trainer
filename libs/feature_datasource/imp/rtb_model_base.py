from libs.feature_datasource.imp.clickhouse_sql import  ClickHouseSQLDataSource
import logging
logger = logging.getLogger(__name__)
from libs.datasource.sql import SQL
import pandas as pd
from sqlalchemy import create_engine
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT,JOB_ROOT_DIR
from libs.feature.define import get_raw_columns,get_feature_base_columns
from conf.conf import ZAMPLUS_ZAMPDA_DATABASE,ZAMPLUS_ZAMPDA_LOCAL_DATABASE,ZAMPLUS_RTB_ALL_JDBC_URL,ZAMPLUS_RTB_LOCAL_JDBC_URL,CLICKHOUSE_DAILY_SQL_DATE_COL,ZAMPLUS_RTB_ALL_URL
from conf.conf import RTB_ALL_TABLE_NAME, RTB_LOCAL_TABLE_NAME,CLICKHOUSE_URL_TEMPLATE

from datetime import datetime
from pyspark.sql.functions import broadcast

_win_filter = ['TotalErrorCode = 0', 'Win_Timestamp > 0']

_clk_filters = ['notEmpty(Click.Timestamp) = 1'] + _win_filter

_imp_filters = ['notEmpty(Click.Timestamp) = 0'] + _win_filter


_edu_mask = pd.DataFrame({
    'Education'    : ['',    '小学',            '中学',            '高中',           '高中(中专)及以下', '高中中专及以下',   '高中（中专）及以下', '大学大专及以上', '本科',      '本科及大专', '硕士', '硕士及以上', '博士', '博士及以上'],
    'education_new': ['nan', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下',  '本科及大专',     '本科及大专', '本科及大专', '硕士', '硕士',      '博士', '博士']
})

_age_mask = pd.DataFrame({
    'Age'    : ['',    '0-18', '19-23', '24-30', '31-40', '41-50', '50以上', '51-999'],
    'age_new': ['nan', '0-18', '19-23', '24-30', '31-40', '41-50', '51-199', '51-199']
})

_gender_mask = pd.DataFrame({
    'Gender'    : ['', 'gender', '女', '男'],
    'gender_new': ['nan', 'nan', '女', '男']
})



def clean_rtb_data(spark, raw):

    for mask, join_col, temp_col in [
        (_edu_mask, 'Education', 'education_new'),
        (_age_mask, 'Age', 'age_new'),
        (_gender_mask, 'Gender', 'gender_new')
    ]:
        _mask = spark.createDataFrame(mask)
        raw = raw.join(broadcast(_mask), join_col, how='left_outer').drop(join_col).withColumnRenamed(temp_col, join_col)

    return raw


class RTBModelBaseDataSource(ClickHouseSQLDataSource):

    @staticmethod
    def get_type():
        return "RTBModelBase"

    def __init__(self,name,start_date,end_date,spark,device_os=None,is_ht=None,pos_proportion=1,neg_proportion=2,global_filter=None,account=None,vendor=None,is_train=True,**kwargs):
        self.name = name
        self._start_date = start_date
        self._end_date = end_date
        self._global_filter = global_filter
        self._apply_filter =[]
        self._account = account
        self._vendor = vendor
        self._pos_proportion = pos_proportion
        self._neg_proportion = neg_proportion
        self._device_os = device_os
        self._is_ht = is_ht
        self._clk_num = None
        self._imp_num = None
        self._engine = create_engine(ZAMPLUS_RTB_ALL_URL)
        self._kwargs = {}
        self._filter_account =None
        self._filter_vendor = None
        self._filter_device_os = None
        self._filter_is_ht = None

        if is_train:
            self._kwargs["TrainData"] = ""
        else:
            self._kwargs["TestData"] = ""
        # self._kwargs.update(kwargs)



        if self._global_filter is not None:
            for f in  self._global_filter:
                f_str = f.replace(' ','')
                if f_str.startswith("Bid_CompanyId"):
                    self._filter_account = True
                elif f_str.startswith("Media_VendorId"):
                    self._filter_vendor = True
                elif f_str.startswith("Device_Os"):
                    self._filter_device_os = True
                elif f_str.startswith("has(Segment.Id, 100012)"):
                    self._filter_is_ht = True
            self._apply_filter += self._global_filter


        filter_str = "EventDate>='" + self._start_date+ "'"
        logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - append filter start date:{self._start_date}")
        self._apply_filter.append(filter_str)
        self._kwargs['SD'] = datetime.strptime(self._start_date, "%Y-%m-%d").strftime("%Y%m%d")

        filter_str = "EventDate<='" + self._end_date + "'"
        logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - append filter end date:{self._end_date}")
        self._apply_filter.append(filter_str)
        self._kwargs['ED'] = datetime.strptime(self._end_date, "%Y-%m-%d").strftime("%Y%m%d")


        if not self._filter_account and self._account is not None:
            filter_str = "Bid_CompanyId=" + str(self._account)
            logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - append filter Bid_CompanyId:{self._account}")
            self._apply_filter.append(filter_str)
            self._kwargs['account'] = self._account

        if not self._filter_vendor and self._vendor is not None:
            filter_str = "Media_VendorId=" + str(self._vendor)
            logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{name} - append filter Media_VendorId:{self._vendor}")
            self._apply_filter.append(filter_str)
            self._kwargs['vendor'] = self._vendor

        if not self._filter_device_os and self._device_os is not None:
            filter_str = "Device_Os=" + str(self._device_os)
            logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - append filter Device_Os:{self._device_os}")
            self._apply_filter.append(filter_str)
            self._kwargs['device_os'] = self._device_os

        if not self._filter_is_ht and self._is_ht is not None:
            filter_str = "has(Segment.Id, 100012)=" + str(self._is_ht)
            logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - append filter has(Segment.Id, 100012):{self._is_ht}")
            self._apply_filter.append(filter_str)
            self._kwargs['ht'] = self._is_ht
        self._apply_filter = self._apply_filter + _win_filter

        super().__init__(name,"",spark,parallel=True,**self._kwargs)

    def _get_clk_imp(self, filters):

        cols = [
            'sum(notEmpty(Click.Timestamp)) as clk',
            'sum(notEmpty(Impression.Timestamp)) as imp'
        ]
        sql_builder = SQL()
        sql = sql_builder.table(RTB_ALL_TABLE_NAME).select(cols).where(filters).to_string()
        #logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - clk_imp_sql:{sql}")
        num = pd.read_sql(sql, self._engine)
        if num.empty:
            clk_num, imp_num = 0, 0
        else:
            clk_num, imp_num = num.clk.sum(), num.imp.sum()

        return clk_num, imp_num

    def _get_sample_ratio(self, clk_num, imp_num):

        pos_ratio = self._pos_proportion
        neg_ratio = self._neg_proportion * clk_num / (imp_num - clk_num)

        if clk_num > MAX_POS_SAMPLE:
            pos_ratio = self._pos_proportion * MAX_POS_SAMPLE / clk_num
            neg_ratio = self._neg_proportion * MAX_POS_SAMPLE / (imp_num - clk_num)

        # ensure ratio not larger than 1
        pos_ratio = min(pos_ratio, 1)
        neg_ratio = min(neg_ratio, 1)

        return pos_ratio, neg_ratio

    @staticmethod
    def _get_executor_num(estimated_samples):
        if estimated_samples < 10 * 10000:
            return 2
        elif estimated_samples < 20 * 10000:
            return 4
        elif estimated_samples < 40 * 10000:
            return 8
        elif estimated_samples < 80 * 10000:
            return 16
        elif estimated_samples < 160 * 10000:
            return 32
        elif estimated_samples < 320 * 10000:
            return 48
        else:
            return 64

    @staticmethod
    def _build_feature_datas_sql(filters, pos_ratio, neg_ratio, table):
        cols = get_raw_columns() + get_feature_base_columns()
        posSql = SQL()
        posSql.table(table).select(cols).sample(pos_ratio).where(filters + _clk_filters)
        negSql = SQL()
        negSql.table(table).select(cols).sample(neg_ratio).where(filters + _imp_filters)
        posSql.union([negSql])
        sql = posSql.to_string()
        return sql

    @staticmethod
    def _get_jdbc_sql(sql):
        return f"({sql})"


    def _get_sql(self):
        clk_num, imp_num = self._get_clk_imp(self._apply_filter)

        logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - clk={clk_num}, imp={imp_num}")

        self._clk_num, self._imp_num = clk_num, imp_num

        if clk_num < CLK_LIMIT:
            raise RuntimeError(f'[{self.name}] too fewer clk({clk_num} < {CLK_LIMIT})')


        pos_ratio, neg_ratio = self._get_sample_ratio(clk_num, imp_num)

        logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} - pos_ratio={round(pos_ratio,3)}, neg_ratio={round(neg_ratio,3)}")

        estimated_samples = int(clk_num * pos_ratio + (imp_num - clk_num) * neg_ratio)

        logger.info(f"[{RTBModelBaseDataSource.get_type()}]:{self.name} -  estimated samples={estimated_samples}")
        self._data_size = estimated_samples

        sql = RTBModelBaseDataSource._build_feature_datas_sql(self._apply_filter, pos_ratio, neg_ratio, RTB_LOCAL_TABLE_NAME)
        sql = RTBModelBaseDataSource._get_jdbc_sql(sql)


        return sql,RTBModelBaseDataSource._get_executor_num(estimated_samples)

    def get_dataframe(self):
        return super().get_dataframe()


    def produce_data(self, overwrite=False):
        sql,executor_num =self._get_sql()
        super().set_sql(sql)
        logger.info(
            f"[{self.name}] - kwargs:{self._kwargs}")
        super().produce_data(overwrite=overwrite,df_handler=clean_rtb_data)

