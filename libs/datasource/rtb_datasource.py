import logging
logger = logging.getLogger(__name__)

from libs.datasource.datasource import DataSource
import random
import pandas as pd
from copy import deepcopy
from sqlalchemy import create_engine
from conf.clickhouse import hosts
from libs.dataio.sql import SQL
from conf.conf import MAX_POS_SAMPLE, CLK_LIMIT,JOB_ROOT_DIR
from libs.env.spark import spark_session,SparkClickhouseReader
from libs.feature.define import get_raw_columns,get_feature_base_columns
from libs.feature.define import get_bidding_feature, context_feature, user_feature, user_cap_feature
from libs.feature.cleaner import clean_data
from libs.feature import udfs
from libs.feature.feature_sql import FeatureSql
from libs.feature.feature_factory import FeatureReader
from datetime import datetime
from conf import clickhouse

_win_filter = ['TotalErrorCode = 0', 'Win_Timestamp > 0']

_clk_filters = ['notEmpty(Click.Timestamp) = 1'] + _win_filter

_imp_filters = ['notEmpty(Click.Timestamp) = 0'] + _win_filter

_zamplus_rtb_all_database = 'zampda'

_zamplus_rtb_local_database = 'zampda_local'

#TODO:这边需要整理下，clickhouse jdbc链接和 clickhouse的直接链接url怎么进行管理。？？？

_zamplus_rtb_local_url = f'jdbc:clickhouse://{random.choice(hosts)}/{_zamplus_rtb_local_database}'

class RTBDataSource(DataSource):

    def __init__(self,job_id,local_dir,filters,
                 pos_proportion,neg_proportion,
                 url_template,all_table,
                 local_table,
                 account,vendor,
                 start_date,end_date,
                 test_start_date, test_end_date,
                 new_features):
        self._url_template = url_template
        self._url_all = self._url_template.format(random.choice(hosts),_zamplus_rtb_all_database)
        self._url_local = self._url_template.format(random.choice(hosts),_zamplus_rtb_local_database)
        self._engine = create_engine(self._url_all)
        self._filters = filters
        self._neg_proportion = neg_proportion
        self._pos_proportion = pos_proportion
        self._all_table = all_table
        self._local_table = local_table
        self._job_id = job_id
        self._local_dir = local_dir
        self._account = account
        self._vendor = vendor
        self._start_date = start_date
        self._end_date = end_date
        self._test_start_date = test_start_date
        self._test_end_date = test_end_date
        self._new_features = new_features
        self._raw_count = None
        self._executor_num = None
        self._clk_num = None
        self._imp_num = None
        self._spark = None
        self._train_filters = deepcopy(self._filters)
        self._test_filters = deepcopy(self._filters)
        self._filter_account = None
        self._filter_vendor = None
        self._data_split_mode = "byRamdon"

        logger.info(" start date:" + self._start_date)
        logging.info(" start date:" + self._start_date)
        logging.info(" end date:" + self._end_date)
        for f in  self._filters:
            f_str = f.replace(' ','')
            if f_str.startswith("Bid_CompanyId"):
                self._filter_account = True
            elif f_str.startswith("Media_VendorId"):
                self._filter_vendor = True

        filter_str = "EventDate>='" + self._start_date+ "'"
        logging.info("append filter start date:" + filter_str)
        self._train_filters.append(filter_str)

        filter_str = "EventDate<='" + self._end_date + "'"
        logging.info("append filter end date:" + filter_str)
        self._train_filters.append(filter_str)

        filter_str = "EventDate>='" + self._test_start_date+ "'"
        logging.info("append filter test start date:" + filter_str)
        self._test_filters.append(filter_str)

        filter_str = "EventDate<='" + self._test_end_date + "'"
        logging.info("append filter test end date:" + filter_str)
        self._test_filters.append(filter_str)

        if not self._filter_account and self._account:
            filter_str = "Bid_CompanyId=" + str(self._account)
            logging.info("append filter Bid_CompanyId:" + filter_str)
            self._train_filters.append(filter_str)
            self._test_filters.append(filter_str)

        if not self._filter_vendor and self._vendor:
            filter_str = "Media_VendorId=" + str(self._vendor)
            logging.info("append filter Media_VendorId:" + filter_str)
            self._train_filters.append(filter_str)
            self._test_filters.append(filter_str)

    def _get_clk_imp(self, filters):

        cols = [
            'sum(notEmpty(Click.Timestamp)) as clk',
            'sum(notEmpty(Impression.Timestamp)) as imp'
        ]
        sql = SQL()
        q = sql.table(self._all_table).select(cols).where(filters).to_string()
        logging.info("sql: " + q)

        num = pd.read_sql(q, self._engine)
        if num.empty:
            clk_num, imp_num = 0, 0
        else:
            clk_num, imp_num = num.clk.sum(), num.imp.sum()


        return clk_num, imp_num



    def _get_sample_ratio(self,clk_num,imp_num):

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
        if estimated_samples < 40 * 10000:
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
    def _build_feature_datas_sql(filters,pos_ratio,neg_ratio,table):
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

    @staticmethod
    def expend_fields(raw, ext_feature):
        # append fields
        from functools import reduce
        ext_dict = 'ext_dict'
        opts = [
            ('weekday', udfs.weekday('ts')),
            ('is_weekend', udfs.is_weekend('ts')),
            (ext_dict, udfs.to_ext_dict('ext_key', 'ext_value')),
        ]

        raw = reduce(lambda d, args: d.withColumn(*args), opts, raw)

        # extract cap feature & bidding feature from ext_dict
        raw = reduce(lambda df, c: df.withColumn(c, df[ext_dict].getItem(c)), ext_feature, raw)

        return raw.drop(ext_dict, 'ext_key', 'ext_value')

    def _get_features(self,raw):
        if self._account and self._vendor:
            bidding_feature = get_bidding_feature(self._account, self._vendor)
            ext_feature = user_cap_feature + bidding_feature
        else:
            ext_feature = user_cap_feature
        raw = RTBDataSource.expend_fields(raw, ext_feature)

        raw = clean_data(self._job_id, raw, self._spark)
        logging.info('[%s] columns %s', self._job_id, raw.schema.names)

        return  raw, user_feature + context_feature + ext_feature

    def _get_multi_value_feature(self):
        return [
            'AppCategory',
            'segment'
        ]

    def get_data_size(self):
        return self._raw_count

    def get_executor_num(self):
        return self._executor_num

    def get_clk_imp(self):
        return self._clk_num,self._imp_num


    def _drop_feature_base_columns(self,raw):
        for col in get_feature_base_columns():
            col_list = col.split("as")
            if len(col_list) > 1:
                drop_col = col_list[1].strip()
            else:
                drop_col = col
            raw = raw.drop(drop_col)

        return raw


    def get_feature_datas(self):
        logging.info("train filters:" + str(self._train_filters))
        logging.info("test filters:" + str(self._test_filters))

        clk_num, imp_num = self._get_clk_imp(self._test_filters)

        logging.info('[%s] test clk=%d, imp=%d', self._job_id, clk_num, imp_num)

        clk_num, imp_num = self._get_clk_imp(self._train_filters)

        self._clk_num ,self._imp_num = clk_num,imp_num

        logging.info('[%s] train clk=%d, imp=%d', self._job_id, clk_num, imp_num)

        if clk_num < CLK_LIMIT:
            raise RuntimeError(f'[{self._job_id}] too fewer clk({clk_num} < {CLK_LIMIT})')

        pos_ratio, neg_ratio = self._get_sample_ratio(clk_num,imp_num)

        logging.info('[%s] pos_ratio = %.4f neg_ratio = %.4f', self._job_id, pos_ratio, neg_ratio)

        estimated_samples = int(clk_num * pos_ratio
                                + (imp_num - clk_num) * neg_ratio)
        logging.info(f'[{self._job_id}] estimated samples = {estimated_samples}')

        sql = RTBDataSource._build_feature_datas_sql(self._train_filters, pos_ratio, neg_ratio, self._local_table)
        sql = RTBDataSource._get_jdbc_sql(sql)

        test_sql = RTBDataSource._build_feature_datas_sql(self._test_filters, pos_ratio, neg_ratio, self._local_table)
        test_sql = RTBDataSource._get_jdbc_sql(test_sql)

        #logging.info(f'[{self._job_id}] get data sql: {sql}')

        spark_executor_num = RTBDataSource._get_executor_num(estimated_samples)

        logging.info(f'[{self._job_id}] spark_executor_num: {spark_executor_num}')
        self._executor_num = spark_executor_num

        spark = spark_session(self._job_id,spark_executor_num,self._local_dir)
        self._spark = spark

        features_readers = []
        if self._new_features:
            logging.info(f'[{self._job_id}] start get features...')
            start_date = datetime.strptime(self._start_date, '%Y-%m-%d')

            if self._test_end_date:
                end_date = datetime.strptime(self._test_end_date, '%Y-%m-%d')
            else:
                if self._end_date:
                    end_date = datetime.strptime(self._end_date, '%Y-%m-%d')
                else:
                    end_date = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')

            for f_path in self._new_features.split("#"):
                logging.info(f'[{self._job_id}] start process new features...')
                logging.info(f'[{self._job_id}] load features file from {f_path}')
                new_features = FeatureSql.from_file(f_path)
                feature_reader = FeatureReader(new_features, _zamplus_rtb_local_url,spark_executor_num)
                args = {'account': self._account, 'vendor': self._vendor}
                if new_features.get_args():
                    args.update(new_features.get_args())

                feature_reader.read( start_date, end_date,clickhouse.ONE_HOST_CONF, session=spark, **args)
                features_readers.append(feature_reader)


        spark_clickhouse_reader = SparkClickhouseReader(spark,_zamplus_rtb_local_url)

        raw = spark_clickhouse_reader.read_sql_parallel(sql,spark_executor_num)

        test = spark_clickhouse_reader.read_sql_parallel(test_sql,spark_executor_num)

        raw, features = self._get_features(raw)

        test, test_features = self._get_features(test)

        if self._new_features:
            for reader in features_readers:
                logging.info(f'[{self._job_id}] start union features...')
                raw = FeatureReader.unionRaw(raw,reader.get_feature_df(),reader.get_feature_keys())
                test = FeatureReader.unionRaw(test, reader.get_feature_df(),reader.get_feature_keys())

                args = {'account': self._account, 'vendor': self._vendor}
                if reader.get_feature().get_args():
                    args.update(reader.get_feature().get_args())
                features = features + reader.get_feature().get_values(**args)

        raw = self._drop_feature_base_columns(raw)

        test = self._drop_feature_base_columns(test)

        raw.repartition(spark_executor_num)

        raw.cache()

        self._raw_count = raw.count()
        raw.show(10)

        logging.info(f'[{self._job_id}] raw count: {self._raw_count}')

        return raw,test,features,self._get_multi_value_feature()

    def close(self):
        try:
            if self._spark:
                self._spark.stop()
                logging.info('[%s] spark stopped', self._job_id)

        except Exception as e:
            logging.exception("close spark error!",e)
