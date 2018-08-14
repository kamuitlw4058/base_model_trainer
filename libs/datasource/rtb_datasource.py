from libs.datasource.datasource import DataSource


class RTBDataSource(DataSource):

    def get_spark_session(self):
        return spark,spark_executor_num
