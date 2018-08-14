from libs.datasource.datasource_factory import DataSoureFactory
from libs.datasource.rtb_datasource import RTBDataSource


class RTBDataSourceFactory(DataSoureFactory):

    def getDataSource(self,job):
        return RTBDataSource()
