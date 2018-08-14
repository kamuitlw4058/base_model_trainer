from abc import ABC,abstractmethod

class DataOutput(ABC):

    @abstractmethod
    def write_hdfs(self,df,path,raw_cols=None):
        return



